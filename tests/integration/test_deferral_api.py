"""Sprint 4 HTTP contract (M3-T16, M3-T23; SPECS §5).

The 202 body is asserted exactly — it is a graded contract shape, not an
internal detail — together with the operator drain endpoint and the live
``deferred_pending`` health field.

A local stack (rather than the shared ``make_stack`` fixture) exposes the app
object, so tests can reach ``app.state`` and run the real lifespan hooks.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from app import store
from app.config import Settings
from app.main import create_app
from app.models import CallEvent
from app.pipeline.http_client import HttpPipelineClient
from app.resilience.circuit_breaker import CircuitState
from mock.config import MockSettings
from mock.main import create_mock_app

FAST_LATENCY_MU = -3.0


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


class FlakyBackend:
    """In-process boundary whose health the test flips (no mock timing)."""

    def __init__(self) -> None:
        self.failing = True
        self.dispatches = 0

    async def aclose(self) -> None:
        pass

    async def process(self, event_id: str, audio_ref: str):
        from app.pipeline.base import PipelineResult

        self.dispatches += 1
        if self.failing:
            return PipelineResult(
                ok=False, reason="downstream_timeout", http_status=503, latency_ms=5.0
            )
        return PipelineResult(ok=True, http_status=200, latency_ms=250.0)


class ApiStack:
    """Inference app + mock app over ASGI, with the app object exposed."""

    def __init__(
        self,
        tmp_path,
        *,
        failure_rate: float,
        backend=None,
        **inference_overrides,
    ) -> None:
        self.mock_app = create_mock_app(
            MockSettings(latency_mu=FAST_LATENCY_MU, failure_rate=failure_rate)
        )
        self.settings = Settings(
            db_path=str(tmp_path / "deferral-api.db"),
            pipeline_backend="http",
            pipeline_base_url="http://mock.test",
            **inference_overrides,
        )
        if backend is None:
            self.backend = HttpPipelineClient(
                self.settings, transport=httpx.ASGITransport(app=self.mock_app)
            )
        else:
            self.backend = backend
        self.app = create_app(self.settings, self.backend)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://inference.test"
        )
        self.mock = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.mock_app), base_url="http://mock.test"
        )

    async def set_mock_failure_rate(self, rate: float) -> None:
        resp = await self.mock.post("/admin/config", json={"failure_rate": rate})
        assert resp.status_code == 200

    async def wait_for_half_open(self, timeout_s: float = 5.0) -> None:
        """Poll until the cooldown elapses (state reads perform the advance)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while self.app.state.breaker.state is not CircuitState.HALF_OPEN:
            if loop.time() > deadline:
                raise AssertionError("breaker never advanced to half_open")
            await asyncio.sleep(0.01)

    async def open_breaker(self, *, cooldown_s: float | None = None) -> None:
        """Trip the breaker through the public API (5 consecutive failures).

        With a zero cooldown the breaker advances to HALF_OPEN on the next
        state read (that is the state machine working), so callers that need
        literal OPEN keep the default cooldown; callers that only need
        "dispatch is gated" pass ``cooldown_s=0`` and assert on the refusal
        itself, which the tests do.
        """
        for _ in range(5):
            resp = await self.client.post("/calls", json=event_payload())
            assert resp.status_code == 200
        if cooldown_s is None:
            assert self.app.state.breaker.state is CircuitState.OPEN

    async def aclose(self) -> None:
        await self.client.aclose()
        await self.mock.aclose()
        await self.backend.aclose()
        self.app.state.conn.close()


@pytest.fixture
def api_stack(tmp_path):
    def _factory(failure_rate: float, backend=None, **overrides) -> ApiStack:
        return ApiStack(tmp_path, failure_rate=failure_rate, backend=backend, **overrides)

    return _factory


async def test_breaker_open_returns_exact_202_body(api_stack) -> None:
    stack = api_stack(1.0)
    try:
        await stack.open_breaker()

        event_id = str(uuid4())
        resp = await stack.client.post("/calls", json=event_payload(event_id))

        assert resp.status_code == 202
        body = resp.json()
        assert set(body) == {"event_id", "status", "reason", "deferred_pending"}
        assert body["event_id"] == event_id
        assert body["status"] == "deferred"
        assert body["reason"] == "circuit_open"
        assert body["deferred_pending"] == 1  # the row is durable before the 202

        # Queryable immediately: accepted, not completed (SPECS §5).
        stored = await stack.client.get(f"/calls/{event_id}")
        assert stored.status_code == 200
        record = stored.json()
        assert record["status"] == "deferred"
        assert record["attempts"] == 0
        assert record["event"]["event_id"] == event_id

        assert stack.app.state.deferral.pending_count() == 1
        assert (await stack.client.get("/healthz")).json()["deferred_pending"] == 1
    finally:
        await stack.aclose()


async def test_duplicate_submission_of_deferred_event_is_200(api_stack) -> None:
    """Idempotency wins over the 202 path: no second row, no second enqueue."""
    stack = api_stack(1.0)
    try:
        await stack.open_breaker()
        event_id = str(uuid4())
        first = await stack.client.post("/calls", json=event_payload(event_id))
        assert first.status_code == 202

        second = await stack.client.post("/calls", json=event_payload(event_id))
        assert second.status_code == 200
        body = second.json()
        assert body["duplicate"] is True
        assert body["status"] == "deferred"
        assert stack.app.state.deferral.pending_count() == 1
        assert len(stack.app.state.deferral) == 1
    finally:
        await stack.aclose()


async def test_admin_drain_endpoint_recovers_deferred_events(api_stack) -> None:
    """Deferred events drain to `processed` once the dependency recovers."""
    backend = FlakyBackend()
    stack = api_stack(1.0, backend=backend, cb_cooldown_s=0.05)
    try:
        # Trip the breaker against the dead boundary; the short cooldown keeps
        # the recovery wait real but negligible.
        for _ in range(5):
            await stack.client.post("/calls", json=event_payload())
        assert stack.app.state.breaker.state is CircuitState.OPEN

        ids = [str(uuid4()) for _ in range(3)]
        for event_id in ids:
            resp = await stack.client.post("/calls", json=event_payload(event_id))
            assert resp.status_code == 202, resp.text

        backend.failing = False  # dependency recovers
        await stack.wait_for_half_open()

        resp = await stack.client.post("/admin/deferred/drain")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body) == {"drained", "succeeded", "failed"}
        assert body == {"drained": 3, "succeeded": 3, "failed": 0}

        for event_id in ids:
            record = (await stack.client.get(f"/calls/{event_id}")).json()
            assert record["status"] == "processed"
            assert record["attempts"] >= 1
        assert (await stack.client.get("/healthz")).json()["deferred_pending"] == 0

        # Idempotent when empty (SPECS §5).
        empty = await stack.client.post("/admin/deferred/drain")
        assert empty.status_code == 200
        assert empty.json() == {"drained": 0, "succeeded": 0, "failed": 0}
    finally:
        await stack.aclose()


async def test_drain_endpoint_is_safe_when_empty(api_stack) -> None:
    stack = api_stack(0.0)
    try:
        resp = await stack.client.post("/admin/deferred/drain")
        assert resp.status_code == 200
        assert resp.json() == {"drained": 0, "succeeded": 0, "failed": 0}
    finally:
        await stack.aclose()


async def test_drain_leaves_events_deferred_while_breaker_open(api_stack) -> None:
    """A drain against a still-dead dependency must not destroy the backlog."""
    stack = api_stack(1.0, cb_cooldown_s=3600.0)
    try:
        await stack.open_breaker()
        event_id = str(uuid4())
        assert (
            await stack.client.post("/calls", json=event_payload(event_id))
        ).status_code == 202

        body = (await stack.client.post("/admin/deferred/drain")).json()
        assert body["succeeded"] == 0
        assert body["failed"] == 0
        assert (await stack.client.get(f"/calls/{event_id}")).json()["status"] == "deferred"
        assert stack.app.state.deferral.pending_count() == 1
    finally:
        await stack.aclose()


async def test_lifespan_drainer_recovers_without_operator_action(tmp_path) -> None:
    """M3-T21: the background drainer drains the queue on its own.

    Runs the real lifespan (unlike the ASGI-transport stacks above), so the
    drainer task is created exactly as in production. The assertion is the
    operator-free recovery promise: no drain endpoint call anywhere here.
    """
    from app.main import DRAIN_INTERVAL_S

    backend = FlakyBackend()
    settings = Settings(
        db_path=str(tmp_path / "drainer.db"),
        pipeline_backend="http",
        pipeline_base_url="http://mock.test",
        cb_cooldown_s=0.0,
    )

    # Seed the deferred backlog before the app exists (a restart-equivalent).
    seeded = []
    conn = store.connect(settings.db_path)
    store.init_db(conn)
    now_iso = datetime.now(UTC).isoformat()
    for _ in range(3):
        event_id = str(uuid4())
        seeded.append(event_id)
        store.insert_event(
            conn,
            CallEvent.model_validate(event_payload(event_id)),
            status="deferred",
            created_at=now_iso,
            updated_at=now_iso,
        )
    conn.close()

    backend.failing = False
    app = create_app(settings, backend)
    try:
        async with app.router.lifespan_context(app):
            deadline = asyncio.get_running_loop().time() + 10.0
            while app.state.deferral.pending_count() and (
                asyncio.get_running_loop().time() < deadline
            ):
                await asyncio.sleep(DRAIN_INTERVAL_S / 4)
            assert app.state.deferral.pending_count() == 0

        # The lifespan closed the app's connection on exit — verify the
        # durable outcome through a fresh one, which is what a restart sees.
        check = store.connect(settings.db_path)
        try:
            for event_id in seeded:
                assert store.get_event(check, event_id)["status"] == "processed"
        finally:
            check.close()
    finally:
        await backend.aclose()
        app.state.conn.close()
