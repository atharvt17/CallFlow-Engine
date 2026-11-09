"""Sprint 3 resilience integration proof (M3-T15; SPECS §6.1, §6.2, §11).

Drives the real inference app against the in-process Appendix B mock over the
full HTTP boundary — no breaker or retry stubs — to prove the four scored
behaviours:

1. **Transient 20% does not trip.** Per-attempt failure 0.20 over 3 attempts
   ⇒ P(logical failure) = 0.2³ = 0.008. The breaker must stay CLOSED and its
   window must hold exactly one outcome per logical call.
2. **Sustained 100% trips and then fails fast.** Opens within ≤ 5 consecutive
   logical failures; while open, zero downstream attempts are made and events
   are persisted as ``deferred`` (the 202/persistence path is M3-T16).
3. **Recovery.** Restore the mock, wait out the cooldown, then two successful
   probes close the breaker.
4. **Dead endpoint.** Connection failures trip the same breaker, and
   ``GET /admin/circuit-breaker`` reflects every transition.

The mock is mutated through its documented admin extension, so failure rates
switch mid-test without rebuilding the app. Cooldown-driven waits are real
(the breaker uses the monotonic clock) but bounded to the 5 s default.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx
import pytest

from app.config import Settings
from app.main import create_app
from app.pipeline.http_client import HttpPipelineClient
from app.resilience.circuit_breaker import CircuitState
from mock.config import MockSettings
from mock.main import create_mock_app

FAST_LATENCY_MU = -3.0
COOLDOWN_S = 0.05  # keep real cooldown waits negligible
RECOVERY_TIMEOUT_S = 5.0


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


class ResilienceStack:
    """Inference app + mock app wired over ASGI, with live admin knobs."""

    def __init__(self, tmp_path, *, failure_rate: float, **inference_overrides):
        self.mock_app = create_mock_app(
            MockSettings(latency_mu=FAST_LATENCY_MU, failure_rate=failure_rate)
        )
        self.db_path = tmp_path / "resilience.db"
        overrides = {"cb_cooldown_s": COOLDOWN_S, "backoff_base_ms": 0, **inference_overrides}
        self.settings = Settings(
            db_path=str(self.db_path),
            pipeline_backend="http",
            pipeline_base_url="http://mock.test",
            **overrides,
        )
        self.backend = HttpPipelineClient(
            self.settings, transport=httpx.ASGITransport(app=self.mock_app)
        )
        self.app = create_app(self.settings, self.backend)
        self.breaker = self.app.state.breaker

    async def __aenter__(self) -> tuple[httpx.AsyncClient, ResilienceStack]:
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://inference.test"
        )
        return self.client, self

    async def __aexit__(self, *exc) -> None:
        await self.client.aclose()
        await self.backend.aclose()
        self.app.state.conn.close()

    async def set_mock_failure_rate(self, rate: float) -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.mock_app), base_url="http://mock.test"
        ) as mock:
            resp = await mock.post("/admin/config", json={"failure_rate": rate})
            assert resp.status_code == 200
            assert resp.json()["failure_rate"] == rate

    async def mock_requests_total(self) -> int:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.mock_app), base_url="http://mock.test"
        ) as mock:
            return (await mock.get("/admin/stats")).json()["requests_total"]

    async def wait_for_state(self, state: CircuitState) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + RECOVERY_TIMEOUT_S
        while self.breaker.state is not state:
            if loop.time() > deadline:
                raise AssertionError(
                    f"breaker stayed {self.breaker.state.value!r}, wanted {state.value!r}"
                )
            await asyncio.sleep(0.01)


@pytest.fixture
def resilience_stack(tmp_path):
    def _factory(failure_rate: float, **inference_overrides) -> ResilienceStack:
        return ResilienceStack(tmp_path, failure_rate=failure_rate, **inference_overrides)

    return _factory


# --- 1. Transient profile never trips ------------------------------------------


async def test_twenty_percent_profile_does_not_trip(resilience_stack) -> None:
    """≥100 logical calls at the Appendix B 20% rate: CLOSED throughout, one
    window entry per logical call, terminal failures near 0.8%."""
    n = 120
    async with resilience_stack(failure_rate=0.20) as (client, stack):
        outcomes = []
        for _ in range(n):
            resp = await client.post("/calls", json=event_payload())
            assert resp.status_code == 200
            outcomes.append(resp.json())
            assert stack.breaker.state is CircuitState.CLOSED

        failed = [body for body in outcomes if body["status"] == "failed_downstream"]
        # SPECS §6.2 math: 0.2³ = 0.8% expected; 120 calls ⇒ ~1. Tolerance is
        # deliberately loose (binomial tail), but the ratio-trip threshold (50%)
        # is hundreds of times away — the assertion that matters is CLOSED.
        assert len(failed) <= 10, f"{len(failed)}/{n} terminal failures exceeds tolerance"

        # Breaker statistics see logical calls only.
        assert stack.breaker.window_samples == min(n, stack.breaker.window_size)
        assert stack.breaker.consecutive_failures < 5
        assert stack.breaker.state is CircuitState.CLOSED

        # Retries stay visible at attempt level and never inflate the window.
        attempts = sum(body["attempts"] for body in outcomes)
        assert attempts > n, "the 20% per-attempt rate must have produced retries"
        assert stack.breaker.window_samples == min(n, stack.breaker.window_size)

        snapshot = (await client.get("/admin/circuit-breaker")).json()
        assert snapshot["state"] == "closed"
        assert snapshot["last_transition_at"] is None


# --- 2. Sustained failure trips and fails fast --------------------------------


async def test_total_failure_opens_and_fails_fast(resilience_stack) -> None:
    async with resilience_stack(failure_rate=1.0) as (client, stack):
        bodies = []
        for _ in range(5):
            resp = await client.post("/calls", json=event_payload())
            bodies.append(resp.json())

        assert stack.breaker.state is CircuitState.OPEN
        assert all(body["status"] == "failed_downstream" for body in bodies)
        assert all(body["attempts"] == 3 for body in bodies)

        requests_before = await stack.mock_requests_total()
        for _ in range(5):
            body = (await client.post("/calls", json=event_payload())).json()
            # Sprint 4 persists deferred events and answers 202 with the
            # circuit_open reason; the signal that lands in this sprint is the
            # service-side deferred status with zero dispatched attempts.
            assert body["status"] == "deferred"
            assert body["attempts"] == 0
        requests_after = await stack.mock_requests_total()

        assert requests_after == requests_before, "open breaker must not dispatch"
        # Rejections never enter the breaker window (SPECS §6.2).
        assert stack.breaker.window_samples == 5

        snapshot = (await client.get("/admin/circuit-breaker")).json()
        assert snapshot["state"] == "open"
        assert snapshot["opened_at"] is not None
        assert snapshot["consecutive_failures"] == 5


async def test_opens_within_five_consecutive_logical_failures(resilience_stack) -> None:
    async with resilience_stack(failure_rate=1.0) as (client, stack):
        for i in range(1, 6):
            await client.post("/calls", json=event_payload())
            if i < 5:
                assert stack.breaker.state is CircuitState.CLOSED, f"tripped at {i}"
        assert stack.breaker.state is CircuitState.OPEN


async def test_deferred_events_are_persisted_and_queryable(resilience_stack) -> None:
    """Nothing is lost while the breaker is open: rows exist and GET returns
    them (the 202 body is M3-T16)."""
    async with resilience_stack(failure_rate=1.0) as (client, stack):
        for _ in range(5):
            await client.post("/calls", json=event_payload())
        assert stack.breaker.state is CircuitState.OPEN

        event_id = str(uuid4())
        posted = (await client.post("/calls", json=event_payload(event_id))).json()
        assert posted["status"] == "deferred"

        stored = await client.get(f"/calls/{event_id}")
        assert stored.status_code == 200
        assert stored.json()["status"] == "deferred"


# --- 3. Recovery via half-open ------------------------------------------------


async def test_recovery_after_two_successful_probes(resilience_stack) -> None:
    async with resilience_stack(failure_rate=1.0) as (client, stack):
        for _ in range(5):
            await client.post("/calls", json=event_payload())
        assert stack.breaker.state is CircuitState.OPEN

        await stack.set_mock_failure_rate(0.0)
        await stack.wait_for_state(CircuitState.HALF_OPEN)
        assert (await client.get("/admin/circuit-breaker")).json()["state"] == "half_open"

        first = (await client.post("/calls", json=event_payload())).json()
        assert first["status"] == "processed"
        assert stack.breaker.state is CircuitState.HALF_OPEN

        second = (await client.post("/calls", json=event_payload())).json()
        assert second["status"] == "processed"
        assert stack.breaker.state is CircuitState.CLOSED
        assert stack.breaker.window_samples == 0  # reset on close

        snapshot = (await client.get("/admin/circuit-breaker")).json()
        assert snapshot["state"] == "closed"
        assert snapshot["last_transition_at"] is not None

        # Normal processing resumed.
        third = (await client.post("/calls", json=event_payload())).json()
        assert third["status"] == "processed"
        assert stack.breaker.window_samples == 1


async def test_failed_probe_reopens_and_restarts_cooldown(resilience_stack) -> None:
    async with resilience_stack(failure_rate=1.0) as (client, stack):
        for _ in range(5):
            await client.post("/calls", json=event_payload())
        opened_at = stack.breaker.opened_at
        assert stack.breaker.state is CircuitState.OPEN

        await stack.wait_for_state(CircuitState.HALF_OPEN)
        body = (await client.post("/calls", json=event_payload())).json()
        assert body["status"] == "failed_downstream"
        assert body["attempts"] == 3  # one logical call, three attempts
        assert stack.breaker.state is CircuitState.OPEN
        assert stack.breaker.opened_at != opened_at  # cooldown restarted


# --- 4. Dead endpoint ---------------------------------------------------------


async def test_dead_endpoint_opens_breaker(tmp_path) -> None:
    """Connection refused behaves like total downstream death."""
    settings = Settings(
        db_path=str(tmp_path / "dead.db"),
        pipeline_backend="http",
        pipeline_base_url="http://127.0.0.1:9",  # discard port: refused
        pipeline_timeout_s=0.25,
        backoff_base_ms=0,
        cb_cooldown_s=COOLDOWN_S,
    )
    backend = HttpPipelineClient(settings)
    app = create_app(settings, backend)
    conn = app.state.conn
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://inference.test"
        ) as client:
            for _ in range(5):
                body = (await client.post("/calls", json=event_payload())).json()
                assert body["status"] == "failed_downstream"
                # No listener on the discard port: the platform may surface
                # this as an immediate refusal (connection_error) or hang until
                # the per-attempt timeout fires (timeout). Both are downstream
                # death and both are retryable — and that is the point here.
                assert body["failure_reason"] in {"connection_error", "timeout"}
                assert body["attempts"] == 3

            assert app.state.breaker.state is CircuitState.OPEN

            rejected = (await client.post("/calls", json=event_payload())).json()
            assert rejected["status"] == "deferred"
            assert rejected["attempts"] == 0

            snapshot = (await client.get("/admin/circuit-breaker")).json()
            assert snapshot["state"] == "open"

            metrics = (await client.get("/metrics")).text
            assert (
                'circuit_transitions_total{from="closed",to="open"} 1.0' in metrics
            )
            assert "circuit_state 1.0" in metrics
    finally:
        await backend.aclose()
        conn.close()


# --- Cross-cutting: the admin surface tracks every transition -----------------


async def test_admin_snapshot_reflects_full_transition_history(resilience_stack) -> None:
    async with resilience_stack(failure_rate=1.0) as (client, stack):
        for _ in range(5):
            await client.post("/calls", json=event_payload())
        await stack.set_mock_failure_rate(0.0)
        await stack.wait_for_state(CircuitState.HALF_OPEN)

        for _ in range(2):
            await client.post("/calls", json=event_payload())
        assert stack.breaker.state is CircuitState.CLOSED

        metrics = (await client.get("/metrics")).text
        for pair in (
            'circuit_transitions_total{from="closed",to="open"} 1.0',
            'circuit_transitions_total{from="open",to="half_open"} 1.0',
            'circuit_transitions_total{from="half_open",to="closed"} 1.0',
        ):
            assert pair in metrics
        assert "circuit_state 0.0" in metrics
