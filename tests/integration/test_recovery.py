"""Restart-recovery acceptance scenario (M3-T25; SPECS §6.3, §11; PLAN A3).

The nine steps, automated against the real HTTP boundary:

1. the dependency fails 100% of requests;
2. multiple events are submitted;
3. the breaker opens;
4. each event is persisted ``deferred`` and answered ``202``;
5. the inference app is **restarted** — a new app instance over the same DB
   file, exactly what a container restart produces;
6. the mock is restored to healthy;
7. pending deferred events are recovered from SQLite on boot;
8. every event becomes ``processed`` or ``failed_downstream``;
9. zero duplicate processing and zero silent loss.

Restoring the breaker across a restart is deliberately *not* modelled: a
fresh process starts CLOSED, which is why recovery works at all. Attempt
counts are asserted against the backend's dispatch log, so "exactly once"
means the dependency saw the event, not just that a row says ``processed``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

import httpx

from app.config import Settings
from app.main import create_app
from app.pipeline.base import PipelineResult
from app.resilience.circuit_breaker import CircuitState
from mock.config import MockSettings
from mock.main import create_mock_app

FAST_LATENCY_MU = -3.0
COOLDOWN_S = 0.05


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


class Process:
    """One inference process (app + connection + HTTP client) over a DB file."""

    def __init__(self, db_path: Path, backend, settings: Settings) -> None:
        self.app = create_app(settings, backend)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://inference.test"
        )
        self.backend = backend

    async def close(self) -> None:
        await self.client.aclose()
        await self.backend.aclose()
        self.app.state.conn.close()


async def submit_all(client: httpx.AsyncClient, ids: list[str]) -> list[httpx.Response]:
    return [await client.post("/calls", json=event_payload(event_id)) for event_id in ids]


def statuses(db_path: Path) -> dict[str, str]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        return {
            row["event_id"]: row["status"]
            for row in conn.execute("SELECT event_id, status FROM call_events")
        }
    finally:
        conn.close()


def dlq_rows(db_path: Path) -> list[dict[str, object]]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM dead_letter")]
    finally:
        conn.close()


class FlakyMockBackend:
    """ASGI backend to the mock with a mutable health switch.

    Wraps ``HttpPipelineClient`` so the restart scenario controls dependency
    health deterministically instead of relying on the mock's random draw at
    a 100% rate (which is equivalent, but this keeps the dispatch log exact).
    """

    def __init__(self, mock_app, settings: Settings) -> None:
        from app.pipeline.http_client import HttpPipelineClient

        self.failing = True
        self._inner = HttpPipelineClient(settings, transport=httpx.ASGITransport(app=mock_app))
        self.dispatches: list[str] = []

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        if self.failing:
            self.dispatches.append(event_id)
            return PipelineResult(
                ok=False, reason="downstream_timeout", http_status=503, latency_ms=5.0
            )
        result = await self._inner.process(event_id, audio_ref)
        if result.ok:
            self.dispatches.append(event_id)
        return result


async def test_nine_step_restart_recovery(tmp_path) -> None:
    db_path = tmp_path / "restart.db"
    mock_app = create_mock_app(MockSettings(latency_mu=FAST_LATENCY_MU, failure_rate=0.0))
    settings = Settings(
        db_path=str(db_path),
        pipeline_backend="http",
        pipeline_base_url="http://mock.test",
        cb_cooldown_s=COOLDOWN_S,
        backoff_base_ms=0,
    )

    # --- steps 1-4: dead dependency, breaker opens, events deferred ---
    backend = FlakyMockBackend(mock_app, settings)
    first = Process(db_path, backend, settings)
    try:
        ids = [str(uuid4()) for _ in range(8)]

        assert first.app.state.breaker.state is CircuitState.CLOSED  # step 1 result
        responses = await submit_all(first.client, ids[:5])  # step 2
        assert all(resp.status_code == 200 for resp in responses)
        assert first.app.state.breaker.state is CircuitState.OPEN  # step 3

        # step 4: new events while open are persisted-and-deferred, 202.
        second_batch = await submit_all(first.client, ids[5:])
        assert [resp.status_code for resp in second_batch] == [202] * 3
        for resp in second_batch:
            body = resp.json()
            assert body["status"] == "deferred" and body["reason"] == "circuit_open"

        before_restart = statuses(db_path)
        assert set(before_restart) == set(ids)
        assert list(before_restart.values()).count("deferred") == 3
        assert first.app.state.deferral.pending_count() == 3
    finally:
        await first.close()

    # --- step 5: restart — a NEW process over the same DB file ---
    backend2 = FlakyMockBackend(mock_app, settings)
    backend2.failing = False  # step 6: dependency restored
    recurring = Process(db_path, backend2, settings)

    try:
        # step 7: the queue was reconstructed from SQLite at boot.
        assert recurring.app.state.deferral.pending_count() == 3
        assert len(recurring.app.state.deferral) == 3
        assert recurring.app.state.breaker.state is CircuitState.CLOSED

        # Drain through the normal operator path (the real drainer task needs
        # lifespan, which ASGI transports skip; the code path is identical).
        drains = []
        while recurring.app.state.deferral.pending_count():
            body = (await recurring.client.post("/admin/deferred/drain")).json()
            drains.append(body)
            assert body["drained"] > 0, f"drain made no progress: {body}"

        # step 8: every event terminal.
        final = statuses(db_path)
        assert set(final) == set(ids)
        assert all(status in {"processed", "failed_downstream"} for status in final.values())
        assert "deferred" not in final.values() and "processing" not in final.values()

        # step 9a: zero silent loss — failed_downstream implies a DLQ row.
        failed = {event_id for event_id, status in final.items() if status == "failed_downstream"}
        dlq = dlq_rows(db_path)
        assert {row["event_id"] for row in dlq} == failed
        assert len(dlq) == len(failed), "exactly one DLQ row per failed event"

        # step 9b: zero duplicate processing — no event reached the healthy
        # dependency more than once after recovery, and the first five were
        # never re-dispatched (they failed terminally before the restart).
        processed = {event_id for event_id, status in final.items() if status == "processed"}
        post_restart_dispatches = [e for e in backend2.dispatches if e in processed]
        assert len(post_restart_dispatches) == len(processed)
        assert len(set(post_restart_dispatches)) == len(post_restart_dispatches)
        # The pre-restart process never saw any successful dispatch.
        assert backend.dispatches and not set(backend.dispatches) & processed

        # Re-running recovery is a no-op (idempotent, SPECS §6.3).
        report = recurring.app.state.deferral.recover()
        assert (report.recovered, report.orphans) == (0, 0)

        # A second restart over the fully recovered store recovers nothing new.
        await recurring.client.aclose()
        third_backend = FlakyMockBackend(mock_app, settings)
        third = Process(db_path, third_backend, settings)
        try:
            assert third.app.state.deferral.pending_count() == 0
            assert len(third.app.state.deferral) == 0
        finally:
            await third.close()
    finally:
        # ``recurring``'s client may already be closed in the happy path.
        if not recurring.client.is_closed:
            await recurring.client.aclose()
        await recurring.backend.aclose()
        recurring.app.state.conn.close()
        await backend2.aclose()


async def test_restart_recovers_processing_orphans(tmp_path) -> None:
    """A crash mid-flight leaves ``processing``; the next boot requeues it."""
    db_path = tmp_path / "orphan.db"
    mock_app = create_mock_app(MockSettings(latency_mu=FAST_LATENCY_MU, failure_rate=0.0))
    settings = Settings(
        db_path=str(db_path),
        pipeline_backend="http",
        pipeline_base_url="http://mock.test",
        backoff_base_ms=0,
    )

    # Simulate the crash: a row claimed by a process that then died.
    from datetime import UTC, datetime

    from app import store as store_mod
    from app.models import CallEvent

    conn = store_mod.connect(db_path)
    store_mod.init_db(conn)
    orphan_id = str(uuid4())
    now_iso = datetime.now(UTC).isoformat()
    store_mod.insert_event(
        conn,
        CallEvent.model_validate(event_payload(orphan_id)),
        status="deferred",
        created_at=now_iso,
        updated_at=now_iso,
    )
    assert store_mod.claim_deferred_event(conn, orphan_id) is True
    assert store_mod.get_event(conn, orphan_id)["status"] == "processing"
    conn.close()

    backend = FlakyMockBackend(mock_app, settings)
    backend.failing = False
    restarted = Process(db_path, backend, settings)
    try:
        assert restarted.app.state.deferral.pending_count() == 1
        body = (await restarted.client.post("/admin/deferred/drain")).json()
        assert body == {"drained": 1, "succeeded": 1, "failed": 0}
        assert (await restarted.client.get(f"/calls/{orphan_id}")).json()["status"] == "processed"
    finally:
        await restarted.close()
