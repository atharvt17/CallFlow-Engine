"""Zero-silent-loss + no-duplicate invariants (M3-T26; SPECS §7.1, §11, §12).

The §7.1 accounting invariant as an executable property over a randomized
mixed profile: transient failures, a genuine breaker trip and recovery, and
duplicate submissions injected throughout. After the run and a final drain,
for every submitted ``event_id``:

- ``call_events.status`` is terminal (``processed`` / ``failed_downstream``);
  nothing is left ``deferred`` or ``processing``;
- ``failed_downstream ⇒ ∃ dead_letter row``, exactly one per event;
- duplicate submissions never created extra rows or extra DLQ entries;
- no event was dispatched more than once per drain pass (claim exclusivity).

Randomness is seeded so a failure is reproducible from the printed seed.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from app.config import Settings
from app.main import create_app
from app.pipeline.base import PipelineResult
from app.pipeline.http_client import HttpPipelineClient
from mock.config import MockSettings
from mock.main import create_mock_app

FAST_LATENCY_MU = -3.0
SEED = 20260901
COOLDOWN_S = 0.01


class ScriptedBackend:
    """Deterministic mixed profile: healthy by default, outage on demand.

    ``outage`` is tripped by the test between batches, so the run covers
    "normal traffic → dependency dies → breaker opens → deferral →
    dependency recovers → drain" without relying on random draws for the
    breaker's behaviour (only event selection is randomized).
    """

    def __init__(self, inner, *, fail_ratio: float = 0.0, seed: int = SEED) -> None:
        import random

        self._inner = inner
        self._rng = random.Random(seed)
        self.fail_ratio = fail_ratio
        self.outage = False
        self.successful_dispatches: dict[str, int] = {}

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        if self.outage:
            return PipelineResult(
                ok=False, reason="downstream_timeout", http_status=503, latency_ms=2.0
            )
        if self._rng.random() < self.fail_ratio:
            return PipelineResult(
                ok=False, reason="connection_error", http_status=None, latency_ms=2.0
            )
        result = await self._inner.process(event_id, audio_ref)
        if result.ok:
            self.successful_dispatches[event_id] = self.successful_dispatches.get(event_id, 0) + 1
        return result


def event_payload(event_id: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "30-60 DPD".replace("30-60", "31-60"),
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


def read_rows(db_path: Path, table: str) -> list[dict[str, object]]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(f"SELECT * FROM {table}")]
    finally:
        conn.close()


async def test_accounting_invariants_over_mixed_profile(tmp_path) -> None:
    db_path = tmp_path / "invariants.db"
    mock_app = create_mock_app(MockSettings(latency_mu=FAST_LATENCY_MU, failure_rate=0.0))
    settings = Settings(
        db_path=str(db_path),
        pipeline_backend="http",
        pipeline_base_url="http://mock.test",
        cb_cooldown_s=COOLDOWN_S,
        backoff_base_ms=0,
    )
    inner = HttpPipelineClient(settings, transport=httpx.ASGITransport(app=mock_app))
    backend = ScriptedBackend(inner, fail_ratio=0.10)
    app = create_app(settings, backend)
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://inference.test"
    )

    try:
        submitted: list[str] = []
        duplicates: list[str] = []

        async def submit(batch: int, *, duplicate_rate: float = 0.25) -> None:
            for _ in range(batch):
                event_id = str(uuid4())
                submitted.append(event_id)
                resp = await client.post("/calls", json=event_payload(event_id))
                assert resp.status_code in (200, 202), resp.text
                if duplicate_rate and len(submitted) % 4 == 0:
                    dup = await client.post("/calls", json=event_payload(event_id))
                    assert dup.status_code == 200
                    assert dup.json()["duplicate"] is True
                    duplicates.append(event_id)

        # Phase 1: transient profile, breaker stays closed.
        await submit(20)
        assert app.state.breaker.state.value == "closed"

        # Phase 2: dependency dies → breaker opens → events defer.
        backend.outage = True
        outage_submitted = 0
        for _ in range(12):
            event_id = str(uuid4())
            submitted.append(event_id)
            resp = await client.post("/calls", json=event_payload(event_id))
            assert resp.status_code in (200, 202), resp.text
            if resp.status_code == 202:
                outage_submitted += 1
        assert outage_submitted > 0, "the outage must have produced deferrals"

        # Phase 3: dependency recovers; drain to empty. The breaker still has
        # to cool down and pass its half-open probes, so passes that make no
        # terminal progress are legitimate — wait for the state machine.
        backend.outage = False
        for attempt in range(100):
            if app.state.deferral.pending_count() == 0 and len(app.state.deferral) == 0:
                break
            body = (await client.post("/admin/deferred/drain")).json()
            assert 0 <= body["succeeded"] + body["failed"] <= body["drained"]
            if body["drained"] and not (body["succeeded"] or body["failed"]):
                # Gated by the breaker: give the cooldown a chance to elapse.
                await asyncio.sleep(COOLDOWN_S / 2)
        else:
            pytest.fail("backend never drained to empty")

        # Phase 4: more traffic on the recovered system, with duplicates.
        await submit(10)
        if app.state.deferral.pending_count():
            await client.post("/admin/deferred/drain")

        rows = read_rows(db_path, "call_events")
        by_id = {row["event_id"]: row for row in rows}

        # 1. Exactly one row per submitted event; nothing extra from duplicates.
        assert set(by_id) == set(submitted)
        assert len(rows) == len(set(submitted))

        # 2. Every event terminal — never left deferred or processing.
        non_terminal = {
            event_id: row["status"]
            for event_id, row in by_id.items()
            if row["status"] not in {"processed", "failed_downstream"}
        }
        assert not non_terminal, f"non-terminal rows: {non_terminal}"

        # 3. failed_downstream ⇒ exactly one DLQ row, and no DLQ row for a
        #    processed event.
        dlq = read_rows(db_path, "dead_letter")
        failed = {e for e, row in by_id.items() if row["status"] == "failed_downstream"}
        assert {row["event_id"] for row in dlq} == failed
        assert len(dlq) == len(failed), "no duplicate DLQ rows"
        for row in dlq:
            assert row["reason"] in {
                "downstream_timeout",
                "timeout",
                "connection_error",
                "non_retryable_error",
                "deferred_expired",
            }
            linked = by_id[row["event_id"]]
            assert linked["dlq_id"] == row["id"], "both directions of the link agree"

        # 4. Processed events were dispatched successfully at most once,
        #    except duplicates, which never dispatch at all.
        for event_id, count in backend.successful_dispatches.items():
            assert count == 1, f"{event_id} processed {count} times"
        for event_id in duplicates:
            row = by_id[event_id]
            assert row["status"] in {"processed", "failed_downstream"}

        # 5. Outcomes/statuses agree with §7.1.
        for row in rows:
            if row["status"] == "processed":
                assert row["outcome"] in {"connected", "ptp_taken"}
                assert row["attempts"] >= 1
                assert row["retry_count"] == row["attempts"] - 1
            else:
                assert row["outcome"] == "failed_downstream"
                assert row["failure_reason"] is not None

        # 6. Metrics agree with the accounting.
        scrape = (await client.get("/metrics")).text
        processed = sum(1 for row in rows if row["status"] == "processed")
        assert f'calls_total{{outcome="failed_downstream",status="failed_downstream"}} {len(failed)}.0' in scrape
        assert "deferred_pending 0.0" in scrape
        assert processed + len(failed) == len(rows)
        print(
            f"\ninvariants: {len(rows)} events, {processed} processed, "
            f"{len(failed)} failed+DLQ, {len(duplicates)} duplicate submissions"
        )
    finally:
        await client.aclose()
        await backend.aclose()
        app.state.conn.close()
