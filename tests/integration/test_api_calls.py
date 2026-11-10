"""End-to-end Sprint 2 API proof against a real (test) mock (M2-T09).

The inference app runs with its HTTP backend transport-injected onto an
in-process mock app, driving the full POST /calls -> backend -> mock -> store
flow. Accounting assertions read the store directly.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

EVENT_ID = "a1f2c3d4-e89b-12d3-a456-426614174000"


def event_payload(event_id: str | None = None) -> dict[str, object]:
    """Minimal valid Appendix A event with all enum values legal."""
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
        "call_duration_s": 47,
    }


def fetch_row(db_path: Path, event_id: str) -> dict[str, object] | None:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM call_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


def terminal_counts(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT status, COUNT(*) AS n FROM call_events GROUP BY status")
        return {row["status"]: row["n"] for row in rows}
    finally:
        conn.close()


async def test_valid_event_processed_with_section5_body(make_stack) -> None:
    async with make_stack() as (client, db_path):
        event_id = str(uuid4())
        resp = await client.post("/calls", json=event_payload(event_id))

        assert resp.status_code == 200
        body = resp.json()
        assert body["event_id"] == event_id
        assert body["status"] == "processed"
        assert body["outcome"] == "connected"
        assert body["attempts"] == 1
        assert body["retry_count"] == 0
        assert isinstance(body["pipeline_latency_ms"], int)
        assert body["e2e_latency_ms"] > 0
        assert "duplicate" not in body and "failure_reason" not in body

        row = fetch_row(db_path, event_id)
        assert row is not None
        assert row["status"] == "processed" and row["outcome"] == "connected"
        assert row["event_json"]  # restart reconstruction source persisted


async def test_invalid_events_rejected_422(make_stack) -> None:
    async with make_stack() as (client, db_path):
        bad_payloads = [
            event_payload() | {"outcome": "hung_up"},  # out-of-enum
            event_payload() | {"borrower_bucket": "31-70 DPD"},  # out-of-enum
            event_payload() | {"event_id": "not-a-uuid"},  # malformed UUID
            event_payload() | {"timestamp": "not-a-date"},  # malformed RFC3339
            event_payload() | {"extra": 1},  # unknown field
        ]
        for payload in bad_payloads:
            resp = await client.post("/calls", json=payload)
            assert resp.status_code == 422, payload
        counts = terminal_counts(db_path)
        assert counts == {}, "rejected events must not be persisted"


async def test_duplicate_submit_is_idempotent(make_stack) -> None:
    event_id = str(uuid4())
    async with make_stack() as (client, db_path):
        first = await client.post("/calls", json=event_payload(event_id))
        assert first.status_code == 200
        assert "duplicate" not in first.json()

        second = await client.post("/calls", json=event_payload(event_id))
        assert second.status_code == 200
        body = second.json()
        assert body["duplicate"] is True
        assert body["status"] == "processed"
        assert body["event_id"] == event_id

        rows = terminal_counts(db_path)
        assert rows == {"processed": 1}, "duplicate must not add a row"


async def test_get_call_record_and_unknown_404(make_stack) -> None:
    async with make_stack() as (client, _db_path):
        event_id = str(uuid4())
        await client.post("/calls", json=event_payload(event_id))

        resp = await client.get(f"/calls/{event_id}")
        assert resp.status_code == 200
        record = resp.json()
        assert record["event_id"] == event_id
        assert record["status"] == "processed"
        assert record["input_outcome"] == "connected"  # audit copy of incoming outcome
        assert record["campaign_id"] == "CAMP-2026-09-A"
        assert record["event"]["event_id"] == event_id  # parsed original payload

        missing = await client.get("/calls/00000000-0000-4000-8000-0000000000ff")
        assert missing.status_code == 404
        assert "detail" in missing.json()


async def test_forced_total_failure_yields_failed_downstream(make_stack) -> None:
    async with make_stack(mock_overrides={"failure_rate": 1.0}) as (client, db_path):
        event_id = str(uuid4())
        resp = await client.post("/calls", json=event_payload(event_id))

        assert resp.status_code == 200  # terminal failure, not an HTTP error
        body = resp.json()
        assert body["status"] == "failed_downstream"
        assert body["outcome"] == "failed_downstream"
        # Resilience is live (M3-T14): the 503 is retryable, so one logical call
        # uses all 3 attempts (SPECS §6.1) and reports retry_count = attempts − 1.
        assert body["attempts"] == 3
        assert body["retry_count"] == 2
        assert body["failure_reason"] == "downstream_timeout"
        # M3-T20: the terminal failure wrote its complete DLQ row.
        assert isinstance(body["dlq_id"], int)

        row = fetch_row(db_path, event_id)
        assert row is not None
        assert row["status"] == "failed_downstream"


async def test_no_silent_losses_under_transient_failures(make_stack) -> None:
    """25 events at the shipped 20% failure rate -> every one terminal."""
    n = 25
    event_ids = [str(uuid4()) for _ in range(n)]
    # Explicit 20% (Appendix B default) — the fixture now defaults to 0%.
    async with make_stack(mock_overrides={"failure_rate": 0.20}) as (client, db_path):
        for event_id in event_ids:
            resp = await client.post("/calls", json=event_payload(event_id))
            assert resp.status_code == 200, "failures are terminal outcomes, not HTTP errors"

        conn = sqlite3.connect(db_path)
        try:
            conn.row_factory = sqlite3.Row
            rows = {
                row["event_id"]: row["status"]
                for row in conn.execute("SELECT event_id, status FROM call_events")
            }
        finally:
            conn.close()

        assert set(rows) == set(event_ids)  # N submitted -> N rows, none missing
        assert all(status in ("processed", "failed_downstream") for status in rows.values()), (
            "every event must reach a terminal state (zero silent losses)"
        )
        statuses = list(rows.values())
        processed = statuses.count("processed")
        failed = statuses.count("failed_downstream")
        print(f"\naccounting: {processed} processed / {failed} failed_downstream of {n}")
        assert processed + failed == n
