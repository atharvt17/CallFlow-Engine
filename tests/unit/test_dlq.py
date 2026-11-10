"""DLQ inspection surface (M4-T01/T05; SPECS §5, §7).

``GET /admin/dlq`` and ``GET /admin/dlq/{id}`` are the HTTP half of the
"inspectable" scored requirement. Payloads are asserted against the SPECS §7
columns — a reviewer must be able to see *why* an event died, how many
attempts it burned, and where it is in the replay lifecycle without opening
SQLite.

Rows are produced by the real write path (forced dependency failure) wherever
possible; the ``replayed`` state — populated by M4-T03 — is staged directly in
the store so the filter can be asserted before replay exists.
"""

from __future__ import annotations

import json
from uuid import uuid4

import httpx

from app import store
from app.models import CallEvent

# Full dead_letter column set (SPECS §7) — the payload must lose none of it.
DLQ_COLUMNS = {
    "id",
    "event_id",
    "reason",
    "attempts",
    "retry_count",
    "first_failed_at",
    "last_failed_at",
    "status",
    "replayed_at",
    "replay_outcome",
    "replay_event_id",
}


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


async def generate_failures(
    stack: tuple[httpx.AsyncClient, object], count: int
) -> list[dict[str, object]]:
    """Drive ``count`` events through a 100%-failing dependency.

    Every event exhausts its retries and lands in the DLQ through the real
    terminal-failure transaction (M3-T20), so the rows are production-shaped.
    Returns the ``POST /calls`` bodies in submission order.
    """
    client, _ = stack
    bodies = []
    for _ in range(count):
        resp = await client.post("/calls", json=event_payload())
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "failed_downstream"
        assert isinstance(body["dlq_id"], int)
        bodies.append(body)
    return bodies


def mark_replayed(db_path, dlq_id: int) -> None:
    """Stage the state M4-T03 will write, so filtering can be asserted now."""
    conn = store.connect(db_path)
    try:
        with store.begin_immediate(conn):
            conn.execute(
                "UPDATE dead_letter SET status = 'replayed', replayed_at = ?,"
                " replay_outcome = 'processed', replay_event_id = ? WHERE id = ?",
                ("2026-09-01T10:00:00+00:00", str(uuid4()), dlq_id),
            )
    finally:
        conn.close()


# --- list ------------------------------------------------------------------


async def test_list_defaults_to_pending_newest_first(make_stack) -> None:
    async with make_stack({"failure_rate": 1.0}, backoff_base_ms=0) as stack:
        client, _ = stack
        bodies = await generate_failures(stack, 3)

        resp = await client.get("/admin/dlq")
        assert resp.status_code == 200
        rows = resp.json()

        # Default filter is status=pending (SPECS §5 request shape).
        assert [row["id"] for row in rows] == [body["dlq_id"] for body in reversed(bodies)]
        assert {row["status"] for row in rows} == {"pending"}
        assert {row["event_id"] for row in rows} == {body["event_id"] for body in bodies}


async def test_list_rows_carry_the_full_spec7_payload(make_stack) -> None:
    async with make_stack({"failure_rate": 1.0}, backoff_base_ms=0) as stack:
        client, _ = stack
        bodies = await generate_failures(stack, 1)
        submitted = bodies[0]

        row = (await client.get("/admin/dlq")).json()[0]

        assert DLQ_COLUMNS <= set(row)
        assert row["id"] == submitted["dlq_id"]
        assert row["event_id"] == submitted["event_id"]
        assert row["reason"] == "downstream_timeout"
        assert row["attempts"] == 3
        assert row["retry_count"] == 2
        assert row["status"] == "pending"
        assert row["replayed_at"] is None
        assert row["replay_outcome"] is None
        assert row["replay_event_id"] is None
        assert row["first_failed_at"] and row["last_failed_at"]

        # ``event_json`` is parsed under ``event``; the raw string is gone.
        assert "event_json" not in row
        assert row["event"]["event_id"] == submitted["event_id"]
        assert row["event"]["campaign_id"] == "CAMP-2026-09-A"
        # The parsed payload round-trips the original submission (UUID and
        # timestamp normalized by pydantic, everything else identical).
        original = CallEvent.model_validate(
            event_payload(str(submitted["event_id"]))
        )
        assert row["event"] == json.loads(original.model_dump_json())


async def test_list_filters_by_replay_status(make_stack) -> None:
    async with make_stack({"failure_rate": 1.0}, backoff_base_ms=0) as stack:
        client, db_path = stack
        bodies = await generate_failures(stack, 2)
        mark_replayed(db_path, bodies[0]["dlq_id"])

        pending = (await client.get("/admin/dlq", params={"status": "pending"})).json()
        replayed = (await client.get("/admin/dlq", params={"status": "replayed"})).json()

        assert [row["id"] for row in pending] == [bodies[1]["dlq_id"]]
        assert [row["id"] for row in replayed] == [bodies[0]["dlq_id"]]
        assert replayed[0]["replay_outcome"] == "processed"
        assert replayed[0]["replayed_at"] is not None


async def test_list_respects_limit(make_stack) -> None:
    async with make_stack({"failure_rate": 1.0}, backoff_base_ms=0) as stack:
        client, _ = stack
        bodies = await generate_failures(stack, 4)

        rows = (await client.get("/admin/dlq", params={"limit": 2})).json()
        assert [row["id"] for row in rows] == [
            bodies[3]["dlq_id"],
            bodies[2]["dlq_id"],
        ]


async def test_list_is_empty_without_failures(make_stack) -> None:
    async with make_stack() as stack:
        client, _ = stack
        resp = await client.get("/admin/dlq")
        assert resp.status_code == 200
        assert resp.json() == []


async def test_list_rejects_invalid_status(make_stack) -> None:
    async with make_stack() as stack:
        client, _ = stack
        resp = await client.get("/admin/dlq", params={"status": "bogus"})
        assert resp.status_code == 422


async def test_list_rejects_non_positive_limit(make_stack) -> None:
    async with make_stack() as stack:
        client, _ = stack
        assert (await client.get("/admin/dlq", params={"limit": 0})).status_code == 422
        assert (await client.get("/admin/dlq", params={"limit": -5})).status_code == 422


# --- detail ----------------------------------------------------------------


async def test_detail_returns_the_complete_row(make_stack) -> None:
    async with make_stack({"failure_rate": 1.0}, backoff_base_ms=0) as stack:
        client, _ = stack
        submitted = (await generate_failures(stack, 1))[0]

        resp = await client.get(f"/admin/dlq/{submitted['dlq_id']}")
        assert resp.status_code == 200
        row = resp.json()

        assert DLQ_COLUMNS <= set(row)
        assert row["id"] == submitted["dlq_id"]
        assert row["event_id"] == submitted["event_id"]
        assert row["event"]["event_id"] == submitted["event_id"]


async def test_detail_of_replayed_row_shows_replay_fields(make_stack) -> None:
    async with make_stack({"failure_rate": 1.0}, backoff_base_ms=0) as stack:
        client, db_path = stack
        dlq_id = (await generate_failures(stack, 1))[0]["dlq_id"]
        mark_replayed(db_path, dlq_id)

        row = (await client.get(f"/admin/dlq/{dlq_id}")).json()
        assert row["status"] == "replayed"
        assert row["replayed_at"] == "2026-09-01T10:00:00+00:00"
        assert row["replay_outcome"] == "processed"
        assert row["replay_event_id"]


async def test_detail_of_unknown_id_is_404(make_stack) -> None:
    async with make_stack() as stack:
        client, _ = stack
        resp = await client.get("/admin/dlq/999999")
        assert resp.status_code == 404
        assert "999999" in resp.json()["detail"]
