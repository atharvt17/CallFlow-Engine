"""Store constraint and transaction tests (M2-T04, SPECS §7)."""

from __future__ import annotations

import sqlite3

import pytest

from app import store
from app.models import CallEvent

NOW = "2026-09-01T09:03:12+00:00"


def make_event(event_id: str = "a1f2c3d4-e89b-12d3-a456-426614174000") -> CallEvent:
    return CallEvent.model_validate(
        {
            "event_id": event_id,
            "campaign_id": "CAMP-2026-09-A",
            "borrower_bucket": "0-30 DPD",
            "timestamp": "2026-09-01T09:03:12Z",
            "outcome": "connected",
            "product_type": "bnpl",
            "call_duration_s": 47,
            "retry_count": 0,
        }
    )


def test_wal_and_fk_pragmas(tmp_db) -> None:
    assert tmp_db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert tmp_db.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_schema_tables_and_indexes_exist(tmp_db) -> None:
    names = {
        row["name"]
        for row in tmp_db.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
        )
    }
    assert {"call_events", "dead_letter"} <= names
    assert {"idx_call_events_status", "idx_dead_letter_status"} <= names


def test_duplicate_event_id_rejected(tmp_db) -> None:
    store.insert_event(tmp_db, make_event(), status="processing", created_at=NOW, updated_at=NOW)
    with pytest.raises(sqlite3.IntegrityError):
        store.insert_event(
            tmp_db, make_event(), status="processing", created_at=NOW, updated_at=NOW
        )


def test_dead_letter_event_id_unique_violation(tmp_db) -> None:
    event_id = str(make_event().event_id)
    store.insert_event(tmp_db, make_event(), status="failed_downstream", created_at=NOW, updated_at=NOW)
    row_sql = (
        "INSERT INTO dead_letter (event_id, event_json, reason, attempts, retry_count,"
        " first_failed_at, last_failed_at) VALUES (?, '{}', 'downstream_timeout', 1, 0, ?, ?)"
    )
    tmp_db.execute(row_sql, (event_id, NOW, NOW))
    with pytest.raises(sqlite3.IntegrityError):
        tmp_db.execute(row_sql, (event_id, NOW, NOW))


def test_dead_letter_fk_enforced(tmp_db) -> None:
    """A dead-letter row for an unknown event must fail (foreign_keys=ON)."""
    with pytest.raises(sqlite3.IntegrityError):
        tmp_db.execute(
            "INSERT INTO dead_letter (event_id, event_json, reason, attempts, retry_count,"
            " first_failed_at, last_failed_at) VALUES ('no-such-event', '{}',"
            " 'downstream_timeout', 1, 0, ?, ?)",
            (NOW, NOW),
        )


def test_begin_immediate_rolls_back_on_exception(tmp_db) -> None:
    event_id = str(make_event().event_id)
    with pytest.raises(RuntimeError, match="boom"), store.begin_immediate(tmp_db):
        store.insert_event(
            tmp_db, make_event(), status="processing", created_at=NOW, updated_at=NOW
        )
        raise RuntimeError("boom")
    assert store.get_event(tmp_db, event_id) is None, "partial write must never survive"


def test_begin_immediate_commits_on_success(tmp_db) -> None:
    event_id = str(make_event().event_id)
    with store.begin_immediate(tmp_db):
        store.insert_event(
            tmp_db, make_event(), status="processing", created_at=NOW, updated_at=NOW
        )
    assert store.get_event(tmp_db, event_id) is not None


def test_insert_preserves_audit_fields(tmp_db) -> None:
    event = make_event()
    store.insert_event(tmp_db, event, status="processing", created_at=NOW, updated_at=NOW)
    row = store.get_event(tmp_db, str(event.event_id))
    assert row is not None
    assert row["input_outcome"] == "connected"  # audit-only copy of the incoming outcome
    assert row["event_json"] == event.model_dump_json()  # restart reconstruction source
    assert row["attempts"] == 0 and row["retry_count"] == 0
    assert row["dlq_id"] is None and row["failure_reason"] is None


def test_connect_creates_parent_directories(tmp_path) -> None:
    db_path = tmp_path / "nested" / "deeper" / "calls.db"
    conn = store.connect(db_path)
    try:
        assert db_path.exists()
    finally:
        conn.close()


def test_init_db_is_idempotent_on_existing_schema(tmp_db) -> None:
    """Boot against an existing store must be a no-op (restart safety, §6.3)."""
    store.init_db(tmp_db)  # second call: both tables present -> no error, no DDL rerun
    names = {
        row["name"]
        for row in tmp_db.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
        )
    }
    assert {"call_events", "dead_letter", "idx_call_events_status", "idx_dead_letter_status"} <= names


def test_init_db_rejects_incompatible_existing_schema(tmp_path) -> None:
    """Both tables present does NOT imply a valid schema: column mismatch fails loudly."""
    conn = store.connect(tmp_path / "legacy.db")
    try:
        conn.execute("CREATE TABLE call_events (event_id TEXT PRIMARY KEY)")  # legacy/foreign schema
        conn.execute("CREATE TABLE dead_letter (id INTEGER PRIMARY KEY)")
        with pytest.raises(RuntimeError, match="schema is incompatible") as excinfo:
            store.init_db(conn)
        message = str(excinfo.value)
        assert "input_outcome" in message  # names concrete missing columns
        assert "first_failed_at" in message
    finally:
        conn.close()


def test_input_outcome_column_is_not_null(tmp_db) -> None:
    """SPECS §7: input_outcome TEXT NOT NULL (Appendix A mandates `outcome`)."""
    columns = {
        row["name"]: row["notnull"]
        for row in tmp_db.execute("PRAGMA table_info(call_events)")
    }
    assert columns["input_outcome"] == 1  # NOT NULL constraint present
    with pytest.raises(sqlite3.IntegrityError):
        tmp_db.execute(
            "INSERT INTO call_events (event_id, campaign_id, borrower_bucket, timestamp,"
            " input_outcome, event_json, status, created_at, updated_at)"
            " VALUES ('x1', 'c', '0-30 DPD', 't', NULL, '{}', 'processing', 'a', 'a')"
        )
