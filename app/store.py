"""SQLite store — the authoritative persistence layer (SPECS §7, PLAN D3).

Connection rules:

- WAL journal mode and ``foreign_keys=ON`` are applied on every connection.
- Connections run in explicit-transaction mode (``isolation_level=None``):
  plain statements autocommit, and multi-statement invariants (event status
  + dead-letter writes, claims) go through :func:`begin_immediate`, which
  never leaves partial writes behind.
- The DB file is auto-created at runtime and never committed to git
  (``data/*.db*`` is ignored); only ``data/sample_events.json`` lives in git.

The DDL below is the SPECS §7 schema verbatim, column for column.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models import CallEvent

# --- SPECS §7 DDL (verbatim, column for column) -----------------------------

DDL_SCRIPT = """
CREATE TABLE call_events (
  event_id TEXT PRIMARY KEY,
  campaign_id TEXT NOT NULL,
  borrower_bucket TEXT NOT NULL,
  product_type TEXT,
  timestamp TEXT NOT NULL,            -- original event timestamp
  call_duration_s INTEGER,
  input_outcome TEXT NOT NULL,        -- incoming outcome, audit only (§3; required by Appendix A)
  event_json TEXT NOT NULL,           -- original validated payload (restart reconstruction source)
  status TEXT NOT NULL,               -- deferred | processing | processed | failed_downstream
  outcome TEXT,                       -- final service-owned Appendix A outcome
  attempts INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,   -- attempts - 1
  pipeline_latency_ms INTEGER,        -- final successful attempt
  e2e_latency_ms REAL,                -- submission → terminal completion (incl. backoff/deferral)
  failure_reason TEXT,                -- §7 reason enum
  dlq_id INTEGER,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX idx_call_events_status ON call_events(status);

CREATE TABLE dead_letter (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,      -- one row per event; repeated replay failures UPDATE it
  event_json TEXT NOT NULL,           -- full original event
  reason TEXT NOT NULL,               -- downstream_timeout | timeout | connection_error |
                                      -- non_retryable_error | deferred_expired
  attempts INTEGER NOT NULL,
  retry_count INTEGER NOT NULL,
  first_failed_at TEXT NOT NULL,
  last_failed_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',   -- pending | replayed
  replayed_at TEXT,
  replay_outcome TEXT,                      -- processed | failed (with reason)
  replay_event_id TEXT,                     -- idempotency link for replayed submissions
  FOREIGN KEY (event_id) REFERENCES call_events(event_id)
);
CREATE INDEX idx_dead_letter_status ON dead_letter(status);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open (and auto-create) the store at ``db_path`` with SPECS §7 pragmas."""
    path = Path(db_path)
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # explicit transaction control
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create the SPECS §7 schema (idempotent once tables exist — caller decides)."""
    conn.executescript(DDL_SCRIPT)


@contextmanager
def begin_immediate(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """``BEGIN IMMEDIATE`` transaction: all-or-nothing, never partial writes."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


# --- event-row operations (implemented here; used by the service, M2-T07) ---


def insert_event(
    conn: sqlite3.Connection,
    event: CallEvent,
    *,
    status: str,
    created_at: str,
    updated_at: str,
) -> None:
    """Insert a ``call_events`` row from a validated event (pre-dispatch state).

    ``input_outcome`` preserves the incoming Appendix A outcome for audit only
    (SPECS §3); ``event_json`` stores the original validated payload.
    """
    conn.execute(
        """
        INSERT INTO call_events (
          event_id, campaign_id, borrower_bucket, product_type, timestamp,
          call_duration_s, input_outcome, event_json, status,
          attempts, retry_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
        """,
        (
            str(event.event_id),
            event.campaign_id,
            event.borrower_bucket.value,
            event.product_type.value if event.product_type is not None else None,
            event.timestamp.isoformat(),
            event.call_duration_s,
            event.outcome.value,
            event.model_dump_json(),
            status,
            created_at,
            updated_at,
        ),
    )


def get_event(conn: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    """Fetch one ``call_events`` row (or ``None``)."""
    cur = conn.execute("SELECT * FROM call_events WHERE event_id = ?", (event_id,))
    return cur.fetchone()


def update_event_terminal(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    status: str,
    outcome: str | None,
    attempts: int,
    retry_count: int,
    pipeline_latency_ms: int | None,
    e2e_latency_ms: float | None,
    failure_reason: str | None,
    updated_at: str,
) -> None:
    """Move an event to a terminal state (``processed`` / ``failed_downstream``)."""
    conn.execute(
        """
        UPDATE call_events
        SET status = ?, outcome = ?, attempts = ?, retry_count = ?,
            pipeline_latency_ms = ?, e2e_latency_ms = ?, failure_reason = ?,
            updated_at = ?
        WHERE event_id = ?
        """,
        (
            status,
            outcome,
            attempts,
            retry_count,
            pipeline_latency_ms,
            e2e_latency_ms,
            failure_reason,
            updated_at,
            event_id,
        ),
    )


# --- dead-letter operations (signature stubs — implemented in M3-T20) -------


def record_dead_letter(
    conn: sqlite3.Connection,
    event_id: str,
    event_json: str,
    *,
    reason: str,
    attempts: int,
    retry_count: int,
    first_failed_at: str,
    last_failed_at: str,
) -> int:
    """Insert-or-update a dead-letter row (M3-T20: transactional with the
    event's ``failed_downstream`` flip; UNIQUE on ``event_id`` forces an
    UPDATE of the existing row on repeated failures, preserving
    ``first_failed_at``)."""
    raise NotImplementedError("DLQ write path lands in M3-T20")


def list_dead_letters(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    limit: int = 50,
) -> list[sqlite3.Row]:
    """List dead-letter rows (M4 inspection surface; stub until M3-T20)."""
    raise NotImplementedError("DLQ listing lands in M3-T20")
