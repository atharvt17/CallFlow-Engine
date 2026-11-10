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
from datetime import UTC, datetime
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

# Expected column names per table (derived from the SPECS §7 DDL above); used
# to detect incompatible pre-existing stores instead of blindly trusting that
# the presence of both tables implies a valid schema.
_EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    "call_events": (
        "event_id",
        "campaign_id",
        "borrower_bucket",
        "product_type",
        "timestamp",
        "call_duration_s",
        "input_outcome",
        "event_json",
        "status",
        "outcome",
        "attempts",
        "retry_count",
        "pipeline_latency_ms",
        "e2e_latency_ms",
        "failure_reason",
        "dlq_id",
        "created_at",
        "updated_at",
    ),
    "dead_letter": (
        "id",
        "event_id",
        "event_json",
        "reason",
        "attempts",
        "retry_count",
        "first_failed_at",
        "last_failed_at",
        "status",
        "replayed_at",
        "replay_outcome",
        "replay_event_id",
    ),
}


def _validate_existing_schema(conn: sqlite3.Connection) -> None:
    """Raise with a precise diff when an existing store's schema doesn't match.

    All tables are validated so the operator sees the complete mismatch in
    one error instead of fixing tables one raise at a time.
    """
    problems: list[str] = []
    for table, expected in _EXPECTED_COLUMNS.items():
        actual = tuple(row["name"] for row in conn.execute(f"PRAGMA table_info({table})"))
        if set(actual) != set(expected):
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            problems.append(
                f"{table!r}: missing columns {missing}, unexpected columns {extra}"
            )
    if problems:
        raise RuntimeError(
            "existing store schema is incompatible with the SPECS §7 DDL — "
            + "; ".join(problems)
            + ". A schema migration is out of Round 1 scope — move or recreate the store."
        )


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
    """Create the SPECS §7 schema; validated no-op when it already exists.

    The DDL text stays verbatim (no ``IF NOT EXISTS``); instead, boot against
    an existing store skips the script **only after** validating that the
    existing tables match the expected SPECS §7 columns — the mere presence
    of both tables is not proof of a compatible schema. A partial schema (one
    table, not the other) or a column mismatch fails loudly with a precise
    diff; schema migration is out of Round 1 scope.
    """
    present = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name IN ('call_events', 'dead_letter')"
        )
    }
    if present == {"call_events", "dead_letter"}:
        _validate_existing_schema(conn)
        return
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
    (SPECS §3; the column is NOT NULL — Appendix A mandates ``outcome`` on
    every event). ``event_json`` stores the original validated payload; any
    incoming ``retry_count`` survives **only inside it** — the authoritative
    ``retry_count`` column starts at 0 and is always recomputed as
    ``attempts - 1`` by terminal updates (SPECS §3/§7).
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


# --- deferral queue operations (M3-T18, M3-T19; SPECS §6.3) -----------------

_DEFERRED = "deferred"
_PROCESSING = "processing"


def claim_deferred_event(
    conn: sqlite3.Connection, event_id: str, *, updated_at: str | None = None
) -> bool:
    """Atomically flip ``deferred → processing``; ``True`` iff this claim won.

    One ``BEGIN IMMEDIATE`` transaction around a conditional UPDATE: SQLite
    serializes writers, and the ``status = 'deferred'`` predicate means only
    the first claimant of an event transitions a row. A second claim — from
    another drainer pass, another task, or another process — matches zero
    rows and returns ``False``, so duplicate processing is structurally
    impossible (SPECS §6.3, M3-T18).
    """
    stamp = updated_at or datetime.now(UTC).isoformat()
    with begin_immediate(conn):
        cur = conn.execute(
            "UPDATE call_events SET status = ?, updated_at = ?"
            " WHERE event_id = ? AND status = ?",
            (_PROCESSING, stamp, event_id, _DEFERRED),
        )
    return cur.rowcount == 1


def list_deferred_events(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """All ``deferred`` rows in FIFO order (``created_at``, then id)."""
    return list(
        conn.execute(
            "SELECT * FROM call_events WHERE status = ?"
            " ORDER BY created_at ASC, event_id ASC",
            (_DEFERRED,),
        )
    )


def requeue_processing_orphans(conn: sqlite3.Connection) -> list[str]:
    """Return crash orphans (``processing``) to ``deferred``; IDs in FIFO order.

    A row left ``processing`` means the process died mid-flight, so no one
    holds a claim on it any more. Requeueing it is safe precisely because the
    claim is a status flip: the dead process can no longer complete it, and a
    live one could never have claimed it (the row was not ``deferred``).
    Idempotent — a second pass over a recovered store returns ``[]``.
    """
    with begin_immediate(conn):
        rows = list(
            conn.execute(
                "SELECT event_id FROM call_events WHERE status = ?"
                " ORDER BY created_at ASC, event_id ASC",
                (_PROCESSING,),
            )
        )
        if rows:
            conn.execute(
                "UPDATE call_events SET status = ?, updated_at = ? WHERE status = ?",
                (_DEFERRED, datetime.now(UTC).isoformat(), _PROCESSING),
            )
    return [row["event_id"] for row in rows]


def count_deferred(conn: sqlite3.Connection) -> int:
    """Deferred events pending in SQLite (the ``deferred_pending`` gauge)."""
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM call_events WHERE status = ?", (_DEFERRED,)
    ).fetchone()
    return int(row["n"])


def count_active_events(conn: sqlite3.Connection) -> int:
    """Events in a non-terminal state (``deferred`` or ``processing``)."""
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM call_events WHERE status IN (?, ?)",
        (_DEFERRED, _PROCESSING),
    ).fetchone()
    return int(row["n"])


# --- dead-letter operations (M3-T20; SPECS §7) ------------------------------

# Full §7.1 reason vocabulary. ``deferred_expired`` is added by M3-T22 on top
# of the four attempt-level reasons the backend boundary already produces.
DEAD_LETTER_REASONS = frozenset(
    {
        "downstream_timeout",
        "timeout",
        "connection_error",
        "non_retryable_error",
        "deferred_expired",
    }
)


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
    """Insert-or-update the event's single dead-letter row; return its ``id``.

    Must run in the **same transaction** as the event's ``failed_downstream``
    flip (SPECS §7): the caller owns the :func:`begin_immediate` boundary, so
    a crash can never leave a failed event without its DLQ entry or a DLQ
    entry without a failed event.

    ``dead_letter.event_id`` is UNIQUE, so a repeated failure UPDATEs the
    existing row — refreshing ``reason``/``attempts``/``retry_count``/
    ``last_failed_at`` while preserving ``first_failed_at`` and the row id
    (§7.1 + §7.2: repeated replay failures never create duplicates).
    """
    if reason not in DEAD_LETTER_REASONS:
        raise ValueError(f"unknown dead-letter reason: {reason!r}")

    conn.execute(
        """
        INSERT INTO dead_letter (
          event_id, event_json, reason, attempts, retry_count,
          first_failed_at, last_failed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
          event_json = excluded.event_json,
          reason = excluded.reason,
          attempts = excluded.attempts,
          retry_count = excluded.retry_count,
          last_failed_at = excluded.last_failed_at,
          -- A fresh terminal failure leaves the row pending again, even
          -- if an earlier replay marked it 'replayed'.
          status = 'pending',
          replayed_at = NULL,
          replay_outcome = NULL
        """,
        (
            event_id,
            event_json,
            reason,
            attempts,
            retry_count,
            first_failed_at,
            last_failed_at,
        ),
    )
    dlq_id = int(
        conn.execute(
            "SELECT id FROM dead_letter WHERE event_id = ?", (event_id,)
        ).fetchone()["id"]
    )
    # The reverse link commits inside the caller's terminal-failure
    # transaction, so `failed_downstream ⇒ dlq_id` can never diverge.
    set_dlq_link(conn, event_id, dlq_id)
    return dlq_id


def set_dlq_link(conn: sqlite3.Connection, event_id: str, dlq_id: int) -> None:
    """Point ``call_events.dlq_id`` at the event's dead-letter row.

    :func:`record_dead_letter` already does this, so callers inside the
    terminal-failure transaction never have to remember the reverse link.
    Exposed for replay (M4), which may re-link after an in-place update.
    """
    conn.execute(
        "UPDATE call_events SET dlq_id = ? WHERE event_id = ?", (dlq_id, event_id)
    )


def list_dead_letters(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    limit: int = 50,
) -> list[sqlite3.Row]:
    """List dead-letter rows newest-first (M4 inspection surface)."""
    if status is None:
        return list(
            conn.execute(
                "SELECT * FROM dead_letter ORDER BY id DESC LIMIT ?", (limit,)
            )
        )
    return list(
        conn.execute(
            "SELECT * FROM dead_letter WHERE status = ? ORDER BY id DESC LIMIT ?",
            (status, limit),
        )
    )


def get_dead_letter(conn: sqlite3.Connection, dlq_id: int) -> sqlite3.Row | None:
    """Fetch one dead-letter row (or ``None``)."""
    return conn.execute("SELECT * FROM dead_letter WHERE id = ?", (dlq_id,)).fetchone()


def count_dead_letters(conn: sqlite3.Connection) -> dict[str, int]:
    """DLQ row counts by status (the ``dlq_entries{status}`` gauge)."""
    counts = {"pending": 0, "replayed": 0}
    for row in conn.execute("SELECT status, COUNT(*) AS n FROM dead_letter GROUP BY status"):
        counts[row["status"]] = int(row["n"])
    return counts
