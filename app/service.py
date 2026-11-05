"""Event lifecycle engine (M2-T07; SPECS §5, §7.1) — single-attempt scope.

Sprint 2 processes each accepted event with one downstream attempt; the
resilience layer (Sprint 3) decorates the backend without changing this flow.

Invariants enforced here:

- **persist-before-dispatch**: the ``call_events`` INSERT commits inside a
  ``BEGIN IMMEDIATE`` transaction *before* any downstream attempt, so a crash
  can never lose an accepted event (prerequisite for restart recovery);
- **idempotency**: a known ``event_id`` short-circuits — no re-dispatch, no
  new row. A lost INSERT race (concurrent duplicate) is resolved via the
  PRIMARY KEY and answered as a duplicate of the stored record;
- **``retry_count = attempts - 1``** (service-owned; the incoming
  ``retry_count`` is audit-only);
- terminal states only: ``processed`` (outcome ``connected``/``ptp_taken``)
  or ``failed_downstream``. DLQ row creation lands in M3-T20 (staged gap:
  ``dlq_id`` stays NULL for now).
"""

from __future__ import annotations

import logging
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app import store
from app.config import Settings
from app.logging import log_event
from app.metrics import Metrics
from app.models import CallEvent, EventOutcome
from app.pipeline.base import PipelineBackend

logger = logging.getLogger(__name__)

# Appendix B stand-in: no real audio exists in Round 1 (PDF Appendix B).
_AUDIO_REF = "synthetic"


@dataclass(frozen=True)
class CallAcceptance:
    """Result of submitting one event (single attempt or duplicate)."""

    event_id: str
    status: str  # processed | failed_downstream (or the stored status for duplicates)
    outcome: str | None
    attempts: int
    retry_count: int
    pipeline_latency_ms: int | None
    e2e_latency_ms: float | None
    failure_reason: str | None
    duplicate: bool


class CallService:
    """Synchronous per-event pipeline execution with durable accounting."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        backend: PipelineBackend,
        settings: Settings,
        metrics: Metrics,
    ) -> None:
        self._conn = conn
        self._backend = backend
        self._settings = settings
        self._metrics = metrics
        # Isolated draw stream so SUCCESS_PTP_RATE never perturbs other RNG use.
        self._ptp_draw = random.Random()

    def _now(self) -> str:
        return datetime.now(UTC).isoformat()

    async def accept(self, event: CallEvent) -> CallAcceptance:
        started = time.perf_counter()
        event_id = str(event.event_id)

        existing = store.get_event(self._conn, event_id)
        if existing is not None:
            log_event(
                logger,
                "call_received",
                event_id=event_id,
                campaign_id=event.campaign_id,
                borrower_bucket=event.borrower_bucket.value,
                duplicate=True,
            )
            return self._acceptance_from_row(existing, duplicate=True)

        # Persist before any dispatch — crash-safe accounting prerequisite.
        now = self._now()
        try:
            with store.begin_immediate(self._conn):
                store.insert_event(
                    self._conn, event, status="processing", created_at=now, updated_at=now
                )
        except sqlite3.IntegrityError:
            # Lost a race with a concurrent accept of the same event_id: the
            # winner's commit is already visible (PRIMARY KEY guaranteed it).
            row = store.get_event(self._conn, event_id)
            if row is None:  # pragma: no cover - defensive; PK makes this impossible
                raise RuntimeError(f"lost INSERT race for {event_id} but row is missing") from None
            log_event(
                logger,
                "call_received",
                event_id=event_id,
                campaign_id=event.campaign_id,
                borrower_bucket=event.borrower_bucket.value,
                duplicate=True,
            )
            return self._acceptance_from_row(row, duplicate=True)

        log_event(
            logger,
            "call_received",
            event_id=event_id,
            campaign_id=event.campaign_id,
            borrower_bucket=event.borrower_bucket.value,
            duplicate=False,
        )

        # One logical call is in flight around the (currently single) attempt;
        # Sprint 3's retry executor keeps this gauge around the whole sequence.
        self._metrics.in_flight_calls.inc()
        try:
            result = await self._backend.process(event_id, _AUDIO_REF)
        finally:
            self._metrics.in_flight_calls.dec()
        e2e_latency_ms = (time.perf_counter() - started) * 1000

        # Attempt-level observability (never feeds breaker statistics, §6.2).
        if result.ok:
            attempt_result = "success"
        elif result.reason == "timeout":
            attempt_result = "timeout"
        else:
            attempt_result = "error"
        self._metrics.pipeline_attempts_total.labels(result=attempt_result).inc()
        if result.latency_ms is not None:
            self._metrics.pipeline_attempt_latency_ms.observe(result.latency_ms)

        if result.ok:
            ptp_taken = self._ptp_draw.random() < self._settings.success_ptp_rate
            outcome = EventOutcome.PTP_TAKEN if ptp_taken else EventOutcome.CONNECTED
            status = "processed"
            failure_reason = None
        else:
            outcome = EventOutcome.FAILED_DOWNSTREAM
            status = "failed_downstream"
            failure_reason = result.reason

        updated_at = self._now()
        pipeline_latency_ms = (
            None if result.latency_ms is None else round(result.latency_ms)
        )
        with store.begin_immediate(self._conn):
            store.update_event_terminal(
                self._conn,
                event_id,
                status=status,
                outcome=outcome.value,
                attempts=1,
                retry_count=0,  # attempts - 1 (SPECS §3)
                pipeline_latency_ms=pipeline_latency_ms,
                e2e_latency_ms=e2e_latency_ms,
                failure_reason=failure_reason,
                updated_at=updated_at,
            )

        fields: dict[str, Any] = {
            "event_id": event_id,
            "outcome": outcome.value,
            "attempts": 1,
            "pipeline_latency_ms": pipeline_latency_ms,
            "e2e_latency_ms": round(e2e_latency_ms, 1),
        }
        if failure_reason is not None:
            fields["failure_reason"] = failure_reason
        log_event(logger, "call_processed" if result.ok else "call_failed", **fields)

        self._metrics.calls_total.labels(outcome=outcome.value, status=status).inc()
        self._metrics.e2e_latency_ms.observe(e2e_latency_ms)

        return CallAcceptance(
            event_id=event_id,
            status=status,
            outcome=outcome.value,
            attempts=1,
            retry_count=0,
            pipeline_latency_ms=pipeline_latency_ms,
            e2e_latency_ms=e2e_latency_ms,
            failure_reason=failure_reason,
            duplicate=False,
        )

    def _acceptance_from_row(self, row: sqlite3.Row, *, duplicate: bool) -> CallAcceptance:
        """Mirror the stored record (idempotent duplicate short-circuit)."""
        return CallAcceptance(
            event_id=row["event_id"],
            status=row["status"],
            outcome=row["outcome"],
            attempts=row["attempts"],
            retry_count=row["retry_count"],
            pipeline_latency_ms=row["pipeline_latency_ms"],
            e2e_latency_ms=row["e2e_latency_ms"],
            failure_reason=row["failure_reason"],
            duplicate=duplicate,
        )
