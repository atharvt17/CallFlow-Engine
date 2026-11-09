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
from app.resilience.retry import RetryOutcome

logger = logging.getLogger(__name__)

# Appendix B stand-in: no real audio exists in Round 1 (PDF Appendix B).
_AUDIO_REF = "synthetic"


@dataclass(frozen=True)
class CallAcceptance:
    """Result of submitting one event (terminal, deferred, or duplicate)."""

    event_id: str
    status: str  # processed | failed_downstream | deferred (or stored status)
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

        # One logical call is in flight around the whole retry sequence; the
        # resilient decorator keeps retry/timeout/breaker concerns inside the
        # boundary (M3-T14), so the service observes one outcome per call.
        self._metrics.in_flight_calls.inc()
        try:
            outcome = await self._dispatch(event_id)
        finally:
            self._metrics.in_flight_calls.dec()
        e2e_latency_ms = (time.perf_counter() - started) * 1000
        result = outcome.result
        attempts = outcome.attempts

        if outcome.deferred:
            # Breaker refused dispatch (SPECS §6.2): no downstream evidence and
            # no terminal accounting. The deferred record stays recoverable;
            # the 202 body/enqueue machinery lands in M3-T16/T17.
            updated_at = self._now()
            with store.begin_immediate(self._conn):
                self._conn.execute(
                    "UPDATE call_events SET status = 'deferred', updated_at = ? "
                    "WHERE event_id = ?",
                    (updated_at, event_id),
                )
            log_event(
                logger,
                "call_deferred",
                event_id=event_id,
                reason=outcome.reason,
                attempts=attempts,
            )
            return CallAcceptance(
                event_id=event_id,
                status="deferred",
                outcome=None,
                attempts=attempts,
                retry_count=0,
                pipeline_latency_ms=None,
                e2e_latency_ms=e2e_latency_ms,
                failure_reason=outcome.reason,
                duplicate=False,
            )

        if result.ok:
            ptp_taken = self._ptp_draw.random() < self._settings.success_ptp_rate
            event_outcome = EventOutcome.PTP_TAKEN if ptp_taken else EventOutcome.CONNECTED
            status = "processed"
            failure_reason = None
        else:
            event_outcome = EventOutcome.FAILED_DOWNSTREAM
            status = "failed_downstream"
            failure_reason = result.reason
        outcome_value = event_outcome.value

        updated_at = self._now()
        pipeline_latency_ms = (
            None if result.latency_ms is None else round(result.latency_ms)
        )
        retry_count = outcome.retry_count
        with store.begin_immediate(self._conn):
            store.update_event_terminal(
                self._conn,
                event_id,
                status=status,
                outcome=outcome_value,
                attempts=attempts,
                retry_count=retry_count,
                pipeline_latency_ms=pipeline_latency_ms,
                e2e_latency_ms=e2e_latency_ms,
                failure_reason=failure_reason,
                updated_at=updated_at,
            )

        fields: dict[str, Any] = {
            "event_id": event_id,
            "outcome": outcome_value,
            "attempts": attempts,
            "pipeline_latency_ms": pipeline_latency_ms,
            "e2e_latency_ms": round(e2e_latency_ms, 1),
        }
        if failure_reason is not None:
            fields["failure_reason"] = failure_reason
        log_event(logger, "call_processed" if result.ok else "call_failed", **fields)

        self._metrics.calls_total.labels(outcome=outcome_value, status=status).inc()
        self._metrics.e2e_latency_ms.observe(e2e_latency_ms)

        return CallAcceptance(
            event_id=event_id,
            status=status,
            outcome=outcome_value,
            attempts=attempts,
            retry_count=retry_count,
            pipeline_latency_ms=pipeline_latency_ms,
            e2e_latency_ms=e2e_latency_ms,
            failure_reason=failure_reason,
            duplicate=False,
        )

    async def _dispatch(self, event_id: str) -> RetryOutcome:
        """One logical call through the boundary.

        The production backend is the resilient decorator (:meth:`execute`
        exposes attempts and the deferred signal). A plain single-attempt
        backend is still supported — it is wrapped into an equivalent
        one-attempt outcome — so the boundary contract stays "any
        PipelineBackend" (PLAN D1).
        """
        execute = getattr(self._backend, "execute", None)
        if execute is not None:
            return await execute(event_id, _AUDIO_REF)
        result = await self._backend.process(event_id, _AUDIO_REF)
        return RetryOutcome(result=result, attempts=1)

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
