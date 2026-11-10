"""Event lifecycle engine (M2-T07, Sprint 4; SPECS §5, §6.3, §7.1).

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
  or ``failed_downstream``. Every terminal failure writes its dead-letter row
  **in the same transaction** as the status flip (M3-T20), so
  ``failed_downstream ⇒ ∃ dead_letter row`` holds at write time;
- **deferral**: a breaker-refused call is persisted as ``deferred`` before
  the ``202`` is returned, then enqueued on the bounded FIFO (M3-T16/T17).
  Enqueue failure (full FIFO) is backpressure only — the row stays durable.
"""

from __future__ import annotations

import logging
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

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

# Outcome of one deferred dispatch (internal to the drainer).
DeferredOutcome = str  # "processed" | "failed" | "expired" | "deferred"


class DeferralSink(Protocol):
    """What the service needs from the deferral queue (M3-T16/T17)."""

    def enqueue(self, event_id: str) -> bool: ...

    def pending_count(self) -> int: ...


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
    dlq_id: int | None = None
    deferred_pending: int | None = None


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
        # Wired by the app factory (main.py) or tests; optional so the service
        # stays usable standalone (durable deferral needs no in-memory queue).
        self._deferral: DeferralSink | None = None

    def bind_deferral(self, deferral: DeferralSink) -> None:
        """Attach the deferral queue (M3-T16)."""
        self._deferral = deferral

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
            # no terminal accounting. The row was already committed as
            # ``processing`` before dispatch, so it becomes ``deferred`` here —
            # still before the 202 is returned (SPECS §6.3).
            updated_at = self._now()
            with store.begin_immediate(self._conn):
                # Attempts burned before the breaker opened are real work and
                # must survive the deferral (SPECS §3: accounting spans the
                # event's whole lifecycle, not just the final pass).
                self._conn.execute(
                    "UPDATE call_events SET status = 'deferred', attempts = ?,"
                    " retry_count = ?, updated_at = ? WHERE event_id = ?",
                    (
                        attempts,
                        max(0, attempts - 1),
                        updated_at,
                        event_id,
                    ),
                )
            pending = self._enqueue_deferred(event_id)
            log_event(
                logger,
                "call_deferred",
                event_id=event_id,
                reason=outcome.reason,
                attempts=attempts,
                deferred_pending=pending,
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
                deferred_pending=pending,
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
        dlq_id = self._finalize(
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
            fields["dlq_id"] = dlq_id
        log_event(logger, "call_processed" if result.ok else "call_failed", **fields)

        self._metrics.calls_total.labels(outcome=outcome_value, status=status).inc()
        self._metrics.e2e_latency_ms.observe(e2e_latency_ms)
        if dlq_id is not None:
            self._refresh_dlq_gauge()

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
            dlq_id=dlq_id,
        )

    async def process_deferred(self, event_id: str, *, now: datetime) -> DeferredOutcome:
        """Dispatch one claimed deferred event (M3-T21/T22, drainer entry).

        The caller has already flipped the row to ``processing`` via
        :func:`app.store.claim_deferred_event`, so exactly one drainer owns
        it. Ordering matters: **expiry is decided before dispatch** — an
        event past ``DEFER_MAX_AGE_S`` is dead-lettered with
        ``deferred_expired`` and never reaches the dependency.

        Returns ``"expired"``, ``"processed"``, ``"failed"`` or
        ``"deferred"`` (the breaker refused again — the row returns to
        ``deferred`` and is retried on a later pass).

        Dispatch goes through the same resilient boundary as first-pass
        traffic: retries, timeout and breaker admission all apply, and a
        failure here writes its DLQ row through :meth:`_finalize` exactly as
        a first-pass failure would.
        """
        row = store.get_event(self._conn, event_id)
        if row is None:  # pragma: no cover - claim implies the row exists
            return "failed"

        created_at = datetime.fromisoformat(row["created_at"])
        if (now - created_at).total_seconds() > self._settings.defer_max_age_s:
            self._expire_deferred(row, now=now)
            return "expired"

        self._metrics.in_flight_calls.inc()
        try:
            outcome = await self._dispatch(event_id)
        finally:
            self._metrics.in_flight_calls.dec()

        if outcome.deferred:
            with store.begin_immediate(self._conn):
                self._conn.execute(
                    "UPDATE call_events SET status = 'deferred', attempts = attempts + ?,"
                    " retry_count = retry_count + ?, updated_at = ? WHERE event_id = ?",
                    (
                        outcome.attempts,
                        max(0, outcome.attempts - 1) if outcome.attempts else 0,
                        self._now(),
                        event_id,
                    ),
                )
            log_event(
                logger,
                "call_deferred",
                event_id=event_id,
                reason=outcome.reason,
                attempts=outcome.attempts,
            )
            return "deferred"

        result = outcome.result
        if result.ok:
            ptp_taken = self._ptp_draw.random() < self._settings.success_ptp_rate
            outcome_value = (EventOutcome.PTP_TAKEN if ptp_taken else EventOutcome.CONNECTED).value
            status = "processed"
            failure_reason = None
        else:
            outcome_value = EventOutcome.FAILED_DOWNSTREAM.value
            status = "failed_downstream"
            failure_reason = result.reason

        # e2e covers submission → terminal completion, deferral wait included
        # (SPECS §9), measured on the original ``created_at``.
        completed = datetime.now(UTC)
        e2e_latency_ms = (completed - created_at).total_seconds() * 1000
        pipeline_latency_ms = None if result.latency_ms is None else round(result.latency_ms)
        # Accounting is cumulative: a deferred event that already burned
        # attempts when the breaker opened keeps that history, so
        # ``retry_count == attempts - 1`` holds across the whole lifecycle.
        total_attempts = int(row["attempts"]) + outcome.attempts
        total_retries = max(0, total_attempts - 1)

        dlq_id = self._finalize(
            event_id,
            status=status,
            outcome=outcome_value,
            attempts=total_attempts,
            retry_count=total_retries,
            pipeline_latency_ms=pipeline_latency_ms,
            e2e_latency_ms=e2e_latency_ms,
            failure_reason=failure_reason,
            updated_at=self._now(),
        )
        self._metrics.calls_total.labels(outcome=outcome_value, status=status).inc()
        self._metrics.e2e_latency_ms.observe(e2e_latency_ms)
        log_event(
            logger,
            "call_processed" if result.ok else "call_failed",
            event_id=event_id,
            outcome=outcome_value,
            attempts=outcome.attempts,
            pipeline_latency_ms=pipeline_latency_ms,
            e2e_latency_ms=round(e2e_latency_ms, 1),
            recovered=True,
            **({"failure_reason": failure_reason, "dlq_id": dlq_id} if not result.ok else {}),
        )
        if dlq_id is not None:
            self._refresh_dlq_gauge()
        return "processed" if result.ok else "failed"

    # --- internals ---------------------------------------------------------

    def _enqueue_deferred(self, event_id: str) -> int:
        """Enqueue after the deferred commit; return the durable pending count.

        Returns the DB count **after** the row is durable, which is what the
        202 body reports. A full FIFO is backpressure, not an error: the row
        remains recoverable and the drainer tops up as capacity frees.
        """
        if self._deferral is not None:
            self._deferral.enqueue(event_id)
            return self._deferral.pending_count()
        return store.count_deferred(self._conn)

    def _finalize(
        self,
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
    ) -> int | None:
        """Commit the terminal status (and its DLQ row) in ONE transaction.

        SPECS §7: "event-status updates and DLQ writes execute in a single
        transaction so event status and dead-letter state can never diverge".
        Returns the dead-letter id for failures, ``None`` for successes.
        """
        dlq_id: int | None = None
        with store.begin_immediate(self._conn):
            store.update_event_terminal(
                self._conn,
                event_id,
                status=status,
                outcome=outcome,
                attempts=attempts,
                retry_count=retry_count,
                pipeline_latency_ms=pipeline_latency_ms,
                e2e_latency_ms=e2e_latency_ms,
                failure_reason=failure_reason,
                updated_at=updated_at,
            )
            if status == "failed_downstream":
                row = store.get_event(self._conn, event_id)
                dlq_id = store.record_dead_letter(
                    self._conn,
                    event_id,
                    row["event_json"],
                    reason=failure_reason or "non_retryable_error",
                    attempts=attempts,
                    retry_count=retry_count,
                    first_failed_at=updated_at,
                    last_failed_at=updated_at,
                )
                log_event(
                    logger,
                    "dlq_insert",
                    event_id=event_id,
                    dlq_id=dlq_id,
                    reason=failure_reason,
                )
        return dlq_id

    def _expire_deferred(self, row: sqlite3.Row, *, now: datetime) -> None:
        """Dead-letter an aged-out event (M3-T22); same transaction semantics."""
        event_id = row["event_id"]
        stamp = now.isoformat()
        dlq_id = self._finalize(
            event_id,
            status="failed_downstream",
            outcome=EventOutcome.FAILED_DOWNSTREAM.value,
            attempts=row["attempts"],
            retry_count=row["retry_count"],
            pipeline_latency_ms=None,
            e2e_latency_ms=(now - datetime.fromisoformat(row["created_at"])).total_seconds()
            * 1000,
            failure_reason="deferred_expired",
            updated_at=stamp,
        )
        self._metrics.deferred_expired_total.inc()
        self._metrics.calls_total.labels(
            outcome=EventOutcome.FAILED_DOWNSTREAM.value, status="failed_downstream"
        ).inc()
        self._refresh_dlq_gauge()
        log_event(
            logger,
            "deferred_expired",
            event_id=event_id,
            dlq_id=dlq_id,
            age_s=round((now - datetime.fromisoformat(row["created_at"])).total_seconds(), 1),
        )

    def _refresh_dlq_gauge(self) -> None:
        counts = store.count_dead_letters(self._conn)
        for status, count in counts.items():
            self._metrics.dlq_entries.labels(status=status).set(count)

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
