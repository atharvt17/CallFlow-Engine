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

import asyncio
import logging
import random
import sqlite3
import time
from dataclasses import dataclass, field
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


def _seconds_since(iso_timestamp: str) -> float:
    """Wall seconds since an ISO-8601 UTC stamp (for ``e2e_latency_ms``)."""
    return (datetime.now(UTC) - datetime.fromisoformat(iso_timestamp)).total_seconds()


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


# Per-row replay status (SPECS §7.2). ``replayed`` is terminal; ``pending``
# means the event failed again and stays actionable; ``missing``/``not_claimed``
# are refusals, never state changes.
ReplayStatus = str  # "replayed" | "pending" | "deferred" | "missing" | "not_claimed"

# Outcomes recorded on the dead-letter row when a replay succeeds vs fails
# again (SPECS §7.2 "replay_outcome: processed | failed (with reason)").
_REPLAY_OUTCOME_FAILED = "failed"


@dataclass(frozen=True)
class ReplayResult:
    """One event's replay attempt, as reported by the API and the CLI."""

    event_id: str
    dlq_id: int | None
    status: ReplayStatus
    reason: str | None = None
    attempts: int = 0
    retry_count: int = 0

    @property
    def succeeded(self) -> bool:
        """Terminal success is ``replayed``; a breaker refusal is not a result."""
        return self.status == "replayed"

    def as_body(self) -> dict[str, object]:
        body: dict[str, object] = {
            "event_id": self.event_id,
            "dlq_id": self.dlq_id,
            "status": self.status,
            "attempts": self.attempts,
        }
        if self.reason is not None:
            body["reason"] = self.reason
        return body


@dataclass(frozen=True)
class ReplayReport:
    """Result of one replay request (SPECS §5 ``POST /admin/dlq/replay``).

    SPECS §5 fixes the top-level body as
    ``{"attempted","succeeded","failed","results":[...]}``; :meth:`as_body`
    emits exactly those four keys, because the spec does not authorize
    additive top-level fields and a spec-exact consumer should not have to
    tolerate them. The extra information still has a home: §5 does not
    constrain the entries inside ``results``, so every row's fate is carried
    there by its ``status``.

    Those statuses partition ``results`` exactly — a row is never counted
    twice and never dropped:

    - ``replayed`` — dispatched and succeeded;
    - ``pending`` — dispatched and failed again (the row stays replayable);
    - ``deferred`` — the breaker refused: no downstream call, no state change;
    - ``missing`` / ``not_claimed`` — the row was not replayable at all
      (unknown ``event_id``, or no longer ``failed_downstream``).

    ``attempted`` therefore counts only rows that actually crossed the
    resilient boundary (``replayed`` + ``pending``), and the invariant
    ``attempted == succeeded + failed`` holds. The ``deferred``/``skipped``
    properties are for our own surfaces (the CLI's summary and exit code);
    they are not serialized, so they cannot become an accidental contract.
    """

    results: tuple[ReplayResult, ...] = field(default_factory=tuple)

    @property
    def attempted(self) -> int:
        return sum(1 for r in self.results if r.status in ("replayed", "pending"))

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.status == "replayed")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "pending")

    @property
    def deferred(self) -> int:
        """Rows the breaker refused — attempted, but never dispatched."""
        return sum(1 for r in self.results if r.status == "deferred")

    @property
    def skipped(self) -> int:
        """Rows that were not replayable at all (unknown or already resolved)."""
        return sum(1 for r in self.results if r.status in ("missing", "not_claimed"))

    def as_body(self) -> dict[str, object]:
        """The §5 body — exactly the four documented keys."""
        return {
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "results": [r.as_body() for r in self.results],
        }


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

        # An event that already carries a DLQ link got here by way of a dead
        # letter — most importantly, a replay claimed the row, the process
        # crashed mid-dispatch, and startup recovery requeued the orphan
        # (``processing → deferred``). Closing that link on success is what
        # keeps "processed event with a pending DLQ row" impossible.
        prior_dlq_id = None if row["dlq_id"] is None else int(row["dlq_id"])
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
            prior_dlq_id=prior_dlq_id,
        )
        if result.ok and prior_dlq_id is not None:
            # The recovered event completed an outstanding replay: count it
            # exactly as the in-line replay success path does.
            self._metrics.dlq_replays_total.labels(result="success").inc()
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
        if dlq_id is not None or (result.ok and prior_dlq_id is not None):
            # Either a new/refreshed dead letter (pending) or one that just
            # moved pending → replayed; the gauge must follow the store, or it
            # over-reports the backlog until the next DLQ write.
            self._refresh_dlq_gauge()
        return "processed" if result.ok else "failed"

    # --- replay (M4-T03; SPECS §7.2) ---------------------------------------

    async def replay(
        self,
        *,
        limit: int = 50,
        event_ids: list[str] | None = None,
    ) -> ReplayReport:
        """Re-submit pending dead letters through the resilient boundary.

        SPECS §7.2, all of which this path enforces:

        - **Same boundary, no shortcut:** dispatch is :meth:`_dispatch`, so
          retries, the per-attempt timeout and breaker admission all apply. A
          breaker that refuses the call leaves the row ``pending`` with the
          event untouched — replay is never a way around the breaker.
        - **Bypasses the public idempotency short-circuit:** :meth:`accept`
          is *not* consulted. The event already has a ``call_events`` row, and
          looking it up would answer "duplicate" instead of re-processing —
          which is the whole point of an authorized replay. Rows are updated
          **in place**: no new ``call_events`` row and no new dead-letter row,
          ever (the UNIQUE ``event_id`` makes a duplicate row impossible by
          construction).
        - **Success** → event ``processed`` + DLQ row ``replayed`` with
          ``replay_outcome`` and ``replay_event_id`` (one transaction).
        - **Failure** → the existing row is updated (reason, attempts,
          ``last_failed_at``; ``first_failed_at`` preserved) and stays
          ``pending``.

        Claims make concurrent replays safe: :func:`store.claim_event_for_replay`
        is a conditional ``failed_downstream → processing`` UPDATE, so two
        replays of one event can never both process it.

        When the caller names ``event_ids``, any id that has no pending
        dead-letter row is reported as ``missing`` instead of being dropped:
        the operator asked for those events by name and is owed an answer.
        """
        rows = store.select_replayable(self._conn, limit=limit, event_ids=event_ids)
        results: list[ReplayResult] = [
            await self._replay_one(row["event_id"], int(row["id"])) for row in rows
        ]
        if event_ids:
            found = {row["event_id"] for row in rows}
            results.extend(
                ReplayResult(event_id, None, "missing")
                for event_id in dict.fromkeys(event_ids)
                if event_id not in found
            )
        return ReplayReport(results=tuple(results))

    async def _replay_one(self, event_id: str, dlq_id: int) -> ReplayResult:
        if not store.claim_event_for_replay(self._conn, event_id):
            # Another replay (HTTP, CLI, a second process) owns it, or the
            # event is no longer failed_downstream. Change nothing.
            return ReplayResult(event_id, dlq_id, "not_claimed")

        row = store.get_event(self._conn, event_id)
        if row is None:  # pragma: no cover - FK guarantees the event exists
            return ReplayResult(event_id, dlq_id, "missing")

        self._metrics.in_flight_calls.inc()
        try:
            outcome = await self._dispatch(event_id)
        except asyncio.CancelledError:
            # Same reasoning as the drain path: a cancelled replay (client
            # disconnect, shutdown) must return the claim, not strand it.
            self._release_replay_claim(event_id)
            raise
        except Exception:
            self._release_replay_claim(event_id)
            raise
        finally:
            self._metrics.in_flight_calls.dec()

        if outcome.deferred:
            # The breaker refused: no downstream evidence, no status change.
            # The claim goes back so the row remains replayable later.
            self._release_replay_claim(event_id)
            self._metrics.dlq_replays_total.labels(result="deferred").inc()
            return ReplayResult(event_id, dlq_id, "deferred", reason=outcome.reason)

        if outcome.result.ok:
            self._complete_replay(event_id, dlq_id, outcome)
            self._metrics.dlq_replays_total.labels(result="success").inc()
            self._refresh_dlq_gauge()
            return ReplayResult(
                event_id,
                dlq_id,
                "replayed",
                attempts=outcome.attempts,
                retry_count=outcome.retry_count,
            )

        self._fail_replay(event_id, dlq_id, outcome)
        self._metrics.dlq_replays_total.labels(result="failed").inc()
        self._refresh_dlq_gauge()
        return ReplayResult(
            event_id,
            dlq_id,
            "pending",
            reason=outcome.result.reason,
            attempts=outcome.attempts,
            retry_count=outcome.retry_count,
        )

    def _complete_replay(self, event_id: str, dlq_id: int, outcome: RetryOutcome) -> None:
        """Success: event terminal + DLQ ``replayed`` in ONE transaction."""
        ptp_taken = self._ptp_draw.random() < self._settings.success_ptp_rate
        event_outcome = EventOutcome.PTP_TAKEN if ptp_taken else EventOutcome.CONNECTED
        row = store.get_event(self._conn, event_id)
        if row is None:  # pragma: no cover - claim implies the row exists
            return
        now = self._now()
        # ``replay_event_id`` links the replay to the event it re-processed
        # (SPECS §7 ``replay_event_id``) — the same id, because replay updates
        # the original event in place rather than creating a new one.
        replay_event_id = event_id
        e2e_latency_ms = _seconds_since(row["created_at"]) * 1000
        pipeline_latency_ms = (
            None if outcome.result.latency_ms is None else round(outcome.result.latency_ms)
        )
        # Cumulative accounting, exactly as the failure path: ``attempts`` and
        # ``retry_count`` describe the event's whole lifecycle, so the success
        # row must add the replay's attempts to what the event already burned
        # (SPECS §3/§5: retry_count == attempts - 1 always holds).
        total_attempts = int(row["attempts"]) + outcome.attempts
        total_retries = max(0, total_attempts - 1)
        with store.begin_immediate(self._conn):
            store.update_event_terminal(
                self._conn,
                event_id,
                status="processed",
                outcome=event_outcome.value,
                attempts=total_attempts,
                retry_count=total_retries,
                pipeline_latency_ms=pipeline_latency_ms,
                e2e_latency_ms=e2e_latency_ms,
                failure_reason=None,
                updated_at=now,
            )
            store.mark_replayed(
                self._conn,
                dlq_id,
                replayed_at=now,
                replay_outcome="processed",
                replay_event_id=replay_event_id,
                attempts=total_attempts,
                retry_count=total_retries,
            )
            # Keep the reverse link: the row keeps pointing at its DLQ history
            # (SPECS §7.2 "the dlq_id link is kept").
            store.set_dlq_link(self._conn, event_id, dlq_id)
        self._metrics.calls_total.labels(
            outcome=event_outcome.value, status="processed"
        ).inc()
        self._metrics.e2e_latency_ms.observe(e2e_latency_ms)
        log_event(
            logger,
            "dlq_replay",
            event_id=event_id,
            dlq_id=dlq_id,
            result="replayed",
            outcome=event_outcome.value,
            attempts=outcome.attempts,
        )

    def _fail_replay(self, event_id: str, dlq_id: int, outcome: RetryOutcome) -> None:
        """Failure: event back to ``failed_downstream``, DLQ row updated in place."""
        reason = outcome.result.reason or "non_retryable_error"
        now = self._now()
        row = store.get_event(self._conn, event_id)
        if row is None:  # pragma: no cover - claim implies the row exists
            return
        # Cumulative accounting, same rule as the deferral path: replay
        # attempts add to what the event already burned.
        total_attempts = int(row["attempts"]) + outcome.attempts
        total_retries = max(0, total_attempts - 1)
        e2e_latency_ms = _seconds_since(row["created_at"]) * 1000
        with store.begin_immediate(self._conn):
            store.update_event_terminal(
                self._conn,
                event_id,
                status="failed_downstream",
                outcome=EventOutcome.FAILED_DOWNSTREAM.value,
                attempts=total_attempts,
                retry_count=total_retries,
                pipeline_latency_ms=None,
                e2e_latency_ms=e2e_latency_ms,
                failure_reason=reason,
                updated_at=now,
            )
            store.refresh_dead_letter(
                self._conn,
                dlq_id,
                reason=reason,
                attempts=total_attempts,
                retry_count=total_retries,
                last_failed_at=now,
            )
        self._metrics.calls_total.labels(
            outcome=EventOutcome.FAILED_DOWNSTREAM.value, status="failed_downstream"
        ).inc()
        log_event(
            logger,
            "dlq_replay",
            event_id=event_id,
            dlq_id=dlq_id,
            result=_REPLAY_OUTCOME_FAILED,
            reason=reason,
            attempts=outcome.attempts,
        )

    def _release_replay_claim(self, event_id: str) -> None:
        """Return a claimed replay row to ``failed_downstream``.

        Conditional on ``processing``, so a replay that already finalized is
        never resurrected. Synchronous (no ``await``), so it completes even
        while the calling task is being cancelled.
        """
        with store.begin_immediate(self._conn):
            self._conn.execute(
                "UPDATE call_events SET status = 'failed_downstream'"
                " WHERE event_id = ? AND status = 'processing'",
                (event_id,),
            )

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
        prior_dlq_id: int | None = None,
    ) -> int | None:
        """Commit the terminal status (and its DLQ row) in ONE transaction.

        SPECS §7: "event-status updates and DLQ writes execute in a single
        transaction so event status and dead-letter state can never diverge".
        Returns the dead-letter id for failures, ``None`` for successes.

        ``prior_dlq_id`` closes a dead letter on a *successful* terminal flip.
        Replay finalizes in place through :meth:`_complete_replay`, but a
        replay interrupted by a crash comes back as a ``deferred`` row at the
        next boot (:func:`app.store.requeue_processing_orphans`) and is then
        drained through :meth:`process_deferred`. That drain is a successful
        replay, so its DLQ row must become ``replayed`` too — otherwise the
        event reads ``processed`` while its dead letter still reads ``pending``.
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
            elif status == "processed" and prior_dlq_id is not None:
                # A dead letter was opened for this event earlier; the event
                # is now terminal-success, so the dead letter is resolved. The
                # counts come from the same cumulative accounting as the event
                # row, so the two never disagree (SPECS §7.2).
                store.mark_replayed(
                    self._conn,
                    prior_dlq_id,
                    replayed_at=updated_at,
                    replay_outcome="processed",
                    replay_event_id=event_id,
                    attempts=attempts,
                    retry_count=retry_count,
                )
                log_event(
                    logger,
                    "dlq_replay",
                    event_id=event_id,
                    dlq_id=prior_dlq_id,
                    result="replayed",
                    outcome=outcome,
                    attempts=attempts,
                    recovered=True,
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
