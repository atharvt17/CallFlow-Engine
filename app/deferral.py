"""Persistent deferral: SQLite-authoritative queue, recovery, drainer (Sprint 4).

SPECS §6.3, normative: **SQLite is the source of truth for deferral.** The
bounded in-memory FIFO is only a processing mechanism.

- A deferred event is persisted by :class:`~app.service.CallService` *before*
  the ``202`` is returned; :meth:`DeferralQueue.enqueue` then puts its id on
  the FIFO.
- The FIFO is bounded by ``DEFER_QUEUE_MAX``. When it is full, records stay
  persisted-but-not-enqueued (backpressure) — :meth:`top_up_from_db` refills
  as capacity frees. **In-memory fullness never drops data.**
- A per-event claim (``deferred → processing``, one conditional UPDATE) makes
  concurrent double processing structurally impossible; two drainers racing
  for the same id yield exactly one winner.
- Startup recovery requeues crash orphans (``processing`` rows left by a
  process that died mid-flight) and re-enqueues every ``deferred`` row in
  ``created_at`` order. It is idempotent: recovery against an already
  recovered store enqueues nothing new.
- At dispatch time an event older than ``DEFER_MAX_AGE_S`` is dead-lettered
  with reason ``deferred_expired`` — deferral is bounded and observable.
- Draining dispatches through the *normal resilient boundary* (no shortcut):
  a breaker that refuses the dispatch leaves the row deferred for the next
  pass.

Concurrency model: the FIFO mutators (:meth:`enqueue`, :meth:`top_up_from_db`)
are synchronous and never ``await`` mid-mutation, so on a single asyncio event
loop each is atomic and needs no lock. Drain passes, which do await dispatch,
serialize on :attr:`_drain_lock`; the database side is serialized by SQLite's
``BEGIN IMMEDIATE`` writer lock. A cancelled drainer leaves at most its
in-flight event ``processing`` — recovered as an orphan on the next boot.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from app import store
from app.config import Settings
from app.logging import log_event
from app.metrics import Metrics
from app.service import CallService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DrainReport:
    """Result of one drain pass (SPECS §5 ``POST /admin/deferred/drain``)."""

    drained: int = 0
    succeeded: int = 0
    failed: int = 0

    def as_body(self) -> dict[str, int]:
        return {"drained": self.drained, "succeeded": self.succeeded, "failed": self.failed}


@dataclass(frozen=True)
class RecoveryReport:
    """Startup reconstruction evidence (``recovery_completed`` log line)."""

    recovered: int
    orphans: int


class DeferralQueue:
    """Bounded FIFO over the durable ``deferred`` set."""

    def __init__(
        self,
        conn,
        service: CallService,
        settings: Settings,
        metrics: Metrics,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._conn = conn
        self._service = service
        self._settings = settings
        self._metrics = metrics
        self._clock = clock or (lambda: datetime.now(UTC))
        self._items: deque[str] = deque()
        self._drain_lock = asyncio.Lock()
        self._publish_gauges()

    # --- introspection ------------------------------------------------------

    def __len__(self) -> int:
        return len(self._items)

    def pending_count(self) -> int:
        """Durable pending count (SQLite — the source of truth)."""
        return store.count_deferred(self._conn)

    def peek_all(self) -> list[str]:
        """FIFO contents in order (test/diagnostic surface)."""
        return list(self._items)

    # --- producer path ------------------------------------------------------

    def enqueue(self, event_id: str) -> bool:
        """Append to the FIFO; ``False`` means backpressure, never loss."""
        if event_id in self._items:
            return True
        if len(self._items) >= self._settings.defer_queue_max:
            log_event(logger, "deferred_backpressure", event_id=event_id)
            self._publish_gauges()
            return False
        self._items.append(event_id)
        self._publish_gauges()
        return True

    def top_up_from_db(self) -> int:
        """Refill the FIFO from SQLite, in FIFO order, up to capacity.

        Returns how many ids were newly enqueued. Safe to call after every
        drain pass: ids already queued are skipped.
        """
        capacity = self._settings.defer_queue_max - len(self._items)
        if capacity > 0:
            added = 0
            for row in store.list_deferred_events(self._conn):
                if added >= capacity:
                    break
                event_id = row["event_id"]
                if event_id in self._items:
                    continue
                self._items.append(event_id)
                added += 1
        else:
            added = 0
        self._publish_gauges()
        return added

    # --- recovery -----------------------------------------------------------

    def recover(self) -> RecoveryReport:
        """Reconstruct the FIFO from SQLite on boot (idempotent, SPECS §6.3).

        Crash orphans (``processing``) are first returned to ``deferred`` —
        the process that held their claim is gone — then every deferred id is
        enqueued oldest-first, deduped, within the FIFO bound. Rows beyond
        capacity stay DB-backed and are topped up as capacity frees.
        """
        orphans = store.requeue_processing_orphans(self._conn)
        recovered = self.top_up_from_db()
        report = RecoveryReport(recovered=recovered, orphans=len(orphans))
        log_event(
            logger,
            "recovery_completed",
            recovered=report.recovered,
            orphans=report.orphans,
            deferred_pending=store.count_deferred(self._conn),
        )
        return report

    # --- drainer ------------------------------------------------------------

    async def drain_once(self, *, now: datetime | None = None) -> DrainReport:
        """Process queued events until the FIFO empties or the breaker refuses.

        Expiry is evaluated **at dispatch time** with the injected clock
        (SPECS §6.3, M3-T22). Every dispatch goes through
        :meth:`CallService.process_deferred` — the same resilient boundary as
        first-pass traffic.

        A refusal (the breaker re-opened mid-drain) puts the event back at the
        FIFO head and ends the pass: every remaining item would produce the
        same refusal, so churning through them is pointless. The row stays
        ``deferred`` in SQLite regardless.
        """
        async with self._drain_lock:
            drained = succeeded = failed = 0
            while self._items:
                event_id = self._items.popleft()
                self._publish_gauges()

                if not store.claim_deferred_event(self._conn, event_id):
                    # Another drainer owns this id, or it was completed or
                    # replayed meanwhile: nothing to do.
                    continue

                try:
                    outcome = await self._service.process_deferred(
                        event_id, now=now if now is not None else self._clock()
                    )
                except Exception:
                    # Never leave a claimed row stranded in ``processing``: an
                    # unexpected error returns it to ``deferred`` (the claim is
                    # released) and the next pass retries it. Restart recovery
                    # would also catch it, but only after a restart.
                    logger.exception("deferred dispatch failed; returning to queue")
                    with store.begin_immediate(self._conn):
                        self._conn.execute(
                            "UPDATE call_events SET status = 'deferred'"
                            " WHERE event_id = ? AND status = 'processing'",
                            (event_id,),
                        )
                    self._items.appendleft(event_id)
                    self._publish_gauges()
                    raise
                # ``drained`` counts work attempted this pass; ``succeeded`` /
                # ``failed`` are the terminal subset. An event the breaker
                # refuses again is attempted but neither.
                drained += 1
                if outcome == "processed":
                    succeeded += 1
                elif outcome in ("failed", "expired"):
                    failed += 1
                else:  # deferred again — the batch waits for the next pass
                    self._items.appendleft(event_id)
                    self._publish_gauges()
                    break

            # Capacity freed during the pass: refill so a backlog larger than
            # the FIFO keeps flowing across passes.
            self.top_up_from_db()
            return DrainReport(drained=drained, succeeded=succeeded, failed=failed)

    async def drainer_loop(self, stop: asyncio.Event, *, interval_s: float = 1.0) -> None:
        """Background top-up + drain (M3-T21); exits on ``stop``."""
        while not stop.is_set():
            try:
                self.top_up_from_db()
                if self._items:
                    await self.drain_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("deferred drain pass failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            except TimeoutError:
                continue

    # --- observability ------------------------------------------------------

    def _publish_gauges(self) -> None:
        """Publish ``deferred_pending`` (DB) and ``deferred_queue_depth`` (FIFO)."""
        self._metrics.deferred_pending.set(store.count_deferred(self._conn))
        self._metrics.deferred_queue_depth.set(len(self._items))
