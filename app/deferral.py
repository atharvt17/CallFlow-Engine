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
``BEGIN IMMEDIATE`` writer lock.

Membership is O(1): the deque is mirrored by a set of the same ids. Deque
``in`` is a linear scan, and :meth:`top_up_from_db` tests membership once per
pending row — with a full 5,000-slot FIFO against a ~9,000-row backlog that
was ~25M comparisons of blocking work per top-up, measured at 188 ms, on the
event loop that also has to serve dispatch timeouts. The set makes each test
constant-time; the deque still owns ordering. Every mutation of the deque goes
through a helper that keeps the mirror exact, so the two cannot drift.

Cancellation is claim-safe. A cancelled drain — most importantly a client
disconnect on ``POST /admin/deferred/drain`` while the application keeps
running — returns its in-flight row to ``deferred`` and re-raises. Relying on
startup recovery alone would be wrong for request-scoped cancellation: no
restart is coming, so the row would sit in ``processing`` indefinitely. Only a
hard-killed process leaves a ``processing`` orphan, and that is what startup
recovery is for.

**Scope: one process per database file.** The conditional claim is what makes
concurrent drainers safe, and SQLite's writer lock makes it correct across
threads and across processes that share a local file. It does *not* make a
single SQLite file safe to share over a network filesystem: WAL depends on
POSIX/Windows file-locking semantics that NFS/SMB do not reliably provide. A
multi-*host* deployment needs a server-class database (or one database per
replica); Round 1 is deliberately single-process (PLAN D5).
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
        # Membership mirror of ``_items``: deque ``in`` is O(n), and top-up
        # tests membership once per pending row. Every deque mutation goes
        # through the helpers below so this stays exact.
        self._queued: set[str] = set()
        self._drain_lock = asyncio.Lock()
        self._publish_gauges()

    # --- FIFO primitives (keep ``_queued`` in step with ``_items``) --------

    def _contains(self, event_id: str) -> bool:
        return event_id in self._queued

    def _push_back(self, event_id: str) -> None:
        self._items.append(event_id)
        self._queued.add(event_id)

    def _push_front(self, event_id: str) -> None:
        self._items.appendleft(event_id)
        self._queued.add(event_id)

    def _pop_front(self) -> str:
        event_id = self._items.popleft()
        self._queued.discard(event_id)
        return event_id

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
        if self._contains(event_id):
            return True
        if len(self._items) >= self._settings.defer_queue_max:
            log_event(logger, "deferred_backpressure", event_id=event_id)
            self._publish_gauges()
            return False
        self._push_back(event_id)
        self._publish_gauges()
        return True

    def top_up_from_db(self) -> int:
        """Refill the FIFO from SQLite, in FIFO order, up to capacity.

        Returns how many ids were newly enqueued. Safe to call after every
        drain pass: ids already queued are skipped.

        The scan stops as soon as the capacity is filled, and membership is a
        set test, so the cost is bounded by the rows examined rather than by
        ``pending × queue_depth``.
        """
        capacity = self._settings.defer_queue_max - len(self._items)
        if capacity > 0:
            added = 0
            for row in store.list_deferred_events(self._conn):
                if added >= capacity:
                    break
                event_id = row["event_id"]
                if self._contains(event_id):
                    continue
                self._push_back(event_id)
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

        Cancellation releases the in-flight claim **before** propagating.
        ``CancelledError`` derives from ``BaseException``, so it is not caught
        by the ``except Exception`` below; without an explicit handler a client
        disconnect on ``POST /admin/deferred/drain`` would leave the row
        ``processing`` until the next restart — and the application is still
        running, so no recovery pass is coming. The release is synchronous and
        cannot itself yield, so it completes even under cancellation.
        """
        async with self._drain_lock:
            drained = succeeded = failed = 0
            while self._items:
                event_id = self._pop_front()
                self._publish_gauges()

                if not store.claim_deferred_event(self._conn, event_id):
                    # Another drainer owns this id, or it was completed or
                    # replayed meanwhile: nothing to do.
                    continue

                try:
                    outcome = await self._service.process_deferred(
                        event_id, now=now if now is not None else self._clock()
                    )
                except asyncio.CancelledError:
                    # Request-scoped cancellation (client disconnect) or
                    # shutdown. Hand the claim back so this very process can
                    # retry it; a shutdown cancels the drainer, not this task.
                    self._release_claim(event_id)
                    logger.warning("drain cancelled; claim returned to deferred")
                    raise
                except Exception:
                    # Never leave a claimed row stranded in ``processing``: an
                    # unexpected error returns it to ``deferred`` (the claim is
                    # released) and the next pass retries it. Restart recovery
                    # would also catch it, but only after a restart.
                    logger.exception("deferred dispatch failed; returning to queue")
                    self._release_claim(event_id)
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
                    self._push_front(event_id)
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

    def _release_claim(self, event_id: str) -> None:
        """Return a claimed row to ``deferred`` and put it back on the FIFO head.

        One conditional UPDATE, so a claim that already finalized (``processed``
        / ``failed_downstream``) is never resurrected. Synchronous by design:
        it contains no ``await``, so it runs to completion even while the
        surrounding task is being cancelled.
        """
        with store.begin_immediate(self._conn):
            self._conn.execute(
                "UPDATE call_events SET status = 'deferred'"
                " WHERE event_id = ? AND status = 'processing'",
                (event_id,),
            )
        if not self._contains(event_id):
            self._push_front(event_id)
        self._publish_gauges()

    def _publish_gauges(self) -> None:
        """Publish ``deferred_pending`` (DB) and ``deferred_queue_depth`` (FIFO)."""
        self._metrics.deferred_pending.set(store.count_deferred(self._conn))
        self._metrics.deferred_queue_depth.set(len(self._items))
