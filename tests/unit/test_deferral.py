"""Persistent-deferral mechanics (M3-T24; SPECS §6.3, §7).

Store-level primitives first (claims, transactional DLQ writes, deferred
enumeration), then the bounded FIFO, startup recovery and expiry. Clocks are
injected; no test sleeps.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app import store
from app.config import Settings
from app.metrics import Metrics
from app.models import CallEvent
from app.pipeline.base import PipelineResult
from app.resilience.retry import RetryOutcome
from app.service import CallService

NOW = datetime(2026, 9, 1, 9, 3, 12, tzinfo=UTC)


def make_event(event_id: str = "a1f2c3d4-e89b-12d3-a456-426614174000") -> CallEvent:
    return CallEvent.model_validate(
        {
            "event_id": event_id,
            "campaign_id": "CAMP-2026-09-A",
            "borrower_bucket": "0-30 DPD",
            "timestamp": "2026-09-01T09:03:12Z",
            "outcome": "connected",
            "product_type": "bnpl",
        }
    )


def seed_deferred(conn, event_id: str, *, created_at: datetime = NOW) -> None:
    store.insert_event(
        conn,
        make_event(event_id),
        status="deferred",
        created_at=created_at.isoformat(),
        updated_at=created_at.isoformat(),
    )


# --- M3-T18: idempotent per-event claims ------------------------------------


def test_claim_transitions_deferred_to_processing(tmp_db) -> None:
    event_id = str(make_event().event_id)
    seed_deferred(tmp_db, event_id)

    assert store.claim_deferred_event(tmp_db, event_id) is True
    row = store.get_event(tmp_db, event_id)
    assert row is not None and row["status"] == "processing"


def test_claim_is_exclusive(tmp_db) -> None:
    """Two claims for one event: exactly one wins (status flips only once)."""
    event_id = str(make_event().event_id)
    seed_deferred(tmp_db, event_id)

    assert store.claim_deferred_event(tmp_db, event_id) is True
    assert store.claim_deferred_event(tmp_db, event_id) is False


def test_claim_refuses_non_deferred_rows(tmp_db) -> None:
    event_id = str(make_event().event_id)
    store.insert_event(
        tmp_db, make_event(event_id), status="processed", created_at=NOW.isoformat(),
        updated_at=NOW.isoformat(),
    )
    assert store.claim_deferred_event(tmp_db, event_id) is False
    assert store.get_event(tmp_db, event_id)["status"] == "processed"


def test_claim_of_unknown_event_is_false(tmp_db) -> None:
    assert store.claim_deferred_event(tmp_db, "00000000-0000-4000-8000-0000000000aa") is False


# --- M3-T20: transactional, unique, complete DLQ writes ---------------------


def test_record_dead_letter_inserts_complete_row(tmp_db) -> None:
    event = make_event()
    event_id = str(event.event_id)
    seed_deferred(tmp_db, event_id)
    store.claim_deferred_event(tmp_db, event_id)

    with store.begin_immediate(tmp_db):
        store.update_event_terminal(
            tmp_db,
            event_id,
            status="failed_downstream",
            outcome="failed_downstream",
            attempts=3,
            retry_count=2,
            pipeline_latency_ms=None,
            e2e_latency_ms=12.5,
            failure_reason="downstream_timeout",
            updated_at=NOW.isoformat(),
        )
        dlq_id = store.record_dead_letter(
            tmp_db,
            event_id,
            event.model_dump_json(),
            reason="downstream_timeout",
            attempts=3,
            retry_count=2,
            first_failed_at=NOW.isoformat(),
            last_failed_at=NOW.isoformat(),
        )

    row = tmp_db.execute("SELECT * FROM dead_letter WHERE id = ?", (dlq_id,)).fetchone()
    assert row["event_id"] == event_id
    assert row["reason"] == "downstream_timeout"
    assert row["attempts"] == 3 and row["retry_count"] == 2
    assert row["first_failed_at"] == NOW.isoformat()
    assert row["last_failed_at"] == NOW.isoformat()
    assert row["status"] == "pending"
    assert row["event_json"] == event.model_dump_json()

    event_row = store.get_event(tmp_db, event_id)
    assert event_row["status"] == "failed_downstream"
    assert event_row["dlq_id"] == dlq_id


def test_record_dead_letter_updates_existing_row(tmp_db) -> None:
    """A repeated failure refreshes the row but preserves ``first_failed_at``."""
    event = make_event()
    event_id = str(event.event_id)
    seed_deferred(tmp_db, event_id)

    first_at = NOW.isoformat()
    second_at = (NOW + timedelta(minutes=5)).isoformat()
    first_id = store.record_dead_letter(
        tmp_db, event_id, event.model_dump_json(), reason="connection_error",
        attempts=3, retry_count=2, first_failed_at=first_at, last_failed_at=first_at,
    )
    second_id = store.record_dead_letter(
        tmp_db, event_id, event.model_dump_json(), reason="timeout",
        attempts=1, retry_count=0, first_failed_at=second_at, last_failed_at=second_at,
    )

    assert second_id == first_id, "one row per event, updated in place"
    rows = tmp_db.execute("SELECT * FROM dead_letter").fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["reason"] == "timeout"
    assert row["attempts"] == 1 and row["retry_count"] == 0
    assert row["first_failed_at"] == first_at, "first failure time is preserved"
    assert row["last_failed_at"] == second_at


def test_record_dead_letter_is_transactional_with_status(tmp_db) -> None:
    """A rollback between the status flip and the DLQ write leaves neither."""
    event = make_event()
    event_id = str(event.event_id)
    seed_deferred(tmp_db, event_id)

    with pytest.raises(RuntimeError, match="boom"), store.begin_immediate(tmp_db):
        store.update_event_terminal(
            tmp_db, event_id, status="failed_downstream", outcome="failed_downstream",
            attempts=1, retry_count=0, pipeline_latency_ms=None, e2e_latency_ms=1.0,
            failure_reason="timeout", updated_at=NOW.isoformat(),
        )
        store.record_dead_letter(
            tmp_db, event_id, event.model_dump_json(), reason="timeout",
            attempts=1, retry_count=0,
            first_failed_at=NOW.isoformat(), last_failed_at=NOW.isoformat(),
        )
        raise RuntimeError("boom")

    assert store.get_event(tmp_db, event_id)["status"] == "deferred"
    assert tmp_db.execute("SELECT COUNT(*) AS n FROM dead_letter").fetchone()["n"] == 0


# --- M3-T19: deferred enumeration + orphan requeue --------------------------


def test_list_deferred_orders_by_created_at(tmp_db) -> None:
    early, late = (
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    )
    seed_deferred(tmp_db, late, created_at=NOW + timedelta(seconds=1))
    seed_deferred(tmp_db, early, created_at=NOW)

    ids = [row["event_id"] for row in store.list_deferred_events(tmp_db)]
    assert ids == [early, late]


def test_requeue_orphans_flips_processing_rows(tmp_db) -> None:
    orphan = "33333333-3333-4333-8333-333333333333"
    deferred = "44444444-4444-4444-8444-444444444444"
    store.insert_event(
        tmp_db, make_event(orphan), status="processing", created_at=NOW.isoformat(),
        updated_at=NOW.isoformat(),
    )
    seed_deferred(tmp_db, deferred)

    requeued = store.requeue_processing_orphans(tmp_db)

    assert requeued == [orphan]
    assert store.get_event(tmp_db, orphan)["status"] == "deferred"
    assert store.get_event(tmp_db, deferred)["status"] == "deferred"
    # Idempotent: a second pass finds nothing to requeue.
    assert store.requeue_processing_orphans(tmp_db) == []


def test_count_deferred_and_dlq_counts(tmp_db) -> None:
    seed_deferred(tmp_db, "55555555-5555-4555-8555-555555555555")
    seed_deferred(tmp_db, "66666666-6666-4666-8666-666666666666")
    assert store.count_deferred(tmp_db) == 2
    assert store.count_dead_letters(tmp_db) == {"pending": 0, "replayed": 0}


# --- M3-T16/T17: 202 acceptance, enqueue, bounded FIFO ----------------------


class ToggleBackend:
    """Resilient-boundary stand-in whose outcome the test scripts.

    Implements ``execute`` (the interface the service prefers) so the service
    sees a full logical-call outcome without running the breaker again.
    """

    def __init__(self, *, ok: bool = True, reason: str | None = None, attempts: int = 1) -> None:
        self.ok = ok
        self.reason = reason
        self.attempts = attempts
        self.dispatches: list[str] = []

    async def aclose(self) -> None:
        pass

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        self.dispatches.append(event_id)
        result = PipelineResult(
            ok=self.ok, reason=self.reason, http_status=200 if self.ok else 503,
            latency_ms=250.0 if self.ok else 100.0,
        )
        return RetryOutcome(result=result, attempts=self.attempts)

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        return (await self.execute(event_id, audio_ref)).result


class DeferredBackend:
    """Always-deferred boundary (breaker open as far as the service knows)."""

    def __init__(self) -> None:
        self.dispatches = 0

    async def aclose(self) -> None:
        pass

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        self.dispatches += 1
        return RetryOutcome(
            result=PipelineResult(ok=False, reason="circuit_open"), attempts=0, deferred=True
        )

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        return PipelineResult(ok=False, reason="circuit_open")


def make_queue(conn, backend, settings: Settings, metrics: Metrics | None = None):
    from app.deferral import DeferralQueue

    service = CallService(conn, backend, settings, metrics or Metrics())
    return DeferralQueue(conn, service, settings, metrics or Metrics()), service


async def test_breaker_open_acceptance_is_deferred_and_enqueued(tmp_db) -> None:
    queue, service = make_queue(tmp_db, DeferredBackend(), Settings())
    service.bind_deferral(queue)
    event = make_event()

    acceptance = await service.accept(event)

    assert acceptance.status == "deferred"
    assert acceptance.deferred_pending == 1
    assert acceptance.attempts == 0
    assert len(queue) == 1
    assert store.get_event(tmp_db, str(event.event_id))["status"] == "deferred"


async def test_queue_overflow_never_drops_data(tmp_db) -> None:
    """In-memory fullness is backpressure only: every row stays DB-backed."""
    queue, service = make_queue(tmp_db, DeferredBackend(), Settings(defer_queue_max=2))
    service.bind_deferral(queue)

    ids = [f"7{i}000000-0000-4000-8000-000000000000" for i in range(5)]
    for event_id in ids:
        acceptance = await service.accept(make_event(event_id))
        assert acceptance.status == "deferred"

    assert len(queue) == 2, "the FIFO is bounded"
    assert store.count_deferred(tmp_db) == 5, "SQLite keeps every accepted event"
    assert queue.pending_count() == 5
    # The first two enqueued are the FIFO head, in submission order.
    assert queue.peek_all() == ids[:2]
    # Capacity is full: top-up is a no-op, never a drop.
    assert queue.top_up_from_db() == 0
    assert queue.peek_all() == ids[:2]


async def test_fifo_membership_mirror_stays_exact(tmp_db) -> None:
    """The O(1) membership set never drifts from the deque it mirrors.

    ``top_up_from_db`` dedupes once per pending row, so membership is a set
    test rather than a deque scan (a 5,000-slot FIFO against a ~9,000-row
    backlog made the scan ~25M comparisons of blocking work per top-up).
    That is only safe while every deque mutation keeps the mirror exact —
    through backpressure refills, pops, and the re-queue on a breaker refusal.
    """
    queue, service = make_queue(tmp_db, DeferredBackend(), Settings(defer_queue_max=3))
    service.bind_deferral(queue)
    ids = [f"8{i}000000-0000-4000-8000-000000000000" for i in range(6)]
    for event_id in ids:
        seed_deferred(tmp_db, event_id)

    assert queue.recover().recovered == 3
    assert set(queue.peek_all()) == set(queue._queued)
    assert len(queue._queued) == len(queue.peek_all())

    # One pass pops the head, the boundary refuses it, and it is pushed back
    # to the head: the mirror must neither lose nor invent an id.
    report = await queue.drain_once(now=NOW)
    assert (report.drained, report.succeeded, report.failed) == (1, 0, 0)
    assert queue.peek_all() == ids[:3], "the refused event returns to the head"
    assert set(queue.peek_all()) == set(queue._queued)
    assert len(queue._queued) == len(queue.peek_all())

    # Top-up adds unseen ids only, and the mirror follows.
    queue.top_up_from_db()
    assert len(queue.peek_all()) == 3
    assert set(queue.peek_all()) == set(queue._queued)
    assert len(queue._queued) == len(queue.peek_all())


async def test_fifo_backpressure_refills_as_capacity_frees(tmp_db) -> None:
    """A backlog larger than the FIFO flows across passes, oldest-first."""
    queue, service = make_queue(tmp_db, ToggleBackend(ok=True), Settings(defer_queue_max=2))
    service.bind_deferral(queue)
    ids = [f"9{i}000000-0000-4000-8000-000000000000" for i in range(5)]
    for event_id in ids:
        seed_deferred(tmp_db, event_id)

    report = queue.recover()
    assert report.recovered == 2, "recovery respects the FIFO bound"
    assert queue.peek_all() == ids[:2]

    first = await queue.drain_once(now=NOW)
    assert (first.drained, first.succeeded) == (2, 2)
    # Capacity freed: the next two are topped up from SQLite in FIFO order.
    assert queue.peek_all() == ids[2:4]

    second = await queue.drain_once(now=NOW)
    assert (second.drained, second.succeeded) == (2, 2)
    third = await queue.drain_once(now=NOW)
    assert (third.drained, third.succeeded) == (1, 1)

    for event_id in ids:
        assert store.get_event(tmp_db, event_id)["status"] == "processed"


async def test_expired_deferred_event_goes_to_dlq(tmp_db) -> None:
    queue, service = make_queue(tmp_db, ToggleBackend(ok=True), Settings(defer_max_age_s=60))
    service.bind_deferral(queue)
    event = make_event()
    old_created = (NOW - timedelta(hours=2)).isoformat()
    store.insert_event(
        tmp_db, event, status="deferred", created_at=old_created, updated_at=old_created
    )
    queue.enqueue(str(event.event_id))

    report = await queue.drain_once(now=NOW)

    assert report.drained == 1 and report.succeeded == 0 and report.failed == 1
    row = store.get_event(tmp_db, str(event.event_id))
    assert row["status"] == "failed_downstream"
    assert row["failure_reason"] == "deferred_expired"
    assert row["dlq_id"] is not None
    dlq = tmp_db.execute("SELECT * FROM dead_letter WHERE event_id = ?", (str(event.event_id),)).fetchone()
    assert dlq["reason"] == "deferred_expired"


async def test_drain_processes_recovered_events_exactly_once(tmp_db) -> None:
    backend = ToggleBackend(ok=True)
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    ids = [f"8{i}000000-0000-4000-8000-000000000000" for i in range(3)]
    for event_id in ids:
        seed_deferred(tmp_db, event_id)

    report = queue.recover()
    assert (report.recovered, report.orphans) == (3, 0)
    report = await queue.drain_once(now=NOW)

    assert report.drained == 3
    assert report.succeeded == 3 and report.failed == 0
    assert sorted(backend.dispatches) == sorted(ids)
    for event_id in ids:
        assert store.get_event(tmp_db, event_id)["status"] == "processed"
    # Nothing left to drain, and the recovered work is not re-enqueued.
    assert len(queue) == 0
    second = queue.recover()
    assert (second.recovered, second.orphans) == (0, 0)


async def test_recovery_requeues_orphans_and_is_idempotent(tmp_db) -> None:
    backend = ToggleBackend(ok=True)
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    orphan = "99999999-9999-4999-8999-999999999999"
    store.insert_event(
        tmp_db, make_event(orphan), status="processing",
        created_at=NOW.isoformat(), updated_at=NOW.isoformat(),
    )

    report = queue.recover()
    assert (report.recovered, report.orphans) == (1, 1)
    assert store.get_event(tmp_db, orphan)["status"] == "deferred"
    second = queue.recover()
    assert (second.recovered, second.orphans) == (0, 0)


async def test_drain_stops_when_dispatch_stack_defers_again(tmp_db) -> None:
    """Breaker re-opened mid-drain: the batch returns to deferred, not lost."""
    backend = DeferredBackend()
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    event_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    seed_deferred(tmp_db, event_id)
    queue.recover()

    report = await queue.drain_once(now=NOW)

    assert report.drained == 1 and report.succeeded == 0
    assert store.get_event(tmp_db, event_id)["status"] == "deferred"
    assert queue.pending_count() == 1


async def test_terminal_failure_writes_complete_dlq_row(tmp_db) -> None:
    backend = ToggleBackend(ok=False, reason="downstream_timeout", attempts=3)
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    event = make_event()

    acceptance = await service.accept(event)

    assert acceptance.status == "failed_downstream"
    assert acceptance.dlq_id is not None
    row = store.get_event(tmp_db, str(event.event_id))
    assert row["dlq_id"] == acceptance.dlq_id
    dlq = tmp_db.execute("SELECT * FROM dead_letter WHERE id = ?", (acceptance.dlq_id,)).fetchone()
    assert dlq["event_id"] == str(event.event_id)
    assert dlq["reason"] == "downstream_timeout"
    assert dlq["attempts"] == 3 and dlq["retry_count"] == 2
    assert dlq["event_json"] == event.model_dump_json()


async def test_deferred_gauges_reflect_db_and_fifo(tmp_db) -> None:
    metrics = Metrics()
    backend = DeferredBackend()
    queue, service = make_queue(tmp_db, backend, Settings(), metrics)
    service.bind_deferral(queue)

    await service.accept(make_event())
    await service.accept(make_event("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"))
    body = metrics.render()[0].decode()

    assert "deferred_pending 2.0" in body
    assert "deferred_queue_depth 2.0" in body


class SelfReopeningBackend:
    """First call burns 2 attempts then defers; drain calls succeed."""

    def __init__(self) -> None:
        self.calls = 0

    async def aclose(self) -> None:
        pass

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        self.calls += 1
        if self.calls == 1:
            return RetryOutcome(
                result=PipelineResult(ok=False, reason="circuit_open"),
                attempts=2,
                deferred=True,
            )
        return RetryOutcome(
            result=PipelineResult(ok=True, http_status=200, latency_ms=10.0), attempts=1
        )

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        return (await self.execute(event_id, audio_ref)).result


async def test_attempt_accounting_is_cumulative_across_deferral(tmp_db) -> None:
    """attempts/retry_count span the whole lifecycle (SPECS §3/§7)."""
    backend = SelfReopeningBackend()
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    event = make_event()

    acceptance = await service.accept(event)
    assert acceptance.status == "deferred"
    row = store.get_event(tmp_db, str(event.event_id))
    assert row["status"] == "deferred"

    report = await queue.drain_once(now=NOW)
    assert (report.drained, report.succeeded) == (1, 1)

    row = store.get_event(tmp_db, str(event.event_id))
    assert row["status"] == "processed"
    assert row["attempts"] == 3, "2 attempts before deferral + 1 after recovery"
    assert row["retry_count"] == row["attempts"] - 1


# --- M3-T27: cancellation must never strand a claim -------------------------


class BlockingBackend:
    """Dispatch that blocks until released, then succeeds.

    Used to cancel a drain *mid-flight*, which is what a client disconnect on
    ``POST /admin/deferred/drain`` produces while the app keeps running.
    """

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.dispatches = 0

    async def aclose(self) -> None:
        pass

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        self.dispatches += 1
        self.started.set()
        await self.release.wait()
        return RetryOutcome(
            result=PipelineResult(ok=True, http_status=200, latency_ms=5.0), attempts=1
        )

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        return (await self.execute(event_id, audio_ref)).result


class CancelOnSecondDispatchBackend:
    """Succeeds for the first event, blocks forever for the second."""

    def __init__(self) -> None:
        self.dispatches = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def aclose(self) -> None:
        pass

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        self.dispatches += 1
        if self.dispatches >= 2:
            self.started.set()
            await self.release.wait()
        return RetryOutcome(
            result=PipelineResult(ok=True, http_status=200, latency_ms=5.0), attempts=1
        )

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        return (await self.execute(event_id, audio_ref)).result


async def test_cancelled_drain_returns_the_inflight_claim(tmp_db) -> None:
    """A cancelled manual drain releases its claim *without* a restart.

    Regression for the client-disconnect case: the app keeps running, so
    startup recovery will never run. The claimed row must go back to
    ``deferred`` (and back on the queue) before ``CancelledError`` propagates.
    """
    backend = BlockingBackend()
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    event_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    seed_deferred(tmp_db, event_id)
    queue.recover()

    task = asyncio.create_task(queue.drain_once(now=NOW))
    await backend.started.wait()
    # Mid-flight: the row is claimed, so it is `processing` right now.
    assert store.get_event(tmp_db, event_id)["status"] == "processing"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    row = store.get_event(tmp_db, event_id)
    assert row["status"] == "deferred", "claim released without a restart"
    assert queue.peek_all() == [event_id], "returned to the FIFO head"
    assert queue.pending_count() == 1

    # The live app can now make progress: a later pass processes it.
    backend.release.set()
    report = await queue.drain_once(now=NOW)
    assert (report.drained, report.succeeded) == (1, 1)
    assert store.get_event(tmp_db, event_id)["status"] == "processed"


async def test_cancelled_drain_never_strands_other_queued_events(tmp_db) -> None:
    """The event being cancelled is released; the untouched backlog is intact."""
    backend = CancelOnSecondDispatchBackend()
    queue, service = make_queue(tmp_db, backend, Settings())
    service.bind_deferral(queue)
    ids = [f"d{i}000000-0000-4000-8000-000000000000" for i in range(3)]
    for event_id in ids:
        seed_deferred(tmp_db, event_id)
    queue.recover()

    task = asyncio.create_task(queue.drain_once(now=NOW))
    await backend.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    statuses = {event_id: store.get_event(tmp_db, event_id)["status"] for event_id in ids}
    assert statuses[ids[0]] == "processed", "committed work survives the cancellation"
    assert statuses[ids[1]] == "deferred", "the interrupted claim is released"
    assert statuses[ids[2]] == "deferred", "the untouched backlog is still pending"
    assert queue.peek_all() == ids[1:], "queue matches the durable state, in order"

