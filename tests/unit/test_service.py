"""Service lifecycle invariants at single-attempt scope (M2-T07).

Deterministic: tmp DB from conftest, recording/fake backends, no sleeps.
"""

from __future__ import annotations

import json
import logging

from app import store
from app.config import Settings
from app.logging import JsonFormatter
from app.metrics import Metrics
from app.models import CallEvent
from app.pipeline.base import PipelineResult
from app.resilience.retry import RetryOutcome
from app.service import CallService

NOW = "2026-09-01T09:03:12+00:00"


def make_service(tmp_db, backend, settings: Settings) -> CallService:
    return CallService(tmp_db, backend, settings, Metrics())


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


class RecordingBackend:
    """Canned-result backend that counts dispatches."""

    def __init__(self, result: PipelineResult) -> None:
        self._result = result
        self.dispatches = 0
        self.last_event_id: str | None = None
        self.last_audio_ref: str | None = None

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        self.dispatches += 1
        self.last_event_id = event_id
        self.last_audio_ref = audio_ref
        return self._result


class InspectingBackend:
    """Proves persist-before-dispatch by querying the store mid-call."""

    def __init__(self, conn, result: PipelineResult) -> None:
        self._conn = conn
        self._result = result

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        row = store.get_event(self._conn, event_id)
        if row is None:
            raise AssertionError("dispatch happened before the INSERT committed")
        return self._result


async def test_happy_path_processed(tmp_db) -> None:
    backend = RecordingBackend(PipelineResult(ok=True, http_status=200, latency_ms=312.0))
    service = make_service(tmp_db, backend, Settings(success_ptp_rate=0.0))
    event = make_event()

    acceptance = await service.accept(event)

    assert acceptance.duplicate is False
    assert acceptance.status == "processed"
    assert acceptance.outcome == "connected"
    assert acceptance.attempts == 1
    assert acceptance.retry_count == 0  # attempts - 1
    assert acceptance.pipeline_latency_ms == 312
    assert acceptance.e2e_latency_ms is not None and acceptance.e2e_latency_ms > 0
    assert acceptance.failure_reason is None
    assert backend.dispatches == 1
    assert backend.last_event_id == str(event.event_id)
    assert backend.last_audio_ref == "synthetic"  # Appendix B stand-in

    row = store.get_event(tmp_db, str(event.event_id))
    assert row is not None
    assert row["status"] == "processed" and row["outcome"] == "connected"
    assert row["attempts"] == 1 and row["retry_count"] == 0
    assert row["event_json"] == event.model_dump_json()
    assert row["input_outcome"] == "connected"  # audit only


async def test_success_ptp_rate_one_yields_ptp_taken(tmp_db) -> None:
    backend = RecordingBackend(PipelineResult(ok=True, http_status=200, latency_ms=1.0))
    service = make_service(tmp_db, backend, Settings(success_ptp_rate=1.0))
    acceptance = await service.accept(make_event())
    assert acceptance.outcome == "ptp_taken"
    assert acceptance.status == "processed"


async def test_failure_maps_to_failed_downstream(tmp_db) -> None:
    backend = RecordingBackend(
        PipelineResult(ok=False, reason="downstream_timeout", http_status=503, latency_ms=40.0)
    )
    service = make_service(tmp_db, backend, Settings())
    acceptance = await service.accept(make_event())

    assert acceptance.status == "failed_downstream"
    assert acceptance.outcome == "failed_downstream"
    assert acceptance.failure_reason == "downstream_timeout"
    assert acceptance.attempts == 1
    assert acceptance.retry_count == 0

    row = store.get_event(tmp_db, str(make_event().event_id))
    assert row is not None
    assert row["status"] == "failed_downstream" and row["failure_reason"] == "downstream_timeout"
    # M2 staged gap (per TASKS M2-T07 DoD): DLQ row creation lands in M3-T20.
    assert row["dlq_id"] is None
    dlq_count = tmp_db.execute("SELECT COUNT(*) AS n FROM dead_letter").fetchone()["n"]
    assert dlq_count == 0


async def test_duplicate_short_circuits_without_redispatch(tmp_db) -> None:
    backend = RecordingBackend(PipelineResult(ok=True, http_status=200, latency_ms=10.0))
    service = make_service(tmp_db, backend, Settings())
    event = make_event()

    first = await service.accept(event)
    assert first.duplicate is False
    second = await service.accept(event)

    assert second.duplicate is True
    assert backend.dispatches == 1, "duplicate must never re-dispatch"
    assert second.event_id == first.event_id
    assert second.status == first.status
    assert second.outcome == first.outcome
    assert second.attempts == first.attempts
    assert second.retry_count == first.retry_count
    assert second.pipeline_latency_ms == first.pipeline_latency_ms


async def test_lost_insert_race_answered_as_duplicate(tmp_db) -> None:
    """Concurrent duplicate: the PK guarantees exactly one INSERT wins."""
    backend = RecordingBackend(PipelineResult(ok=True, http_status=200, latency_ms=1.0))
    service = make_service(tmp_db, backend, Settings())
    event = make_event()
    store.insert_event(tmp_db, event, status="processing", created_at=NOW, updated_at=NOW)

    acceptance = await service.accept(event)

    assert acceptance.duplicate is True
    assert backend.dispatches == 0, "race-loser must not dispatch"
    assert acceptance.status == "processing"  # mirrors the stored record


async def test_persist_before_dispatch(tmp_db) -> None:
    """The INSERT is committed before the downstream attempt runs."""
    backend = InspectingBackend(
        tmp_db, PipelineResult(ok=False, reason="downstream_timeout", http_status=503)
    )
    service = make_service(tmp_db, backend, Settings())
    acceptance = await service.accept(make_event())
    assert acceptance.status == "failed_downstream"


async def test_incoming_retry_count_is_audit_only_in_event_json(tmp_db) -> None:
    """The incoming `retry_count` survives only inside `event_json` (audit).

    The database's authoritative `retry_count` column is always service-owned:
    `attempts - 1` (SPECS §3/§7).
    """
    backend = RecordingBackend(PipelineResult(ok=True, http_status=200, latency_ms=5.0))
    service = make_service(tmp_db, backend, Settings())
    event = make_event()
    event_with_retry = CallEvent.model_validate(event.model_dump() | {"retry_count": 7})

    acceptance = await service.accept(event_with_retry)

    assert acceptance.retry_count == 0  # attempts(1) - 1, service-owned
    row = store.get_event(tmp_db, str(event.event_id))
    assert row is not None
    assert row["retry_count"] == 0  # authoritative column: attempts - 1
    stored = json.loads(row["event_json"])
    assert stored["retry_count"] == 7  # original incoming value preserved for audit only


class ResilientBackendStub:
    """Stands in for ``ResilientPipelineClient`` (M3-T14 service wiring).

    Records ``execute`` calls and returns a scripted :class:`RetryOutcome`, so
    service tests can assert on the *logical call* contract without running
    the breaker/executor again (those are covered in their own suites).
    """

    def __init__(self, outcomes: list[RetryOutcome]) -> None:
        self._outcomes = list(outcomes)
        self.executions = 0
        self.processed: list[tuple[str, str]] = []

    async def aclose(self) -> None:
        pass

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        self.executions += 1
        return self._outcomes.pop(0)

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        # The service must prefer ``execute`` (it needs attempts/deferred).
        self.processed.append((event_id, audio_ref))
        return (await self.execute(event_id, audio_ref)).result


def _outcome(*, ok: bool, attempts: int = 1, reason: str | None = None) -> RetryOutcome:
    result = PipelineResult(
        ok=ok,
        reason=reason,
        http_status=200 if ok else 503,
        latency_ms=250.0 if ok else 100.0,
    )
    return RetryOutcome(result=result, attempts=attempts)


async def test_resilient_backend_attempts_and_retry_count_persisted(tmp_db) -> None:
    """M3-T14: attempts/retry_count come from the logical call, not a fixed 1."""
    backend = ResilientBackendStub([_outcome(ok=True, attempts=3)])
    service = make_service(tmp_db, backend, Settings(success_ptp_rate=0.0))
    event = make_event()

    acceptance = await service.accept(event)

    assert acceptance.status == "processed"
    assert acceptance.attempts == 3
    assert acceptance.retry_count == 2
    row = store.get_event(tmp_db, str(event.event_id))
    assert row is not None
    assert row["attempts"] == 3 and row["retry_count"] == 2
    assert backend.processed == [], "service must dispatch through execute()"


async def test_resilient_backend_exhausted_attempts_recorded(tmp_db) -> None:
    backend = ResilientBackendStub(
        [_outcome(ok=False, attempts=3, reason="downstream_timeout")]
    )
    service = make_service(tmp_db, backend, Settings())
    acceptance = await service.accept(make_event())

    assert acceptance.status == "failed_downstream"
    assert acceptance.failure_reason == "downstream_timeout"
    assert acceptance.attempts == 3
    assert acceptance.retry_count == 2


async def test_retries_do_not_double_count_attempt_metrics(tmp_db) -> None:
    """Attempt metrics belong to the executor (inside the decorator); the
    service must not add a second observation for the logical call."""
    metrics = Metrics()
    backend = ResilientBackendStub([_outcome(ok=True, attempts=3)])
    service = CallService(tmp_db, backend, Settings(success_ptp_rate=0.0), metrics)

    await service.accept(make_event())

    body = metrics.render()[0].decode()
    # Attempt-level observations stay at zero: the executor inside the
    # decorator owns them, so the service must not add a second observation
    # per logical call (it would double-count attempts).
    assert "pipeline_attempt_latency_ms_count 0.0" in body
    assert "retries_total 0.0" in body
    assert "e2e_latency_ms_count 1.0" in body
    assert 'calls_total{outcome="connected",status="processed"} 1.0' in body


def test_json_formatter_shape() -> None:
    formatter = JsonFormatter()
    logger = logging.getLogger("format-probe")
    record = logger.makeRecord(
        "format-probe", logging.INFO, __file__, 1, "call_received", None, None
    )
    record.json_fields = {"event_id": "e-1", "duplicate": False}
    line = json.loads(formatter.format(record))
    assert set(line) >= {"ts", "level", "event", "event_id", "duplicate"}
    assert line["event"] == "call_received"
    assert line["level"] == "info"
    assert line["event_id"] == "e-1"
