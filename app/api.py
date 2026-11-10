"""Public inference API (M2-T08, Sprint 4; SPECS §5).

- ``POST /calls`` — Appendix A event validation (422 on schema errors via
  FastAPI defaults); 200 processed / failed_downstream / duplicate; **202**
  when the breaker accepted the event for later processing (§6.3);
- ``GET /calls/{event_id}`` — stored record (200) / 404;
- ``POST /admin/deferred/drain`` — force one drain pass (§5);
- ``GET /admin/dlq`` / ``GET /admin/dlq/{id}`` — dead-letter inspection
  (M4-T01): the full §7 row with ``event_json`` parsed under ``event``, so a
  reviewer never has to open SQLite to see why an event died;
- ``POST /admin/dlq/replay`` — re-submit pending dead letters through the
  same resilient boundary (M4-T04, §7.2);
- ``GET /stats`` — durable counts + breaker state for operators (M4-T02/§9);
- errors use the ``{"detail": ...}`` envelope.

Payload shapes and the stats body live in :mod:`app.reporting` so the CLI
(``python -m app.cli``) renders exactly the same data (SPECS §2).
"""

from __future__ import annotations

import json
import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from app import store
from app.models import CallEvent
from app.reporting import record_payload, stats_body
from app.resilience.circuit_breaker import CircuitBreaker
from app.service import CallAcceptance, CallService, ReplayReport

router = APIRouter()


@router.post("/calls")
async def submit_call(request: Request, event: CallEvent) -> Response:
    service: CallService = request.app.state.service
    acceptance: CallAcceptance = await service.accept(event)

    if acceptance.duplicate:
        row = store.get_event(request.app.state.conn, acceptance.event_id)
        if row is None:  # pragma: no cover - duplicate implies an existing row
            raise HTTPException(status_code=500, detail="duplicate event record missing")
        return _json(record_payload(row) | {"duplicate": True}, status_code=200)

    if acceptance.status == "deferred":
        # Accepted, not completed (SPECS §5 / refinement B). The row was
        # committed as `deferred` before this response was built.
        return _json(
            {
                "event_id": acceptance.event_id,
                "status": "deferred",
                "reason": acceptance.failure_reason or "circuit_open",
                "deferred_pending": acceptance.deferred_pending or 0,
            },
            status_code=202,
        )

    body: dict[str, object] = {
        "event_id": acceptance.event_id,
        "status": acceptance.status,
        "outcome": acceptance.outcome,
        "attempts": acceptance.attempts,
        "retry_count": acceptance.retry_count,
        "pipeline_latency_ms": acceptance.pipeline_latency_ms,
        "e2e_latency_ms": acceptance.e2e_latency_ms,
    }
    if acceptance.status == "failed_downstream":
        body["failure_reason"] = acceptance.failure_reason
        body["dlq_id"] = acceptance.dlq_id
    return _json(body, status_code=200)


@router.get("/calls/{event_id}")
async def get_call(event_id: str, request: Request) -> dict[str, object]:
    row = store.get_event(request.app.state.conn, event_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown event_id: {event_id}")
    return record_payload(row)


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    """Prometheus text exposition for this app's registry (SPECS §9)."""
    body, content_type = request.app.state.metrics.render()
    return Response(content=body, media_type=content_type)


@router.get("/admin/circuit-breaker")
async def circuit_breaker_snapshot(request: Request) -> dict[str, object]:
    """Live breaker snapshot for the runbook (SPECS §5, Appendix I; M3-T11)."""
    breaker: CircuitBreaker = request.app.state.breaker
    return breaker.snapshot()


@router.post("/admin/deferred/drain")
async def drain_deferred(request: Request) -> dict[str, int]:
    """Force one drain pass over the deferred queue (SPECS §5; M3-T23).

    Idempotent when nothing is pending. Events the breaker still refuses stay
    ``deferred`` — a drain is an operator nudge, never a data-destroying purge.
    """
    return (await request.app.state.deferral.drain_once()).as_body()


@router.get("/admin/dlq")
async def list_dead_letters(
    request: Request,
    status: Literal["pending", "replayed"] | None = "pending",
    limit: int = Query(default=50, ge=1, le=1000),
) -> list[dict[str, object]]:
    """Inspect dead letters, newest first (M4-T01; SPECS §5, §7).

    ``status`` defaults to ``pending`` — the rows an operator can still act on.
    ``?status=replayed`` returns replay history; an unknown value is rejected
    (422) rather than silently ignored, so a typo can never look like an empty
    DLQ.
    """
    rows = store.list_dead_letters(request.app.state.conn, status=status, limit=limit)
    return [record_payload(row) for row in rows]


@router.get("/admin/dlq/{dlq_id}")
async def get_dead_letter(dlq_id: int, request: Request) -> dict[str, object]:
    """One dead-letter row by its DLQ id (M4-T01); 404 when unknown."""
    row = store.get_dead_letter(request.app.state.conn, dlq_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown dlq_id: {dlq_id}")
    return record_payload(row)


class ReplayRequest(BaseModel):
    """``POST /admin/dlq/replay`` body (SPECS §5); both fields optional."""

    limit: int = Field(default=50, ge=1, le=1000)
    event_ids: list[str] = Field(default_factory=list)


@router.post("/admin/dlq/replay")
async def replay_dead_letters(request: Request, body: ReplayRequest | None = None) -> dict[str, object]:
    """Replay pending dead letters through the resilient boundary (M4-T04).

    Delegates entirely to :meth:`CallService.replay`, the same code path the
    CLI uses (SPECS §2: no logic duplication between surfaces). Rows the
    breaker refuses make no downstream call and are reported as ``deferred``
    rather than counted as attempted (SPECS §7.2).
    """
    payload = body or ReplayRequest()
    report: ReplayReport = await request.app.state.service.replay(
        limit=payload.limit,
        event_ids=payload.event_ids or None,
    )
    return report.as_body()


@router.get("/stats")
async def stats(request: Request) -> dict[str, object]:
    """Run summary for operators and the traffic generator (SPECS §5, §9).

    Assembled from the authoritative store (statuses, DLQ states, deferral
    depth) and the live breaker, not from in-process counters that would
    reset on restart. ``latency_percentiles`` are read from the Prometheus
    histograms, so they reflect every processed event since boot — and reset
    with the process, which is stated in the field docs.
    """
    breaker: CircuitBreaker = request.app.state.breaker
    return stats_body(
        request.app.state.conn,
        request.app.state.metrics,
        breaker=breaker,
        uptime_s=round(time.monotonic() - request.app.state.started_at, 1),
    )


def _json(body: dict[str, object], *, status_code: int) -> Response:
    """Explicit status control while keeping ``response_model`` out of the way."""
    return Response(
        content=json.dumps(body, default=str),
        status_code=status_code,
        media_type="application/json",
    )
