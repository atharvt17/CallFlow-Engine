"""Public inference API (M2-T08, Sprint 4; SPECS §5).

- ``POST /calls`` — Appendix A event validation (422 on schema errors via
  FastAPI defaults); 200 processed / failed_downstream / duplicate; **202**
  when the breaker accepted the event for later processing (§6.3);
- ``GET /calls/{event_id}`` — stored record (200) / 404;
- ``POST /admin/deferred/drain`` — force one drain pass (§5);
- errors use the ``{"detail": ...}`` envelope.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request, Response

from app import store
from app.models import CallEvent
from app.resilience.circuit_breaker import CircuitBreaker
from app.service import CallAcceptance, CallService

router = APIRouter()


def _record_payload(row) -> dict[str, object]:
    """Stored row → response record; ``event_json`` parsed under ``event``."""
    record = dict(row)
    record["event"] = json.loads(record.pop("event_json"))
    return record


@router.post("/calls")
async def submit_call(request: Request, event: CallEvent) -> Response:
    service: CallService = request.app.state.service
    acceptance: CallAcceptance = await service.accept(event)

    if acceptance.duplicate:
        row = store.get_event(request.app.state.conn, acceptance.event_id)
        if row is None:  # pragma: no cover - duplicate implies an existing row
            raise HTTPException(status_code=500, detail="duplicate event record missing")
        return _json(_record_payload(row) | {"duplicate": True}, status_code=200)

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
    return _record_payload(row)


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


def _json(body: dict[str, object], *, status_code: int) -> Response:
    """Explicit status control while keeping ``response_model`` out of the way."""
    return Response(
        content=json.dumps(body, default=str),
        status_code=status_code,
        media_type="application/json",
    )
