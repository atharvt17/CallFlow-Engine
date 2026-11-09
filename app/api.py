"""Public inference API (M2-T08; SPECS §5).

- ``POST /calls`` — Appendix A event validation (422 on schema errors via
  FastAPI defaults), 200 processed / failed_downstream / duplicate, 202
  deferred in Sprint 4;
- ``GET /calls/{event_id}`` — stored record (200) / 404;
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
async def submit_call(request: Request, event: CallEvent) -> dict[str, object]:
    service: CallService = request.app.state.service
    acceptance: CallAcceptance = await service.accept(event)

    if acceptance.duplicate:
        row = store.get_event(request.app.state.conn, acceptance.event_id)
        if row is None:  # pragma: no cover - duplicate implies an existing row
            raise HTTPException(status_code=500, detail="duplicate event record missing")
        return _record_payload(row) | {"duplicate": True}

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
        # dlq_id stays NULL until the DLQ write path lands (M3-T20).
        body["failure_reason"] = acceptance.failure_reason
        body["dlq_id"] = None
    return body


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
