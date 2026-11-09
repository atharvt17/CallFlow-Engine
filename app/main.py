"""FastAPI application factory for the inference service (M0 skeleton + M2 core).

`GET /healthz` already exposes the SPECS §5 health contract. The circuit
breaker and deferral components land in Sprints 3–4; until then the health
endpoint reports placeholder values (``closed`` / ``0``) for their state, as
prescribed by the M0-T05 task definition.

Wiring (M2-T08): the store connection, pipeline backend, and call service are
built eagerly in :func:`create_app` (so ASGI-level tests see complete state
without lifespan), while cleanup runs in the lifespan shutdown hook.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel

from app import store
from app.api import router
from app.config import Settings
from app.logging import log_event, setup_logging
from app.metrics import Metrics
from app.pipeline import PipelineBackend, create_backend
from app.pipeline.resilient import ResilientPipelineClient
from app.resilience.circuit_breaker import CircuitBreaker
from app.service import CallService

logger = logging.getLogger(__name__)

# Placeholder value until deferral exists (Sprint 4).
_PLACEHOLDER_DEFERRED_PENDING = 0


class HealthResponse(BaseModel):
    """Body of ``GET /healthz`` per SPECS §5."""

    status: Literal["ok"]
    uptime_s: float
    circuit_state: str
    deferred_pending: int


def create_app(
    settings: Settings | None = None, backend: PipelineBackend | None = None
) -> FastAPI:
    """Build the inference app; settings and backend injectable for tests."""
    if settings is None:
        settings = Settings()
    if backend is None:
        backend = create_backend(settings)
    conn = store.connect(settings.db_path)
    store.init_db(conn)
    metrics = Metrics()
    breaker = CircuitBreaker.from_settings(settings, metrics=metrics)
    # Resilience wraps the boundary (M3-T14): retry + timeout + breaker
    # admission live here, so concrete backends stay policy-free.
    resilient = ResilientPipelineClient.from_settings(
        backend, breaker, settings, metrics=metrics
    )
    service = CallService(conn, resilient, settings, metrics)
    started_at = time.monotonic()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await backend.aclose()
            conn.close()

    app = FastAPI(
        title="collections-voice-agent inference",
        version="0.1.0",
        description="Resilient collections voice-agent inference service (Round 1).",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.conn = conn
    app.state.service = service
    app.state.backend = resilient
    app.state.raw_backend = backend
    app.state.metrics = metrics
    app.state.breaker = breaker
    app.state.started_at = started_at
    app.include_router(router)

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz() -> HealthResponse:
        return HealthResponse(
            status="ok",
            uptime_s=time.monotonic() - started_at,
            circuit_state=breaker.state.value,
            deferred_pending=_PLACEHOLDER_DEFERRED_PENDING,
        )

    return app


app = create_app()


def main() -> None:
    """Run the service with uvicorn, honoring SERVICE_HOST/SERVICE_PORT."""
    import uvicorn

    settings = Settings()
    setup_logging(settings.log_level)
    # Startup evidence line (M0-T07): the resolved dependency base URL,
    # e.g. the compose-network address `http://mock:8100`.
    log_event(logger, "startup", pipeline_base_url=settings.pipeline_base_url)
    uvicorn.run(
        "app.main:app",
        host=settings.service_host,
        port=settings.service_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
