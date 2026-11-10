"""FastAPI application factory for the inference service (M0 skeleton + M2 core).

`GET /healthz` exposes the SPECS §5 health contract with the live breaker
state and the durable deferred count (Sprint 4 replaced the M0 placeholders).

Wiring (M2-T08, Sprint 4): the store connection, pipeline backend, call
service and deferral queue are built eagerly in :func:`create_app` (so
ASGI-level tests see complete state without lifespan), while the background
drainer and cleanup run in the lifespan hooks. Startup recovery is also eager:
the queue is reconstructed from SQLite at factory time, so an app built in a
test sees its recovered backlog without running lifespan — recovery is
idempotent, so a second construction against the same file is a no-op.
"""

from __future__ import annotations

import asyncio
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
from app.deferral import DeferralQueue
from app.logging import log_event, setup_logging
from app.pipeline import PipelineBackend
from app.runtime import ServiceRuntime

logger = logging.getLogger(__name__)

# Background drainer cadence (SPECS §6.3: "runs when the breaker closes").
DRAIN_INTERVAL_S = 1.0


class HealthResponse(BaseModel):
    """Body of ``GET /healthz`` per SPECS §5."""

    status: Literal["ok"]
    uptime_s: float
    circuit_state: str
    deferred_pending: int


def create_app(
    settings: Settings | None = None,
    backend: PipelineBackend | None = None,
    *,
    run_drainer: bool = True,
) -> FastAPI:
    """Build the inference app; settings and backend injectable for tests."""
    if settings is None:
        settings = Settings()
    # One wiring path with the CLI (M4-T02): the same store + resilience stack
    # + service, so a replay from either surface behaves identically.
    runtime = ServiceRuntime.build(settings, backend)
    conn = runtime.conn
    metrics = runtime.metrics
    breaker = runtime.breaker
    service = runtime.service
    deferral = DeferralQueue(conn, service, settings, metrics)
    service.bind_deferral(deferral)
    # Reconstruct the queue from SQLite before the app serves anything
    # (SPECS §6.3). Idempotent, so eager execution is safe for tests that
    # never run lifespan.
    deferral.recover()
    started_at = time.monotonic()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        stop = asyncio.Event()
        drainer: asyncio.Task[None] | None = None
        if run_drainer:
            drainer = asyncio.create_task(
                deferral.drainer_loop(stop, interval_s=DRAIN_INTERVAL_S),
                name="deferred-drainer",
            )
        try:
            yield
        finally:
            stop.set()
            if drainer is not None:
                drainer.cancel()
                try:
                    await drainer
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("deferred drainer stopped with an error")
            await runtime.aclose()

    app = FastAPI(
        title="collections-voice-agent inference",
        version="0.1.0",
        description="Resilient collections voice-agent inference service (Round 1).",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.conn = conn
    app.state.service = service
    app.state.deferral = deferral
    app.state.backend = runtime.backend
    app.state.raw_backend = runtime.raw_backend
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
            deferred_pending=store.count_deferred(conn),
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
