"""FastAPI application factory for the inference service (M0 skeleton).

`GET /healthz` already exposes the SPECS §5 health contract. The circuit
breaker and deferral components land in Sprints 3–4; until then the health
endpoint reports placeholder values (``closed`` / ``0``) for their state, as
prescribed by the M0-T05 task definition.
"""

from __future__ import annotations

import logging
import time
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel

from app.config import Settings

logger = logging.getLogger(__name__)

# Placeholder values until the breaker (Sprint 3) and deferral (Sprint 4) exist.
_PLACEHOLDER_CIRCUIT_STATE = "closed"
_PLACEHOLDER_DEFERRED_PENDING = 0


class HealthResponse(BaseModel):
    """Body of ``GET /healthz`` per SPECS §5."""

    status: Literal["ok"]
    uptime_s: float
    circuit_state: str
    deferred_pending: int


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the inference FastAPI app; settings injectable for tests."""
    if settings is None:
        settings = Settings()
    started_at = time.monotonic()

    app = FastAPI(
        title="collections-voice-agent inference",
        version="0.1.0",
        description="Resilient collections voice-agent inference service (Round 1).",
    )
    app.state.settings = settings

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz() -> HealthResponse:
        return HealthResponse(
            status="ok",
            uptime_s=time.monotonic() - started_at,
            circuit_state=_PLACEHOLDER_CIRCUIT_STATE,
            deferred_pending=_PLACEHOLDER_DEFERRED_PENDING,
        )

    return app


app = create_app()


def main() -> None:
    """Run the service with uvicorn, honoring SERVICE_HOST/SERVICE_PORT."""
    import uvicorn

    settings = Settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Startup evidence line (M0-T07): shows the resolved dependency base URL,
    # e.g. the compose-network address `http://mock:8100`.
    logger.info(
        "inference starting: pipeline backend base URL = %s",
        settings.pipeline_base_url,
    )
    uvicorn.run(
        "app.main:app",
        host=settings.service_host,
        port=settings.service_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
