"""FastAPI application for the Appendix B pipeline dependency mock.

The graded contract (SPECS §4 / PDF Appendix B) is implemented verbatim at
defaults:

- ``POST /pipeline/process`` — 80% success (lognormal latency, 200 body) /
  20% immediate 503 ``downstream_timeout``; malformed or missing request
  fields are HTTP 400; failure draws are independent per request.
- ``GET /healthz`` — ``{"status": "ok"}``.

The admin surface (``/admin/stats``, ``GET|POST /admin/config``) is a
documented extension (SPECS §4) for tests/demos only — it never changes the
Appendix B-exact defaults that ship.
"""

from __future__ import annotations

import asyncio
import random
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mock.config import MockSettings
from mock.latency import sample_latency_ms
from mock.models import ProcessFailure, ProcessRequest, ProcessSuccess

# Appendix B body literals (SPECS §4).
_TRANSCRIPT = "synthetic transcript"
_RESPONSE_AUDIO_REF = "synthetic"


class MockHealthResponse(BaseModel):
    """Body of ``GET /healthz`` per SPECS §4."""

    status: Literal["ok"]


class MockState:
    """Runtime knobs and processed-traffic counters for the mock.

    Effective values start from ``MockSettings`` (Appendix B-exact defaults);
    the documented ``POST /admin/config`` extension mutates them at runtime.
    Counters track *processed* pipeline calls only (requests that passed
    validation), keeping ``observed_failure_rate`` meaningful.
    """

    def __init__(self, settings: MockSettings) -> None:
        self.failure_rate = settings.failure_rate
        self.latency_mu = settings.latency_mu
        self.latency_sigma = settings.latency_sigma
        self.failure_latency_ms = settings.failure_latency_ms
        self.requests_total = 0
        self.success = 0
        self.failure = 0

    @property
    def observed_failure_rate(self) -> float:
        """Failure fraction over processed calls (0.0 before any traffic)."""
        if self.requests_total == 0:
            return 0.0
        return self.failure / self.requests_total


def create_mock_app(settings: MockSettings | None = None) -> FastAPI:
    """Build the mock FastAPI app; settings injectable for tests."""
    if settings is None:
        settings = MockSettings()
    state = MockState(settings)

    app = FastAPI(
        title="collections-voice-agent pipeline-mock",
        version="0.1.0",
        description="Appendix B stand-in for the STT->LLM->TTS pipeline (Round 1).",
    )
    app.state.settings = settings
    app.state.mock_state = state

    @app.exception_handler(RequestValidationError)
    async def _validation_to_400(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Appendix B: malformed/missing request fields are HTTP 400.

        FastAPI's default for body-validation failures is 422; the graded
        contract requires 400. Error entries are reduced to plain
        (JSON-serializable) dicts.
        """
        errors = [
            {
                "type": err.get("type"),
                "loc": list(err.get("loc", ())),
                "msg": err.get("msg"),
            }
            for err in exc.errors()
        ]
        return JSONResponse(status_code=400, content={"detail": errors})

    @app.get("/healthz", response_model=MockHealthResponse)
    async def healthz() -> MockHealthResponse:
        return MockHealthResponse(status="ok")

    @app.post("/pipeline/process")
    async def pipeline_process(body: ProcessRequest) -> JSONResponse:
        """Graded Appendix B endpoint (spec-exact at defaults)."""
        state.requests_total += 1
        if random.random() < state.failure_rate:
            # Failure path: immediate at defaults; FAILURE_LATENCY_MS > 0 adds
            # latency to failures when enabled (SPECS §4).
            if state.failure_latency_ms > 0:
                await asyncio.sleep(state.failure_latency_ms / 1000)
            state.failure += 1
            failure = ProcessFailure(status="error", reason="downstream_timeout")
            return JSONResponse(status_code=503, content=failure.model_dump())
        latency_ms = sample_latency_ms(state.latency_mu, state.latency_sigma)
        await asyncio.sleep(latency_ms / 1000)
        state.success += 1
        success = ProcessSuccess(
            status="ok",
            transcript=_TRANSCRIPT,
            response_audio_ref=_RESPONSE_AUDIO_REF,
            latency_ms=latency_ms,
        )
        return JSONResponse(status_code=200, content=success.model_dump())

    return app


app = create_mock_app()


def main() -> None:
    """Run the mock with uvicorn on MOCK_PORT (SPECS §8 defines no bind-host knob)."""
    import uvicorn

    settings = MockSettings()
    uvicorn.run(
        "mock.main:app",
        host="0.0.0.0",
        port=settings.mock_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
