"""FastAPI skeleton for the Appendix B pipeline dependency mock.

The graded contract endpoint (`POST /pipeline/process`) and the admin surface
land in Sprint 1; this module currently provides the app factory and the
`GET /healthz` liveness endpoint per SPECS §4.
"""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel

from mock.config import MockSettings


class MockHealthResponse(BaseModel):
    """Body of ``GET /healthz`` per SPECS §4."""

    status: Literal["ok"]


def create_mock_app(settings: MockSettings | None = None) -> FastAPI:
    """Build the mock FastAPI app; settings injectable for tests."""
    if settings is None:
        settings = MockSettings()

    app = FastAPI(
        title="collections-voice-agent pipeline-mock",
        version="0.1.0",
        description="Appendix B stand-in for the STT->LLM->TTS pipeline (Round 1).",
    )
    app.state.settings = settings

    @app.get("/healthz", response_model=MockHealthResponse)
    async def healthz() -> MockHealthResponse:
        return MockHealthResponse(status="ok")

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
