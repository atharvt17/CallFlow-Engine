"""Inline no-op backend for tests and ``PIPELINE_BACKEND=inline``."""

from __future__ import annotations

from app.pipeline.base import PipelineResult


class InlinePipelineClient:
    """Immediate success echo — no network, no latency; never fails."""

    async def aclose(self) -> None:
        """Nothing to release (uniform lifecycle with the HTTP client)."""

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        return PipelineResult(ok=True, http_status=200, latency_ms=0.0)
