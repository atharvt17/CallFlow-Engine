"""Pipeline backend package: boundary protocol, HTTP/inline clients, factory."""

from __future__ import annotations

from app.config import Settings
from app.pipeline.base import PipelineBackend, PipelineResult
from app.pipeline.http_client import HttpPipelineClient
from app.pipeline.inline_client import InlinePipelineClient

__all__ = [
    "HttpPipelineClient",
    "InlinePipelineClient",
    "PipelineBackend",
    "PipelineResult",
    "create_backend",
]


def create_backend(settings: Settings) -> PipelineBackend:
    """Build the backend selected by ``PIPELINE_BACKEND`` (``http`` | ``inline``)."""
    if settings.pipeline_backend == "http":
        return HttpPipelineClient(settings)
    if settings.pipeline_backend == "inline":
        return InlinePipelineClient()
    raise ValueError(  # pragma: no cover - Settings' Literal already rejects this
        f"unknown PIPELINE_BACKEND: {settings.pipeline_backend!r}"
    )
