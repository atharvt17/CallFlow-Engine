"""HTTP backend targeting any Appendix B-conformant pipeline service.

Response mapping (M2-T05 / SPECS §6.1):

- 200 with the spec success body (``status == "ok"``, numeric
  ``latency_ms``) → ok result echoing ``latency_ms``;
- 200 with an unexpected body → ``non_retryable_error`` (logged);
- 5xx with the spec failure body (``reason == "downstream_timeout"``) →
  ``downstream_timeout``; 5xx with any other body → ``reason=None`` +
  ``http_status`` (the Sprint 3 classifier retries on any 5xx);
- 4xx / 3xx → ``non_retryable_error``;
- transport timeouts → ``timeout``; other transport failures (connect
  refused, read errors, …) → ``connection_error``.

``transport`` is a test seam (httpx.MockTransport / ASGITransport) so client
mapping is provable without a live dependency.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from app.config import Settings
from app.pipeline.base import PipelineResult

logger = logging.getLogger(__name__)

_SPEC_SUCCESS_STATUS = "ok"
_SPEC_FAILURE_REASON = "downstream_timeout"


class HttpPipelineClient:
    """``PipelineBackend`` over HTTP (per-attempt timeout from settings)."""

    def __init__(self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            base_url=settings.pipeline_base_url,
            timeout=settings.pipeline_timeout_s,
            transport=transport,
        )

    async def aclose(self) -> None:
        """Release the underlying connection pool (app shutdown)."""
        await self._client.aclose()

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        started = time.perf_counter()
        try:
            response = await self._client.post(
                "/pipeline/process",
                json={"event_id": event_id, "audio_ref": audio_ref},
            )
        except httpx.TimeoutException as exc:
            latency_ms = (time.perf_counter() - started) * 1000
            logger.warning(
                "pipeline attempt timed out",
                extra={"json_fields": {"event_id": event_id, "error": str(exc)}},
            )
            return PipelineResult(ok=False, reason="timeout", latency_ms=latency_ms)
        except httpx.TransportError as exc:
            latency_ms = (time.perf_counter() - started) * 1000
            logger.warning(
                "pipeline transport failure",
                extra={"json_fields": {"event_id": event_id, "error": str(exc)}},
            )
            return PipelineResult(ok=False, reason="connection_error", latency_ms=latency_ms)

        latency_ms = (time.perf_counter() - started) * 1000
        status = response.status_code

        if 200 <= status < 300:
            return self._map_success(response, status, latency_ms, event_id)
        if status >= 500:
            return self._map_server_error(response, status, latency_ms)
        logger.info(
            "pipeline returned non-retryable status",
            extra={"json_fields": {"event_id": event_id, "http_status": status}},
        )
        return PipelineResult(
            ok=False, reason="non_retryable_error", http_status=status, latency_ms=latency_ms
        )

    def _map_success(
        self, response: httpx.Response, status: int, latency_ms: float, event_id: str
    ) -> PipelineResult:
        body = self._parse_json(response)
        if body is None or body.get("status") != _SPEC_SUCCESS_STATUS or not isinstance(
            body.get("latency_ms"), (int, float)
        ):
            logger.warning(
                "pipeline returned 2xx with unexpected body",
                extra={
                    "json_fields": {
                        "event_id": event_id,
                        "http_status": status,
                        "body": str(response.text)[:200],
                    }
                },
            )
            return PipelineResult(
                ok=False, reason="non_retryable_error", http_status=status, latency_ms=latency_ms
            )
        return PipelineResult(
            ok=True, http_status=status, latency_ms=float(body["latency_ms"])
        )

    def _map_server_error(
        self, response: httpx.Response, status: int, latency_ms: float
    ) -> PipelineResult:
        body = self._parse_json(response)
        if isinstance(body, dict) and body.get("reason") == _SPEC_FAILURE_REASON:
            return PipelineResult(
                ok=False, reason="downstream_timeout", http_status=status, latency_ms=latency_ms
            )
        # Non-spec 5xx: keep the status (the classifier retries any 5xx); the
        # reason vocabulary stays reserved rather than mislabeling the failure.
        return PipelineResult(ok=False, reason=None, http_status=status, latency_ms=latency_ms)

    @staticmethod
    def _parse_json(response: httpx.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            return None
