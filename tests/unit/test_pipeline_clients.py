"""Backend client response-mapping tests (M2-T06) — no live dependency.

Every ``PipelineResult.reason`` branch of ``HttpPipelineClient`` is covered
via ``httpx.MockTransport`` handlers; the inline client's echo contract is
locked as well.
"""

from __future__ import annotations

import json

import httpx

from app.config import Settings
from app.pipeline.http_client import HttpPipelineClient
from app.pipeline.inline_client import InlinePipelineClient

SETTINGS = Settings(pipeline_base_url="http://mock.test", pipeline_timeout_s=2.5)

_SUCCESS_BODY = {
    "status": "ok",
    "transcript": "synthetic transcript",
    "response_audio_ref": "synthetic",
    "latency_ms": 312,
}

_FAILURE_BODY = {"status": "error", "reason": "downstream_timeout"}


def make_http_client(handler) -> HttpPipelineClient:
    """Backend with the given MockTransport handler (test seam from M2-T05)."""
    return HttpPipelineClient(SETTINGS, transport=httpx.MockTransport(handler))


async def test_200_success_body_maps_to_ok() -> None:
    client = make_http_client(lambda request: httpx.Response(200, json=_SUCCESS_BODY))
    result = await client.process("e-1", "synthetic")
    assert result.ok is True
    assert result.reason is None
    assert result.http_status == 200
    assert result.latency_ms == 312.0


async def test_503_spec_body_maps_to_downstream_timeout() -> None:
    client = make_http_client(lambda request: httpx.Response(503, json=_FAILURE_BODY))
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "downstream_timeout"
    assert result.http_status == 503


async def test_400_maps_to_non_retryable_error() -> None:
    client = make_http_client(lambda request: httpx.Response(400, json={"detail": "bad"}))
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "non_retryable_error"
    assert result.http_status == 400


async def test_malformed_200_body_maps_to_non_retryable_error() -> None:
    client = make_http_client(lambda request: httpx.Response(200, content=b"{not json"))
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "non_retryable_error"
    assert result.http_status == 200


async def test_200_unexpected_json_maps_to_non_retryable_error() -> None:
    body = {"status": "bogus", "latency_ms": 100}
    client = make_http_client(lambda request: httpx.Response(200, json=body))
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "non_retryable_error"


async def test_200_non_numeric_latency_maps_to_non_retryable_error() -> None:
    body = _SUCCESS_BODY | {"latency_ms": "fast"}
    client = make_http_client(lambda request: httpx.Response(200, json=body))
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "non_retryable_error"


async def test_5xx_non_spec_body_keeps_status_reason_reserved() -> None:
    """Non-spec 5xx: retryability rides on http_status; reason stays None."""
    client = make_http_client(lambda request: httpx.Response(500, json={"oops": True}))
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason is None
    assert result.http_status == 500


async def test_transport_timeout_maps_to_timeout() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = make_http_client(handler)
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "timeout"
    assert result.latency_ms is not None and result.latency_ms >= 0


async def test_connect_refusal_maps_to_connection_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = make_http_client(handler)
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "connection_error"


async def test_read_error_maps_to_connection_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("read failed", request=request)

    client = make_http_client(handler)
    result = await client.process("e-1", "synthetic")
    assert result.ok is False
    assert result.reason == "connection_error"


async def test_inline_client_always_succeeds() -> None:
    client = InlinePipelineClient()
    for i in range(3):
        result = await client.process(f"e-{i}", "synthetic")
        assert result.ok is True
        assert result.reason is None
        assert result.http_status == 200
        assert result.latency_ms == 0.0


async def test_request_payload_is_appendix_b_shape() -> None:
    """The HTTP client posts exactly {event_id, audio_ref} to /pipeline/process."""
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["json"] = json.loads(request.content.decode())
        return httpx.Response(200, json=_SUCCESS_BODY)

    client = make_http_client(handler)
    await client.process("e-9", "synthetic")
    assert seen["path"] == "/pipeline/process"
    assert seen["json"] == {"event_id": "e-9", "audio_ref": "synthetic"}
