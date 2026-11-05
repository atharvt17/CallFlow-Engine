"""Appendix B request/response models (SPECS §4), verbatim at defaults.

Field order matches the contract JSON so serialization is byte-stable:

- request:      ``{"event_id": ..., "audio_ref": ...}`` (both required; the
  endpoint maps missing fields to HTTP 400 in M1-T02)
- success 200:  ``{"status": "ok", "transcript": ..., "response_audio_ref":
  ..., "latency_ms": <sampled>}``
- failure 503:  ``{"status": "error", "reason": "downstream_timeout"}``

Unknown request fields are *ignored*, not rejected: neither Appendix B nor
SPECS §4 specifies behavior for extra fields (400 is reserved for malformed
bodies / missing required fields), and the mock stands in for a third-party
dependency — liberal acceptance avoids failing spec-conformant traffic over
undocumented strictness.

``event_id`` is deliberately a plain ``str`` here: the mock treats it as an
opaque correlation string (Appendix B's own sample values are not UUIDs); the
UUID validation rule belongs to the Appendix A / inference side (SPECS §3).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ProcessRequest(BaseModel):
    """``POST /pipeline/process`` request body (Appendix B)."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    audio_ref: str


class ProcessSuccess(BaseModel):
    """HTTP 200 success body (probability ``1 - FAILURE_RATE``)."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]
    transcript: str
    response_audio_ref: str
    latency_ms: float


class ProcessFailure(BaseModel):
    """HTTP 503 failure body (probability ``FAILURE_RATE``)."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["error"]
    reason: Literal["downstream_timeout"]


class AdminConfigUpdate(BaseModel):
    """Partial update for the documented ``POST /admin/config`` extension.

    Validation mirrors the env-level constraints (SPECS §4/§8). Extension
    only — it never changes the Appendix B-exact defaults that ship; invalid
    values are rejected (FastAPI 422) before any knob is applied.
    """

    model_config = ConfigDict(extra="forbid")

    failure_rate: float | None = Field(default=None, ge=0, le=1)
    latency_mu: float | None = None
    latency_sigma: float | None = Field(default=None, gt=0)
    failure_latency_ms: float | None = Field(default=None, ge=0)
