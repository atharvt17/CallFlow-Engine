"""Typed settings for the Appendix B pipeline mock (SPECS §4/§8).

Defaults are Appendix B-exact; the documented `/admin/config` extension
(lands in M1-T03) adjusts behavior at runtime without changing these shipped
defaults. An optional shared `.env` is supported; unknown keys (inference /
traffic-gen variables) are ignored, and invalid values are rejected at
startup with explicit errors — the mock never runs with nonsense knobs.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MockSettings(BaseSettings):
    """All mock knobs from SPECS §4/§8, with the documented defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- service bind ---
    mock_port: int = Field(default=8100, ge=1, le=65535)

    # --- Appendix B failure/latency knobs (defaults are spec-exact) ---
    failure_rate: float = Field(default=0.20, ge=0, le=1)
    latency_mu: float = 5.7
    latency_sigma: float = Field(default=0.62, gt=0)
    failure_latency_ms: float = Field(default=0, ge=0)

    # --- logging (shared with the inference service per SPECS §8) ---
    log_level: str = "INFO"
