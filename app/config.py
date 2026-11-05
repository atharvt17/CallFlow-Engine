"""Typed settings for the inference service (SPECS §8).

Every knob is a flat environment variable; pydantic-settings maps env vars onto
these fields case-insensitively (``SERVICE_HOST`` -> ``service_host``) and also
reads an optional local ``.env`` file. Defaults are the shipped SPECS §8
defaults. A shared ``.env`` may legitimately contain mock/traffic-gen variables,
so unknown keys are ignored rather than rejected.

Validation errors (bad ranges, unknown ``PIPELINE_BACKEND``) raise at
instantiation with an explicit message — invalid configuration never starts
the service silently.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All inference knobs from SPECS §8, with the documented defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- service bind ---
    service_host: str = "0.0.0.0"
    service_port: int = Field(default=8000, ge=1, le=65535)

    # --- pipeline backend (model-agnostic boundary) ---
    pipeline_backend: Literal["http", "inline"] = "http"
    pipeline_base_url: str = "http://localhost:8100"

    # --- retry policy (SPECS §6.1) ---
    max_attempts: int = Field(default=3, ge=1)
    pipeline_timeout_s: float = Field(default=2.5, gt=0)
    backoff_base_ms: int = Field(default=100, ge=0)
    backoff_factor: float = Field(default=2.0, ge=1)
    backoff_max_ms: int = Field(default=1000, ge=0)
    backoff_jitter: float = Field(default=0.5, ge=0, le=1)

    # --- circuit breaker (SPECS §6.2; logical-call accounting) ---
    cb_window_size: int = Field(default=20, ge=1)
    cb_failure_threshold: float = Field(default=0.5, gt=0, le=1)
    cb_min_throughput: int = Field(default=10, ge=1)
    cb_consecutive_failures: int = Field(default=5, ge=1)
    cb_cooldown_s: float = Field(default=5.0, ge=0)
    cb_half_open_probes: int = Field(default=3, ge=1)
    cb_half_open_success: int = Field(default=2, ge=1)

    # --- persistent deferral (SPECS §6.3) ---
    defer_queue_max: int = Field(default=5000, ge=1)
    defer_max_age_s: int = Field(default=3600, gt=0)

    # --- outcome mapping ---
    success_ptp_rate: float = Field(default=0.0, ge=0, le=1)

    # --- persistence ---
    db_path: str = "data/collections.db"

    # --- logging ---
    log_level: str = "INFO"
