"""Retry policy + classification unit tests (M3-T01, M3-T02; SPECS §6.1, §11).

Deterministic, no network, no real sleeps: policy defaults are asserted
against SPECS §6.1/§8, settings wiring is round-tripped, and the classifier
is exercised table-driven over every ``PipelineResult`` shape the HTTP
client can produce (see app/pipeline/http_client.py mapping).
"""

from __future__ import annotations

import pytest

from app.config import Settings
from app.pipeline.base import PipelineResult
from app.resilience.retry import RetryDecision, RetryPolicy, classify


class TestRetryPolicyDefaults:
    """M3-T01 acceptance: defaults match SPECS §6.1/§8 exactly."""

    def test_defaults_match_spec(self) -> None:
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.timeout_s == 2.5
        assert policy.backoff_base_ms == 100
        assert policy.backoff_factor == 2.0
        assert policy.backoff_max_ms == 1000
        assert policy.backoff_jitter == 0.5

    def test_from_settings_round_trip(self) -> None:
        settings = Settings()
        policy = RetryPolicy.from_settings(settings)
        assert policy.max_attempts == settings.max_attempts
        assert policy.timeout_s == settings.pipeline_timeout_s
        assert policy.backoff_base_ms == settings.backoff_base_ms
        assert policy.backoff_factor == settings.backoff_factor
        assert policy.backoff_max_ms == settings.backoff_max_ms
        assert policy.backoff_jitter == settings.backoff_jitter

    def test_from_settings_honors_overrides(self) -> None:
        settings = Settings(max_attempts=5, pipeline_timeout_s=1.0, backoff_base_ms=10)
        policy = RetryPolicy.from_settings(settings)
        assert (policy.max_attempts, policy.timeout_s, policy.backoff_base_ms) == (5, 1.0, 10)

    def test_policy_is_immutable(self) -> None:
        policy = RetryPolicy()
        with pytest.raises(AttributeError):
            policy.max_attempts = 99  # type: ignore[misc]

    def test_worst_case_bound_is_7_8s_nominal(self) -> None:
        # SPECS §6.1: (3 × 2.5s) + 0.1s + 0.2s = 7.8s (≈7.95s at max jitter).
        assert RetryPolicy().worst_case_wall_time_s == pytest.approx(7.8)
        # Max-jitter ceiling: each delay ≤ base·factor^(k-1)·(1+j) capped.
        policy = RetryPolicy()
        jittered = (
            policy.max_attempts * policy.timeout_s
            + sum(
                min(policy.backoff_base_ms * policy.backoff_factor ** (k - 1), policy.backoff_max_ms)
                * (1 + policy.backoff_jitter)
                / 1000.0
                for k in range(1, policy.max_attempts)
            )
        )
        assert jittered == pytest.approx(7.95)


class TestClassify:
    """M3-T02 acceptance: classification matches SPECS §6.1 exactly.

    Retryable: timeout, connection_error, downstream_timeout (the spec
    503), and any 5xx — including non-spec 5xx bodies reported as
    ``reason=None`` + ``http_status``. Not retryable: non_retryable_error
    (4xx, malformed 2xx) and success.
    """

    @pytest.mark.parametrize(
        "result",
        [
            PipelineResult(ok=False, reason="downstream_timeout", http_status=503),
            PipelineResult(ok=False, reason="timeout"),
            PipelineResult(ok=False, reason="connection_error"),
            # Non-spec 5xx: reason stays None, http_status carries the signal.
            PipelineResult(ok=False, reason=None, http_status=500),
            PipelineResult(ok=False, reason=None, http_status=502),
            PipelineResult(ok=False, reason=None, http_status=503),
        ],
    )
    def test_retryable(self, result: PipelineResult) -> None:
        assert classify(result) is RetryDecision.RETRY

    @pytest.mark.parametrize(
        "result",
        [
            PipelineResult(ok=True, http_status=200, latency_ms=312.0),
            PipelineResult(ok=False, reason="non_retryable_error", http_status=400),
            PipelineResult(ok=False, reason="non_retryable_error", http_status=422),
            # Malformed 2xx body maps to non_retryable_error with 200 status.
            PipelineResult(ok=False, reason="non_retryable_error", http_status=200),
        ],
    )
    def test_not_retryable(self, result: PipelineResult) -> None:
        assert classify(result) is RetryDecision.STOP

    def test_failure_without_any_signal_stops(self) -> None:
        # Defensive branch: no reason, no status — must not loop forever.
        assert classify(PipelineResult(ok=False)) is RetryDecision.STOP
