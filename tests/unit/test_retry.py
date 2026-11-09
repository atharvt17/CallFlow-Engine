"""Retry policy + classification unit tests (M3-T01, M3-T02; SPECS §6.1, §11).

Deterministic, no network, no real sleeps: policy defaults are asserted
against SPECS §6.1/§8, settings wiring is round-tripped, and the classifier
is exercised table-driven over every ``PipelineResult`` shape the HTTP
client can produce (see app/pipeline/http_client.py mapping).
"""

from __future__ import annotations

import random

import pytest

from app.config import Settings
from app.pipeline.base import PipelineResult
from app.resilience.retry import RetryDecision, RetryPolicy, backoff_delay, classify


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
        assert policy.worst_case_wall_time_max_jitter_s == pytest.approx(7.95)

    def test_wall_time_is_derived_from_non_default_policy(self) -> None:
        """Not merely reproducing the defaults: a non-default policy, checked
        by an independent closed-form calculation.

        max_attempts=4, timeout_s=1.0, base=50, factor=3.0, cap=400, j=0:
        base delays = min(50·3^0, 400)=50, min(150, 400)=150, min(450, 400)=400
        -> attempts 4×1.0 = 4.0s + backoff (0.05+0.15+0.40) = 4.6s.
        Independent closed form: base·(f^(n-1)-1)/(f-1) with the last term
        capped: 50 + 150 + 400 = 600 ms -> 0.6s.
        """
        policy = RetryPolicy(
            max_attempts=4,
            timeout_s=1.0,
            backoff_base_ms=50,
            backoff_factor=3.0,
            backoff_max_ms=400,
            backoff_jitter=0.0,
        )
        assert policy.worst_case_wall_time_s == pytest.approx(4.6)
        assert [policy.base_delay_ms(k) for k in (1, 2, 3)] == [50, 150, 400]

    def test_cap_is_applied_before_jitter(self) -> None:
        """Normative ordering (SPECS §6.1): cap bounds the base delay, then
        jitter scales it — a capped delay can exceed backoff_max_ms.

        jitter=0.5 on a capped 1000 ms delay -> ceiling 1500 ms. Under the
        opposite ordering (cap-after-jitter) the ceiling would be 1000 ms.
        """
        policy = RetryPolicy(
            max_attempts=2,  # one retry, so one capped delay
            timeout_s=0.001,  # near-zero so the assertion isolates the backoff
            backoff_base_ms=1000,
            backoff_factor=1.0,
            backoff_max_ms=1000,
            backoff_jitter=0.5,
        )
        assert policy.base_delay_ms(1) == 1000
        assert policy.worst_case_wall_time_s == pytest.approx(1.002)
        assert policy.worst_case_wall_time_max_jitter_s == pytest.approx(1.502)

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"max_attempts": 0}, "max_attempts"),
            ({"timeout_s": 0}, "timeout_s"),
            ({"backoff_base_ms": -1}, "backoff_base_ms"),
            ({"backoff_factor": 0.5}, "backoff_factor"),
            ({"backoff_max_ms": -1}, "backoff_max_ms"),
            ({"backoff_jitter": 1.5}, "backoff_jitter"),
        ],
    )
    def test_direct_construction_validates_ranges(self, kwargs: dict, message: str) -> None:
        """Direct construction cannot bypass Settings' range checks."""
        with pytest.raises(ValueError, match=message):
            RetryPolicy(**kwargs)

    def test_base_delay_is_one_indexed(self) -> None:
        with pytest.raises(ValueError, match="1-indexed"):
            RetryPolicy().base_delay_ms(0)


class TestBackoffDelay:
    """M3-T03 acceptance: jittered, capped, deterministic-under-seed delays."""

    def test_jitter_zero_sequence_is_exactly_100_200(self) -> None:
        policy = RetryPolicy(backoff_jitter=0.0)
        rng = random.Random(42)
        assert [backoff_delay(k, policy, rng) for k in (1, 2)] == [100.0, 200.0]

    def test_cap_holds_for_higher_attempts(self) -> None:
        """Exponential growth stops at backoff_max_ms (j=0 isolates the cap)."""
        policy = RetryPolicy(max_attempts=8, backoff_jitter=0.0)
        delays = [backoff_delay(k, policy, random.Random(0)) for k in range(1, 8)]
        assert delays == [100.0, 200.0, 400.0, 800.0, 1000.0, 1000.0, 1000.0]

    def test_jitter_stays_within_bounds(self) -> None:
        policy = RetryPolicy(backoff_jitter=0.5)
        rng = random.Random(7)
        for k in (1, 2, 3, 4, 5):
            base = policy.base_delay_ms(k)
            for _ in range(200):
                delay = backoff_delay(k, policy, rng)
                assert 0.5 * base <= delay <= 1.5 * base

    def test_jitter_actually_varies(self) -> None:
        policy = RetryPolicy(backoff_jitter=0.5)
        rng = random.Random(11)
        draws = {backoff_delay(1, policy, rng) for _ in range(50)}
        assert len(draws) > 40, "jitter must perturb each draw"

    def test_deterministic_under_seed(self) -> None:
        policy = RetryPolicy()
        first = [backoff_delay(k, policy, random.Random(1234)) for k in (1, 2)]
        second = [backoff_delay(k, policy, random.Random(1234)) for k in (1, 2)]
        assert first == second

    def test_capped_delay_can_exceed_cap_by_jitter(self) -> None:
        """Cap-before-jitter ordering (SPECS §6.1): at a capped delay the
        jittered result may exceed backoff_max_ms."""
        policy = RetryPolicy(max_attempts=2, backoff_base_ms=1000, backoff_factor=1.0,
                             backoff_max_ms=1000, backoff_jitter=0.5)
        rng = random.Random(0)
        draws = [backoff_delay(1, policy, rng) for _ in range(200)]
        assert max(draws) > 1000.0
        assert max(draws) <= 1500.0

    def test_retry_k_is_one_indexed(self) -> None:
        with pytest.raises(ValueError, match="1-indexed"):
            backoff_delay(0, RetryPolicy(), random.Random(0))

    def test_no_jitter_uses_uniform_call_but_is_exact(self) -> None:
        """j=0 must return the base exactly, even through rng.uniform(1,1)."""
        policy = RetryPolicy(backoff_jitter=0.0)
        for k in (1, 2, 3):
            assert backoff_delay(k, policy, random.Random(k)) == policy.base_delay_ms(k)


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
