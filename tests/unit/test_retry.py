"""Retry policy + classification unit tests (M3-T01, M3-T02; SPECS §6.1, §11).

Deterministic, no network, no real sleeps: policy defaults are asserted
against SPECS §6.1/§8, settings wiring is round-tripped, and the classifier
is exercised table-driven over every ``PipelineResult`` shape the HTTP
client can produce (see app/pipeline/http_client.py mapping).
"""

from __future__ import annotations

import asyncio
import random

import pytest

from app.config import Settings
from app.metrics import Metrics
from app.pipeline.base import PipelineResult
from app.resilience.retry import (
    AttemptRecord,
    RetryDecision,
    RetryOutcome,
    RetryPolicy,
    backoff_delay,
    classify,
    execute_with_retry,
)


class FakeSleeper:
    """Records sleeps instead of performing them (M3-T05: zero real sleeping)."""

    def __init__(self) -> None:
        self.sleeps: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


class FakeClock:
    """Monotonic fake clock; each call advances by a fixed step (ms→s)."""

    def __init__(self, step_s: float = 0.001) -> None:
        self.now = 0.0
        self.step_s = step_s

    def __call__(self) -> float:
        self.now += self.step_s
        return self.now


def _fail(reason: str = "downstream_timeout", status: int | None = 503) -> PipelineResult:
    return PipelineResult(ok=False, reason=reason, http_status=status, latency_ms=250.0)


def _ok() -> PipelineResult:
    return PipelineResult(ok=True, http_status=200, latency_ms=300.0)


class _ScriptedBackend:
    """Returns scripted results in order; counts calls."""

    def __init__(self, results: list[PipelineResult]) -> None:
        self._results = list(results)
        self.calls = 0

    async def __call__(self) -> PipelineResult:
        self.calls += 1
        return self._results.pop(0) if self._results else _fail()


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


class TestExecuteWithRetry:
    """M3-T04/T05 acceptance: deterministic executor, zero real sleeps."""

    async def test_success_first_attempt_no_sleep(self) -> None:
        backend = _ScriptedBackend([_ok()])
        sleeper = FakeSleeper()
        outcome = await execute_with_retry(backend, RetryPolicy(), sleeper=sleeper)
        assert isinstance(outcome, RetryOutcome)
        assert outcome.ok and outcome.attempts == 1 and outcome.retry_count == 0
        assert sleeper.sleeps == []
        assert backend.calls == 1
        assert isinstance(outcome.records[0], AttemptRecord)
        assert outcome.records[0].delay_before_ms == 0.0

    async def test_retries_on_spec_503_then_succeeds(self) -> None:
        backend = _ScriptedBackend([_fail(), _ok()])
        sleeper = FakeSleeper()
        policy = RetryPolicy(backoff_jitter=0.0)
        outcome = await execute_with_retry(backend, policy, sleeper=sleeper)
        assert outcome.ok and outcome.attempts == 2 and outcome.retry_count == 1
        assert sleeper.sleeps == [0.1]  # exactly one 100ms backoff (j=0)
        assert backend.calls == 2

    async def test_retries_on_timeout_and_connection_error(self) -> None:
        for reason in ("timeout", "connection_error"):
            backend = _ScriptedBackend([_fail(reason=reason, status=None), _ok()])
            sleeper = FakeSleeper()
            outcome = await execute_with_retry(backend, RetryPolicy(), sleeper=sleeper)
            assert outcome.ok and outcome.attempts == 2, reason

    async def test_no_retry_on_4xx(self) -> None:
        backend = _ScriptedBackend([_fail(reason="non_retryable_error", status=400)])
        sleeper = FakeSleeper()
        outcome = await execute_with_retry(backend, RetryPolicy(), sleeper=sleeper)
        assert not outcome.ok and outcome.attempts == 1
        assert sleeper.sleeps == []
        assert backend.calls == 1

    async def test_max_attempts_exhausted(self) -> None:
        backend = _ScriptedBackend([_fail(), _fail(), _fail()])
        sleeper = FakeSleeper()
        policy = RetryPolicy(backoff_jitter=0.0)
        outcome = await execute_with_retry(backend, policy, sleeper=sleeper)
        assert not outcome.ok
        assert outcome.attempts == 3 and outcome.retry_count == 2
        assert sleeper.sleeps == [0.1, 0.2]  # 3 attempts → 2 sleeps
        assert backend.calls == 3

    async def test_per_attempt_timeout_is_enforced(self) -> None:
        async def hanging() -> PipelineResult:
            await asyncio.sleep(10)  # would blow the test budget if not cut off
            return _ok()

        policy = RetryPolicy(max_attempts=1, timeout_s=0.01)
        outcome = await execute_with_retry(hanging, policy, sleeper=FakeSleeper())
        assert not outcome.ok
        assert outcome.result.reason == "timeout"
        assert outcome.records[0].timed_out is True

    async def test_unexpected_exception_is_retryable_connection_error(self) -> None:
        calls = {"n": 0}

        async def flaky() -> PipelineResult:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return _ok()

        sleeper = FakeSleeper()
        outcome = await execute_with_retry(flaky, RetryPolicy(), sleeper=sleeper)
        assert outcome.ok and outcome.attempts == 2
        assert outcome.records[0].result.reason == "connection_error"

    async def test_delay_before_recorded_on_records(self) -> None:
        backend = _ScriptedBackend([_fail(), _fail(), _ok()])
        policy = RetryPolicy(backoff_jitter=0.0)
        outcome = await execute_with_retry(backend, policy, sleeper=FakeSleeper())
        assert [r.delay_before_ms for r in outcome.records] == [0.0, 100.0, 200.0]

    async def test_clock_is_injectable(self) -> None:
        # No backend-reported latency → the executor measures with the clock.
        backend = _ScriptedBackend([PipelineResult(ok=True, http_status=200)])
        clock = FakeClock(step_s=1.0)
        outcome = await execute_with_retry(
            backend, RetryPolicy(), sleeper=FakeSleeper(), clock=clock
        )
        # One start + one end reading = 1 simulated second.
        assert outcome.records[0].latency_ms == pytest.approx(1000.0)

    async def test_backend_reported_latency_takes_precedence(self) -> None:
        backend = _ScriptedBackend([_ok()])
        clock = FakeClock(step_s=1.0)
        outcome = await execute_with_retry(
            backend, RetryPolicy(), sleeper=FakeSleeper(), clock=clock
        )
        assert outcome.records[0].latency_ms == 300.0

    async def test_wall_time_within_7_8s_bound(self) -> None:
        """All attempts' timeouts + the policy's backoff sum ≤ the bound."""
        policy = RetryPolicy()
        assert policy.worst_case_wall_time_s <= 7.8
        assert policy.worst_case_wall_time_max_jitter_s <= 7.95


class TestRetryMetrics:
    """M3-T06 acceptance: metric values match the attempt logs."""

    def _values(self, metrics: Metrics) -> dict[str, float]:
        body = metrics.render()[0].decode()
        return {
            line.split("{")[0]: float(line.rsplit(" ", 1)[1])
            for line in body.splitlines()
            if line and not line.startswith("#")
        }

    async def test_three_attempt_exhaustion_counts(self) -> None:
        metrics = Metrics()
        backend = _ScriptedBackend([_fail(), _fail(), _fail()])
        outcome = await execute_with_retry(
            backend, RetryPolicy(), sleeper=FakeSleeper(), metrics=metrics
        )
        assert outcome.attempts == 3
        body = metrics.render()[0].decode()
        assert 'pipeline_attempts_total{result="error"} 3.0' in body
        assert "retries_total 2.0" in body
        assert "pipeline_attempt_latency_ms_count 3.0" in body

    async def test_success_then_stop_metrics(self) -> None:
        metrics = Metrics()
        backend = _ScriptedBackend([_ok()])
        await execute_with_retry(backend, RetryPolicy(), sleeper=FakeSleeper(), metrics=metrics)
        body = metrics.render()[0].decode()
        assert 'pipeline_attempts_total{result="success"} 1.0' in body
        assert "retries_total 0.0" in body

    async def test_metrics_hooks_optional(self) -> None:
        """No metrics bundle must never break the executor."""
        outcome = await execute_with_retry(
            _ScriptedBackend([_ok()]), RetryPolicy(), sleeper=FakeSleeper()
        )
        assert outcome.ok
