"""Circuit-breaker unit tests (M3-T07–T09; SPECS §6.2, §11).

Deterministic: a fake clock drives cooldown, and outcomes are recorded
directly — no sleeping, no backend. The accounting unit under test is the
**completed logical call**, so every ``record_logical_outcome`` here
represents one whole retry sequence.

M3-T09 adds the HALF_OPEN probe-permit lifecycle: one permit = one complete
logical call (its whole retry sequence), acquired before dispatch, held
through every attempt, released exactly once, never leaked by cancellation
or unexpected exceptions, and never counted per attempt.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.config import Settings
from app.metrics import Metrics
from app.pipeline.base import PipelineResult
from app.resilience.circuit_breaker import CircuitBreaker, CircuitState
from app.resilience.retry import RetryPolicy, execute_with_retry


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_breaker(clock: FakeClock | None = None, **overrides) -> CircuitBreaker:
    clock = clock or FakeClock()
    params = {
        "window_size": 20,
        "failure_threshold": 0.5,
        "min_throughput": 10,
        "consecutive_failures": 5,
        "cooldown_s": 5.0,
        "half_open_probes": 3,
        "half_open_success": 2,
        "clock": clock,
    }
    params.update(overrides)
    return CircuitBreaker(**params)


def trip_open(breaker: CircuitBreaker, clock: FakeClock) -> None:
    """Drive to OPEN via consecutive failures, then advance past cooldown."""
    for _ in range(5):
        breaker.record_logical_outcome(success=False)
    assert breaker.state is CircuitState.OPEN


class TestDefaultsAndSettings:
    def test_from_settings_uses_spec_defaults(self) -> None:
        breaker = CircuitBreaker.from_settings(Settings())
        assert breaker.window_size == 20
        assert breaker.failure_ratio == 0.0
        assert breaker.state is CircuitState.CLOSED

    def test_invalid_construction_rejected(self) -> None:
        with pytest.raises(ValueError):
            CircuitBreaker(window_size=0)
        with pytest.raises(ValueError):
            CircuitBreaker(failure_threshold=0)
        with pytest.raises(ValueError):
            CircuitBreaker(min_throughput=0)
        with pytest.raises(ValueError):
            CircuitBreaker(consecutive_failures=0)
        with pytest.raises(ValueError):
            CircuitBreaker(cooldown_s=-1)
        with pytest.raises(ValueError):
            CircuitBreaker(half_open_probes=0)
        with pytest.raises(ValueError):
            CircuitBreaker(half_open_success=0)


class TestTripRules:
    """M3-T08 acceptance: one entry per logical call; exact boundaries."""

    def test_one_window_entry_per_logical_call(self) -> None:
        breaker = make_breaker()
        for _ in range(7):
            breaker.record_logical_outcome(success=True)
        assert breaker.window_samples == 7
        assert breaker.window_failures == 0

    def test_ratio_trip_at_threshold(self) -> None:
        breaker = make_breaker()
        for _ in range(5):
            breaker.record_logical_outcome(success=False)
        for _ in range(5):
            breaker.record_logical_outcome(success=True)
        # 5/10 = 0.5 ≥ 0.5, throughput threshold met exactly → trip.
        assert breaker.state is CircuitState.OPEN

    def test_min_throughput_boundary_nine_samples(self) -> None:
        """9 failures with min_throughput=10 must NOT trip on ratio (and the
        consecutive counter is deliberately raised out of the way)."""
        breaker = make_breaker(consecutive_failures=100)
        for _ in range(9):
            breaker.record_logical_outcome(success=False)
        assert breaker.failure_ratio == 1.0
        assert breaker.state is CircuitState.CLOSED

    def test_min_throughput_boundary_ten_samples(self) -> None:
        breaker = make_breaker(consecutive_failures=100)
        for _ in range(10):
            breaker.record_logical_outcome(success=False)
        assert breaker.state is CircuitState.OPEN

    def test_ratio_below_threshold_does_not_trip(self) -> None:
        breaker = make_breaker(consecutive_failures=100)
        # 4 failures + 6 successes = 0.4 < 0.5
        for _ in range(4):
            breaker.record_logical_outcome(success=False)
        for _ in range(6):
            breaker.record_logical_outcome(success=True)
        assert breaker.state is CircuitState.CLOSED

    def test_consecutive_trip_before_throughput(self) -> None:
        # 5 consecutive failures trips even though the window is only 5.
        breaker = make_breaker()
        for _ in range(5):
            breaker.record_logical_outcome(success=False)
        assert breaker.state is CircuitState.OPEN
        assert breaker.window_samples == 5

    def test_success_resets_consecutive_counter(self) -> None:
        breaker = make_breaker()
        for _ in range(4):
            breaker.record_logical_outcome(success=False)
        breaker.record_logical_outcome(success=True)
        assert breaker.consecutive_failures == 0
        for _ in range(4):
            breaker.record_logical_outcome(success=False)
        assert breaker.state is CircuitState.CLOSED  # never 5 in a row

    def test_sliding_window_forgets_old_outcomes(self) -> None:
        breaker = make_breaker(failure_threshold=0.5, consecutive_failures=100)
        for _ in range(10):
            breaker.record_logical_outcome(success=True)
        for _ in range(10):
            breaker.record_logical_outcome(success=False)
        # Window of 20 now holds exactly 10/10 → ratio 0.5 trips.
        assert breaker.state is CircuitState.OPEN
        assert breaker.window_samples == 20


class TestTransientProfileDoesNotTrip:
    """SPECS §6.2: Appendix B's 20% per-attempt rate is absorbed by retries.

    A logical call fails only if all 3 attempts fail (0.2³ = 0.8%). A
    deterministic stand-in: 1 failure in every 125 logical calls, plus the
    worst realistic cluster (a few failures, never 5 in a row).
    """

    def test_zero_point_eight_percent_failure_profile(self) -> None:
        breaker = make_breaker()
        failed = 0
        for i in range(1000):
            if i % 125 == 0:  # 8 failures per 1000 = 0.8%
                breaker.record_logical_outcome(success=False)
                failed += 1
            else:
                breaker.record_logical_outcome(success=True)
        assert failed == 8
        assert breaker.state is CircuitState.CLOSED

    def test_sparse_failures_never_reach_consecutive_threshold(self) -> None:
        breaker = make_breaker()
        pattern = [True, True, False, True, True, True, False, True]  # ≤1 fail in a row
        for _ in range(50):
            for success in pattern:
                breaker.record_logical_outcome(success=success)
        assert breaker.state is CircuitState.CLOSED


class TestOpenGate:
    def test_open_rejects_without_recording(self) -> None:
        breaker = make_breaker()
        trip_open(breaker, FakeClock())
        samples_before = breaker.window_samples
        consecutive_before = breaker.consecutive_failures
        for _ in range(10):
            assert breaker.allow_request() is False
        # Rejections are not outcomes: no evidence, no statistics movement.
        assert breaker.window_samples == samples_before
        assert breaker.consecutive_failures == consecutive_before

    def test_open_returns_false_before_cooldown(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        clock.advance(4.999)
        assert breaker.allow_request() is False
        assert breaker.state is CircuitState.OPEN

    def test_cooldown_advances_to_half_open(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN

    def test_half_open_probe_budget(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock, half_open_probes=3)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert [breaker.allow_request() for _ in range(4)] == [True, True, True, False]

    def test_probe_success_two_closes_and_resets_window(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock, half_open_success=2)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.allow_request() is True
        breaker.record_logical_outcome(success=True)
        assert breaker.state is CircuitState.HALF_OPEN
        assert breaker.allow_request() is True
        breaker.record_logical_outcome(success=True)
        assert breaker.state is CircuitState.CLOSED
        assert breaker.window_samples == 0  # reset on close
        assert breaker.consecutive_failures == 0

    def test_post_close_failures_start_from_clean_window(self) -> None:
        """Reset-on-close: a just-recovered breaker must not re-trip from
        stale outage failures."""
        clock = FakeClock()
        breaker = make_breaker(clock=clock, half_open_success=2)
        trip_open(breaker, clock)
        clock.advance(5.0)
        breaker.allow_request()
        breaker.record_logical_outcome(success=True)
        breaker.allow_request()
        breaker.record_logical_outcome(success=True)
        assert breaker.state is CircuitState.CLOSED
        # Four fresh failures (< consecutive threshold, window < min) stay closed.
        for _ in range(4):
            breaker.record_logical_outcome(success=False)
        assert breaker.state is CircuitState.CLOSED

    def test_probe_failure_reopens_and_restarts_cooldown(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.allow_request() is True
        breaker.record_logical_outcome(success=False)
        assert breaker.state is CircuitState.OPEN
        assert breaker.opened_at == clock.now
        # Cooldown restarts from the re-open time.
        clock.advance(4.999)
        assert breaker.state is CircuitState.OPEN
        clock.advance(0.001)
        assert breaker.state is CircuitState.HALF_OPEN

    def test_late_outcome_while_open_is_inert(self) -> None:
        """A logical call dispatched before the trip finishing after it must
        not change state or the window."""
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        samples = breaker.window_samples
        breaker.record_logical_outcome(success=True)  # late result
        assert breaker.state is CircuitState.OPEN
        assert breaker.window_samples == samples


class FakeSleeper:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def _ok() -> PipelineResult:
    return PipelineResult(ok=True, http_status=200, latency_ms=300.0)


def _fail() -> PipelineResult:
    return PipelineResult(
        ok=False, reason="downstream_timeout", http_status=503, latency_ms=250.0
    )


class ScriptedBackend:
    """One downstream attempt per call; returns scripted results in order."""

    def __init__(self, results: list[PipelineResult]) -> None:
        self._results = list(results)
        self.calls = 0

    async def __call__(self) -> PipelineResult:
        self.calls += 1
        return self._results.pop(0) if self._results else _fail()


async def logical_call(
    breaker: CircuitBreaker,
    op,
    *,
    policy: RetryPolicy | None = None,
    sleeper=None,
    on_attempt=None,
):
    """The M3-T14 decorator shape, in miniature.

    One permit is acquired BEFORE ``execute_with_retry`` and held across the
    whole retry sequence. ``with permit`` is the production safety net:
    cancellation and unexpected exceptions release it without recording
    probe evidence; the normal path calls ``finish`` exactly once.
    """
    permit = breaker.acquire_probe()
    if permit is None:
        return None
    with permit:
        outcome = await execute_with_retry(
            op,
            policy or RetryPolicy(backoff_jitter=0.0),
            sleeper=sleeper or FakeSleeper(),
            on_attempt=on_attempt,
        )
        permit.finish(success=outcome.ok)
    return outcome


class TestHalfOpenProbePermits:
    """M3-T09: a probe permit is one complete logical call, never one attempt."""

    def _half_open(self, clock: FakeClock, **overrides) -> CircuitBreaker:
        breaker = make_breaker(clock=clock, **overrides)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        return breaker

    def test_admits_up_to_configured_budget(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3)
        permits = [breaker.acquire_probe() for _ in range(3)]
        assert all(permit is not None and permit.is_probe for permit in permits)
        assert breaker.half_open_in_flight == 3

    def test_rejects_when_budget_exhausted(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3)
        for _ in range(3):
            assert breaker.acquire_probe() is not None
        assert breaker.acquire_probe() is None
        assert breaker.half_open_in_flight == 3

    def test_open_breaker_refuses_permit(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        assert breaker.acquire_probe() is None

    def test_permit_release_after_success_frees_budget_only(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3, half_open_success=2)
        permit = breaker.acquire_probe()
        assert breaker.half_open_in_flight == 1
        permit.finish(success=True)
        assert permit.released is True
        assert breaker.half_open_in_flight == 0
        # One success is not enough to close (threshold 2) …
        assert breaker.state is CircuitState.HALF_OPEN
        # … and the freed slot is reusable.
        assert breaker.acquire_probe() is not None

    def test_permit_release_after_failure_reopens(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        permit = breaker.acquire_probe()
        permit.finish(success=False)
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.OPEN
        assert breaker.opened_at == clock.now

    def test_release_is_idempotent(self) -> None:
        """Releasing twice must not over-restore the budget or double-count
        evidence (defensive: ``with permit`` and an explicit release can both
        fire)."""
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_success=2)
        permit = breaker.acquire_probe()
        permit.finish(success=True)
        permit.finish(success=True)
        permit.release()
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.HALF_OPEN  # still only 1 success

    def test_context_manager_releases_without_evidence(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        permit = breaker.acquire_probe()
        with permit:
            assert breaker.half_open_in_flight == 1
        assert permit.released is True
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.HALF_OPEN  # no probe evidence

    def test_plain_release_records_no_probe_evidence(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        permit = breaker.acquire_probe()
        permit.release()
        assert permit.released is True
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.HALF_OPEN

    def test_two_successful_probes_close(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_success=2)
        for _ in range(2):
            permit = breaker.acquire_probe()
            assert permit is not None
            permit.finish(success=True)
        assert breaker.state is CircuitState.CLOSED
        assert breaker.window_samples == 0  # reset on close

    def test_closed_breaker_passthrough_permit_is_not_a_probe(self) -> None:
        breaker = make_breaker()
        permit = breaker.acquire_probe()
        assert permit is not None
        assert permit.is_probe is False
        assert breaker.half_open_in_flight == 0

    async def test_concurrent_admission_bounded_by_budget(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3)
        gate = asyncio.Event()

        async def op() -> PipelineResult:
            await gate.wait()
            return _ok()

        tasks = [asyncio.create_task(logical_call(breaker, op)) for _ in range(5)]
        await asyncio.sleep(0)
        assert breaker.half_open_in_flight == 3
        gate.set()
        results = await asyncio.gather(*tasks)
        admitted = [r for r in results if r is not None]
        rejected = [r for r in results if r is None]
        assert len(admitted) == 3
        assert len(rejected) == 2
        assert breaker.half_open_in_flight == 0

    async def test_retries_consume_one_permit(self) -> None:
        """A probe that retries twice still holds exactly one permit, and its
        final outcome (success) is what counts — attempts never re-acquire."""
        clock = FakeClock()
        breaker = self._half_open(clock)
        backend = ScriptedBackend([_fail(), _fail(), _ok()])
        in_flight_samples: list[int] = []
        outcome = await logical_call(
            breaker, backend, on_attempt=lambda _rec: in_flight_samples.append(
                breaker.half_open_in_flight
            )
        )
        assert outcome is not None
        assert outcome.attempts == 3
        assert backend.calls == 3
        assert in_flight_samples == [1, 1, 1]
        assert breaker.state is CircuitState.HALF_OPEN  # one successful probe
        assert breaker.half_open_in_flight == 0

    async def test_failed_retry_sequence_counts_as_one_failed_probe(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        backend = ScriptedBackend([_fail(), _fail(), _fail()])
        outcome = await logical_call(breaker, backend)
        assert outcome is not None
        assert outcome.attempts == 3
        assert breaker.state is CircuitState.OPEN
        assert breaker.half_open_in_flight == 0

    async def test_cancellation_releases_permit(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3)
        started = asyncio.Event()

        async def op() -> PipelineResult:
            started.set()
            await asyncio.Event().wait()
            return _ok()

        task = asyncio.create_task(logical_call(breaker, op))
        await asyncio.wait_for(started.wait(), timeout=2.0)
        assert breaker.half_open_in_flight == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.HALF_OPEN  # cancellation is not evidence

    async def test_unexpected_exception_releases_permit(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)

        class ExplodingSleeper:
            async def sleep(self, seconds: float) -> None:
                raise RuntimeError("sleeper exploded")

        with pytest.raises(RuntimeError):
            await logical_call(
                breaker, ScriptedBackend([_fail()]), sleeper=ExplodingSleeper()
            )
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.HALF_OPEN  # no leaked evidence

    async def test_closed_retry_sequence_records_exactly_one_window_entry(self) -> None:
        breaker = make_breaker()
        backend = ScriptedBackend([_fail(), _fail(), _ok()])
        outcome = await logical_call(breaker, backend)
        assert outcome is not None and outcome.attempts == 3
        assert breaker.window_samples == 1  # attempts never inflate the window
        assert breaker.window_failures == 0

    async def test_window_count_equals_logical_calls_not_attempts(self) -> None:
        """SPECS §6.2 accounting: N logical calls failing 3 attempts each enter
        the window as N outcomes, never 3N — so retry amplification cannot trip
        a breaker that logical-call math says is healthy."""
        breaker = make_breaker(consecutive_failures=100)
        # 6 logical calls × 3 failed attempts = 18 attempts < min_throughput(10)?
        # No: 18 ≥ 10, so attempt-based accounting WOULD trip on 18/18 = 100%.
        for _ in range(6):
            outcome = await logical_call(
                breaker, ScriptedBackend([_fail(), _fail(), _fail()])
            )
            assert outcome is not None and outcome.attempts == 3
        assert breaker.window_samples == 6
        assert breaker.state is CircuitState.CLOSED  # 6 < min_throughput 10

    async def test_retry_success_hides_attempt_failures_from_window(self) -> None:
        """A logical call that recovers on attempt 3 is a *success* in the
        window even though two attempts failed."""
        breaker = make_breaker(consecutive_failures=100)
        for _ in range(10):
            await logical_call(breaker, ScriptedBackend([_fail(), _fail(), _ok()]))
        assert breaker.window_samples == 10
        assert breaker.window_failures == 0
        assert breaker.failure_ratio == 0.0
        assert breaker.state is CircuitState.CLOSED  # ratio 0.0 never trips

    async def test_open_fail_fast_dispatches_zero_backend_calls(self) -> None:
        """SPECS §6.2/§11: while OPEN the gate rejects before any dispatch, so
        the backend is never called and no window entry is recorded."""
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        samples_before = breaker.window_samples
        backend = ScriptedBackend([_ok()])
        for _ in range(5):
            assert await logical_call(breaker, backend) is None
        assert backend.calls == 0
        assert breaker.window_samples == samples_before
        assert breaker.state is CircuitState.OPEN

    async def test_half_open_over_budget_fail_fast_dispatches_zero_backend_calls(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3)
        gate = asyncio.Event()

        async def op() -> PipelineResult:
            await gate.wait()
            return _ok()

        held = [asyncio.create_task(logical_call(breaker, op)) for _ in range(3)]
        await asyncio.sleep(0)
        assert breaker.half_open_in_flight == 3
        spare = ScriptedBackend([_ok()])
        assert await logical_call(breaker, spare) is None
        assert spare.calls == 0
        gate.set()
        await asyncio.gather(*held)


class TestStaleProbes:
    """M3-T10/T13: results from an earlier breaker generation are inert.

    A probe is tagged with the breaker ``epoch`` at acquisition; settlement
    from any other epoch may free its slot but must never drive a transition,
    touch the probe-success counter, or enter the window.
    """

    def _half_open(self, clock: FakeClock, **overrides) -> CircuitBreaker:
        breaker = make_breaker(clock=clock, **overrides)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        return breaker

    def test_permit_carries_acquisition_epoch(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        epoch = breaker.epoch
        permit = breaker.acquire_probe()
        assert permit is not None
        assert permit.epoch == epoch

    def test_epoch_advances_on_every_transition(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        assert breaker.epoch == 0
        trip_open(breaker, clock)
        assert breaker.epoch == 1
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        assert breaker.epoch == 2
        for _ in range(2):
            permit = breaker.acquire_probe()
            assert permit is not None
            permit.finish(success=True)
        assert breaker.state is CircuitState.CLOSED
        assert breaker.epoch == 3

    def test_stale_probe_success_after_reopen_is_inert(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3, half_open_success=2)
        early = breaker.acquire_probe()
        late = breaker.acquire_probe()
        assert early is not None and late is not None
        late.finish(success=False)  # reopens: epoch advances
        assert breaker.state is CircuitState.OPEN
        clock.advance(5.0)  # fresh HALF_OPEN generation
        assert breaker.state is CircuitState.HALF_OPEN

        early.finish(success=True)  # stale: must not count as a probe success
        fresh = breaker.acquire_probe()
        assert fresh is not None
        fresh.finish(success=True)
        # Only the fresh success counts (1/2) — if the stale one had counted,
        # this would have closed the breaker.
        assert breaker.state is CircuitState.HALF_OPEN

        final = breaker.acquire_probe()
        assert final is not None
        final.finish(success=True)
        assert breaker.state is CircuitState.CLOSED

    def test_stale_probe_failure_after_close_does_not_touch_window(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3, half_open_success=2)
        stale = breaker.acquire_probe()
        assert stale is not None
        for _ in range(2):
            permit = breaker.acquire_probe()
            assert permit is not None
            permit.finish(success=True)
        assert breaker.state is CircuitState.CLOSED
        assert breaker.window_samples == 0  # reset on close

        stale.finish(success=False)  # epoch mismatch: no failure recorded
        assert breaker.state is CircuitState.CLOSED
        assert breaker.window_samples == 0
        assert breaker.consecutive_failures == 0

    def test_stale_result_does_not_double_penalize_after_reopen(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3)
        first = breaker.acquire_probe()
        second = breaker.acquire_probe()
        assert first is not None and second is not None
        first.finish(success=False)  # reopens
        assert breaker.state is CircuitState.OPEN
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        # A second stale failure must not reopen the fresh generation.
        second.finish(success=False)
        assert breaker.state is CircuitState.HALF_OPEN

    def test_stale_settlement_dispatches_no_transition_and_no_window_reset(self) -> None:
        """After a close, a stale probe's late failure must not re-enter the
        window, bump the consecutive counter, restamp transition metadata, or
        emit a transition."""
        clock = FakeClock()
        transitions: list[tuple[CircuitState, CircuitState]] = []
        breaker = make_breaker(
            clock=clock, half_open_probes=3, half_open_success=2,
            on_transition=lambda f, t: transitions.append((f, t)),
        )
        trip_open(breaker, clock)
        clock.advance(5.0)
        stale = breaker.acquire_probe()
        assert stale is not None
        for _ in range(2):
            permit = breaker.acquire_probe()
            assert permit is not None
            permit.finish(success=True)
        assert breaker.state is CircuitState.CLOSED
        history = list(transitions)
        transition_at = breaker.last_transition_at
        epoch = breaker.epoch

        clock.advance(1.0)
        stale.finish(success=False)

        assert transitions == history  # no transition emitted
        assert breaker.last_transition_at == transition_at
        assert breaker.epoch == epoch  # generation untouched
        assert breaker.window_samples == 0
        assert breaker.consecutive_failures == 0

    def test_stale_slot_is_still_freed(self) -> None:
        """Stale evidence is discarded, but the HALF_OPEN budget must not leak:
        a stale permit settles its slot exactly once."""
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=2, half_open_success=2)
        stale = breaker.acquire_probe()
        other = breaker.acquire_probe()
        assert stale is not None and other is not None
        other.finish(success=True)  # 1/2: stays HALF_OPEN, slot freed
        assert breaker.half_open_in_flight == 1
        breaker.reset()
        # Force a new generation by closing then re-opening via a fresh trip.
        fresh = breaker.acquire_probe()
        assert fresh is not None
        fresh.finish(success=True)
        assert breaker.state is CircuitState.CLOSED  # 2nd success closes
        assert breaker.half_open_in_flight == 1  # only `stale` still holds one
        stale.finish(success=True)  # stale: no evidence, slot freed
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.CLOSED
        assert breaker.window_samples == 0

    async def test_stale_probe_outcome_after_reopen_through_retry_sequence(
        self, caplog
    ) -> None:
        """A whole retry sequence acquired in generation N settles after the
        breaker has reopened in N+1: none of its attempts can affect the new
        generation, and the staleness is logged for operators."""
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=3, half_open_success=2)
        gate = asyncio.Event()

        async def slow_ok() -> PipelineResult:
            await gate.wait()
            return _ok()

        slow = asyncio.create_task(logical_call(breaker, slow_ok))
        await asyncio.sleep(0)
        assert breaker.half_open_in_flight == 1

        reopens = breaker.acquire_probe()
        assert reopens is not None
        reopens.finish(success=False)
        assert breaker.state is CircuitState.OPEN
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN

        with caplog.at_level(logging.INFO, logger="app.resilience.circuit_breaker"):
            gate.set()
            outcome = await asyncio.wait_for(slow, timeout=2.0)
        assert outcome is not None and outcome.ok
        assert breaker.state is CircuitState.HALF_OPEN  # stale success: no close
        assert any(
            record.getMessage() == "cb_stale_result" for record in caplog.records
        )


class TestObservability:
    """M3-T11: transitions visible in logs, metrics, and a snapshot."""

    def _metric_values(self, metrics) -> dict[str, float]:
        body = metrics.render()[0].decode()
        return {
            line.split("{")[0]: float(line.rsplit(" ", 1)[1])
            for line in body.splitlines()
            if line and not line.startswith("#")
        }

    def test_snapshot_shape_and_values(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        breaker.record_logical_outcome(success=True)
        breaker.record_logical_outcome(success=False)
        snapshot = breaker.snapshot()
        assert set(snapshot) == {
            "state",
            "window_size",
            "failure_ratio",
            "consecutive_failures",
            "opened_at",
            "last_transition_at",
        }
        assert snapshot["state"] == "closed"
        assert snapshot["window_size"] == 20
        assert snapshot["failure_ratio"] == 0.5
        assert snapshot["consecutive_failures"] == 1
        assert snapshot["opened_at"] is None

    def test_snapshot_reflects_open_with_timestamps(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        snapshot = breaker.snapshot()
        assert snapshot["state"] == "open"
        assert snapshot["opened_at"] == clock.now
        assert snapshot["last_transition_at"] == clock.now

    def test_snapshot_advances_cooldown(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.snapshot()["state"] == "half_open"

    def test_gauge_follows_state(self) -> None:
        metrics = Metrics()
        clock = FakeClock()
        breaker = make_breaker(clock=clock, metrics=metrics)
        assert "circuit_state 0.0" in metrics.render()[0].decode()
        trip_open(breaker, clock)
        assert "circuit_state 1.0" in metrics.render()[0].decode()
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        assert "circuit_state 2.0" in metrics.render()[0].decode()

    def test_transitions_counted_by_pair(self) -> None:
        metrics = Metrics()
        clock = FakeClock()
        breaker = make_breaker(clock=clock, metrics=metrics)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        for _ in range(2):
            permit = breaker.acquire_probe()
            assert permit is not None
            permit.finish(success=True)
        body = metrics.render()[0].decode()
        assert 'circuit_transitions_total{from="closed",to="open"} 1.0' in body
        assert 'circuit_transitions_total{from="open",to="half_open"} 1.0' in body
        assert 'circuit_transitions_total{from="half_open",to="closed"} 1.0' in body
        assert "circuit_state 0.0" in body

    def test_transition_logged_with_json_fields(self, caplog) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        with caplog.at_level(logging.INFO, logger="app.resilience.circuit_breaker"):
            trip_open(breaker, clock)
        records = [r for r in caplog.records if r.getMessage() == "cb_transition"]
        assert records, "every transition must emit cb_transition"
        fields = records[-1].json_fields
        assert (fields["from"], fields["to"]) == ("closed", "open")

    def test_stale_result_logged_as_metric_only(self, caplog) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock, half_open_probes=3, half_open_success=2)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        stale = breaker.acquire_probe()
        assert stale is not None
        for _ in range(2):
            permit = breaker.acquire_probe()
            assert permit is not None
            permit.finish(success=True)
        assert breaker.state is CircuitState.CLOSED
        with caplog.at_level(logging.INFO, logger="app.resilience.circuit_breaker"):
            stale.finish(success=False)
        records = [r for r in caplog.records if r.getMessage() == "cb_stale_result"]
        assert len(records) == 1
        assert records[0].json_fields["success"] is False
        assert breaker.state is CircuitState.CLOSED


class TestTransitionHook:
    def test_transitions_reported_to_hook(self) -> None:
        seen: list[tuple[CircuitState, CircuitState]] = []
        breaker = make_breaker(
            on_transition=lambda old, new: seen.append((old, new)),
        )
        for _ in range(5):
            breaker.record_logical_outcome(success=False)
        assert seen == [(CircuitState.CLOSED, CircuitState.OPEN)]

    def test_last_transition_timestamp_updates(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        assert breaker.last_transition_at is None
        trip_open(breaker, clock)
        assert breaker.last_transition_at == clock.now
        assert breaker.opened_at == clock.now

    def test_reset_clears_window_and_counter(self) -> None:
        breaker = make_breaker()
        for _ in range(4):
            breaker.record_logical_outcome(success=False)
        breaker.reset()
        assert breaker.window_samples == 0
        assert breaker.consecutive_failures == 0
