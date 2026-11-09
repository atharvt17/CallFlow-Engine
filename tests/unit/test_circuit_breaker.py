"""Circuit-breaker unit tests (M3-T07, M3-T08; SPECS §6.2, §11).

Deterministic: a fake clock drives cooldown, and outcomes are recorded
directly — no sleeping, no backend. The accounting unit under test is the
**completed logical call**, so every ``record_logical_outcome`` here
represents one whole retry sequence.
"""

from __future__ import annotations

import pytest

from app.config import Settings
from app.resilience.circuit_breaker import CircuitBreaker, CircuitState


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
