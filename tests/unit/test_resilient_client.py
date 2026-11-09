"""``ResilientPipelineClient`` unit tests (M3-T14; SPECS §6, §6.2).

The decorator composes per-attempt timeout + backoff retry + breaker
admission behind the unchanged :class:`PipelineBackend` boundary:

- one breaker **outcome** per logical call (never per attempt);
- one HALF_OPEN **permit** per logical call, held across the retry sequence;
- breaker state is re-checked **before every attempt dispatch**, so a call
  whose next retry would land in an OPEN breaker stops early and reports a
  *circuit-open* result instead of hammering a dead dependency;
- retry/sleeper/clock/RNG seams stay injectable, so nothing here sleeps.
"""

from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.metrics import Metrics
from app.pipeline.base import PipelineResult
from app.pipeline.resilient import ResilientPipelineClient
from app.resilience.circuit_breaker import CircuitBreaker, CircuitState
from app.resilience.retry import RetryPolicy


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleeper:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def _ok() -> PipelineResult:
    return PipelineResult(ok=True, http_status=200, latency_ms=300.0)


def _fail() -> PipelineResult:
    return PipelineResult(ok=False, reason="downstream_timeout", http_status=503, latency_ms=250.0)


class ScriptedBackend:
    """One attempt per ``process``; scripted results, call counter."""

    def __init__(self, results: list[PipelineResult]) -> None:
        self._results = list(results)
        self.calls = 0

    async def aclose(self) -> None:
        pass

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        self.calls += 1
        return self._results.pop(0) if self._results else _fail()


def make_breaker(clock: FakeClock | None = None, **overrides) -> CircuitBreaker:
    params = {
        "window_size": 20,
        "failure_threshold": 0.5,
        "min_throughput": 10,
        "consecutive_failures": 5,
        "cooldown_s": 5.0,
        "half_open_probes": 3,
        "half_open_success": 2,
        "clock": clock or FakeClock(),
    }
    params.update(overrides)
    return CircuitBreaker(**params)


def make_client(backend, breaker, **kwargs) -> ResilientPipelineClient:
    kwargs.setdefault("policy", RetryPolicy(backoff_jitter=0.0))
    kwargs.setdefault("sleeper", FakeSleeper())
    return ResilientPipelineClient(backend, breaker, **kwargs)


def trip_open(breaker: CircuitBreaker, clock: FakeClock) -> None:
    for _ in range(5):
        breaker.record_logical_outcome(success=False)
    assert breaker.state is CircuitState.OPEN


class TestClosedPath:
    async def test_success_on_first_attempt(self) -> None:
        backend = ScriptedBackend([_ok()])
        breaker = make_breaker()
        client = make_client(backend, breaker)
        outcome = await client.execute("evt-1", "audio")
        assert outcome.ok and outcome.attempts == 1
        assert outcome.deferred is False
        assert backend.calls == 1
        assert breaker.window_samples == 1

    async def test_retries_then_succeeds_records_one_winning_outcome(self) -> None:
        backend = ScriptedBackend([_fail(), _fail(), _ok()])
        breaker = make_breaker()
        sleeper = FakeSleeper()
        client = make_client(backend, breaker, sleeper=sleeper)
        outcome = await client.execute("evt-1", "audio")
        assert outcome.ok and outcome.attempts == 3 and outcome.retry_count == 2
        assert sleeper.sleeps == [0.1, 0.2]
        assert breaker.window_samples == 1  # attempts never inflate the window
        assert breaker.window_failures == 0

    async def test_exhausted_retries_records_one_failing_outcome(self) -> None:
        backend = ScriptedBackend([_fail(), _fail(), _fail()])
        breaker = make_breaker(consecutive_failures=100)
        client = make_client(backend, breaker)
        outcome = await client.execute("evt-1", "audio")
        assert not outcome.ok and outcome.attempts == 3
        assert outcome.deferred is False
        assert breaker.window_samples == 1
        assert breaker.window_failures == 1

    async def test_non_retryable_stops_after_one_attempt(self) -> None:
        backend = ScriptedBackend(
            [PipelineResult(ok=False, reason="non_retryable_error", http_status=400)]
        )
        breaker = make_breaker()
        client = make_client(backend, breaker)
        outcome = await client.execute("evt-1", "audio")
        assert outcome.attempts == 1 and backend.calls == 1
        assert breaker.window_failures == 1


class TestOpenFailFast:
    async def test_open_breaker_rejects_without_dispatch(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        backend = ScriptedBackend([_ok()])
        client = make_client(backend, breaker)
        outcome = await client.execute("evt-1", "audio")
        assert outcome.deferred is True
        assert outcome.reason == "circuit_open"
        assert outcome.attempts == 0
        assert backend.calls == 0  # zero downstream attempts
        assert breaker.window_samples == 5  # the trip evidence, nothing more

    async def test_rejection_is_not_a_failure_outcome(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock)
        trip_open(breaker, clock)
        client = make_client(ScriptedBackend([_ok()]), breaker)
        for _ in range(10):
            await client.execute("evt", "audio")
        # Open rejections never enter the window or advance the epoch.
        assert breaker.window_samples == 5
        assert breaker.epoch == 1
        assert breaker.state is CircuitState.OPEN


class TestMidRetryOpen:
    async def test_stops_when_breaker_opens_between_attempts(self) -> None:
        """SPECS §6.2: an in-flight call whose next retry would dispatch into
        an OPEN breaker is finalized as deferred — it must not attempt again."""
        clock = FakeClock()
        breaker = make_breaker(clock=clock, consecutive_failures=5)
        backend = ScriptedBackend([_fail(), _ok(), _ok()])
        client = make_client(backend, breaker, sleeper=None)

        class TripDuringSleep:
            """Simulates other traffic tripping the breaker mid-retry."""

            def __init__(self) -> None:
                self.calls = 0

            async def sleep(self, seconds: float) -> None:
                self.calls += 1
                trip_open(breaker, clock)

        client = make_client(backend, breaker, sleeper=TripDuringSleep())
        outcome = await client.execute("evt-1", "audio")
        assert outcome.deferred is True
        assert outcome.reason == "circuit_open"
        assert outcome.attempts == 1  # the second attempt never dispatched
        assert backend.calls == 1

    async def test_mid_retry_defer_does_not_enter_window(self) -> None:
        clock = FakeClock()
        breaker = make_breaker(clock=clock, consecutive_failures=5)
        backend = ScriptedBackend([_fail(), _fail(), _fail()])
        samples_before = 0

        class TripDuringSleep:
            async def sleep(self, seconds: float) -> None:
                trip_open(breaker, clock)

        client = make_client(backend, breaker, sleeper=TripDuringSleep())
        samples_before = breaker.window_samples
        outcome = await client.execute("evt-1", "audio")
        assert outcome.deferred is True
        # The trip itself added its 5 outcomes; the deferred call added none.
        assert breaker.window_samples == samples_before + 5

    async def test_first_attempt_allowed_then_mid_retry_open(self) -> None:
        """Ordering proof: the gate is checked before attempt 1 (allowed while
        CLOSED), then again before attempt 2 (rejected once OPEN)."""
        clock = FakeClock()
        breaker = make_breaker(clock=clock, consecutive_failures=5)
        backend = ScriptedBackend([_fail(), _ok()])
        dispatch_states: list[str] = []

        class StateRecordingBackend:
            async def aclose(self) -> None:
                pass

            async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
                dispatch_states.append(breaker.state.value)
                return await backend.process(event_id, audio_ref)

        class TripDuringSleep:
            async def sleep(self, seconds: float) -> None:
                trip_open(breaker, clock)

        client = make_client(
            StateRecordingBackend(), breaker, sleeper=TripDuringSleep()
        )
        outcome = await client.execute("evt-1", "audio")
        assert dispatch_states == ["closed"]  # attempt 2 never reached the backend
        assert outcome.deferred is True


class TestHalfOpenProbes:
    def _half_open(self, clock: FakeClock, **overrides) -> CircuitBreaker:
        breaker = make_breaker(clock=clock, **overrides)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        return breaker

    async def test_probe_permit_held_across_retry_sequence(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        backend = ScriptedBackend([_fail(), _fail(), _ok()])
        in_flight: list[int] = []

        class SamplingBackend:
            async def aclose(self) -> None:
                pass

            async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
                in_flight.append(breaker.half_open_in_flight)
                return await backend.process(event_id, audio_ref)

        client = make_client(SamplingBackend(), breaker)
        outcome = await client.execute("evt-1", "audio")
        assert outcome.ok and outcome.attempts == 3
        assert in_flight == [1, 1, 1]
        assert breaker.state is CircuitState.HALF_OPEN  # one successful probe

    async def test_two_successful_probes_close(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_success=2)
        client = make_client(ScriptedBackend([_ok(), _ok()]), breaker)
        await client.execute("evt-1", "audio")
        assert breaker.state is CircuitState.HALF_OPEN
        await client.execute("evt-2", "audio")
        assert breaker.state is CircuitState.CLOSED
        assert breaker.window_samples == 0

    async def test_failed_probe_reopens(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock)
        client = make_client(ScriptedBackend([_fail(), _fail(), _fail()]), breaker)
        outcome = await client.execute("evt-1", "audio")
        assert not outcome.ok and outcome.attempts == 3
        assert breaker.state is CircuitState.OPEN
        assert breaker.opened_at == clock.now

    async def test_over_budget_probe_defers(self) -> None:
        clock = FakeClock()
        breaker = self._half_open(clock, half_open_probes=1)
        gate = asyncio.Event()

        class SlowBackend:
            def __init__(self) -> None:
                self.calls = 0

            async def aclose(self) -> None:
                pass

            async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
                self.calls += 1
                await gate.wait()
                return _ok()

        slow = SlowBackend()
        client = make_client(
            slow, breaker, policy=RetryPolicy(max_attempts=1, backoff_jitter=0.0)
        )
        held = asyncio.create_task(client.execute("evt-1", "audio"))
        await asyncio.sleep(0)
        assert breaker.half_open_in_flight == 1
        rejected = await client.execute("evt-2", "audio")
        assert rejected.deferred is True
        assert slow.calls == 1
        gate.set()
        assert (await held).ok


class TestCancellationAndLeaks:
    async def test_cancellation_releases_probe_permit(self) -> None:
        clock = FakeClock()
        breaker = self._half_open_paid(clock)
        started = asyncio.Event()

        class HangingBackend:
            async def aclose(self) -> None:
                pass

            async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
                started.set()
                await asyncio.Event().wait()
                return _ok()

        client = make_client(HangingBackend(), breaker)
        task = asyncio.create_task(client.execute("evt-1", "audio"))
        await asyncio.wait_for(started.wait(), timeout=2.0)
        assert breaker.half_open_in_flight == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)
        assert breaker.half_open_in_flight == 0
        assert breaker.state is CircuitState.HALF_OPEN

    def _half_open_paid(self, clock: FakeClock) -> CircuitBreaker:
        breaker = make_breaker(clock=clock, half_open_probes=3)
        trip_open(breaker, clock)
        clock.advance(5.0)
        assert breaker.state is CircuitState.HALF_OPEN
        return breaker

    async def test_records_metrics_attempts_and_retries(self) -> None:
        metrics = Metrics()
        breaker = make_breaker()
        client = make_client(
            ScriptedBackend([_fail(), _ok()]), breaker, metrics=metrics
        )
        await client.execute("evt-1", "audio")
        body = metrics.render()[0].decode()
        assert 'pipeline_attempts_total{result="error"} 1.0' in body
        assert 'pipeline_attempts_total{result="success"} 1.0' in body
        assert "retries_total 1.0" in body


class TestFromSettings:
    def test_builds_policy_and_injection_seams(self) -> None:
        settings = Settings()
        clock = FakeClock()
        client = ResilientPipelineClient.from_settings(
            ScriptedBackend([_ok()]), make_breaker(clock=clock), settings
        )
        assert client.policy == RetryPolicy.from_settings(settings)

    async def test_swap_safe_for_inline_backend(self) -> None:
        from app.pipeline.inline_client import InlinePipelineClient

        breaker = make_breaker()
        client = make_client(InlinePipelineClient(), breaker)
        outcome = await client.execute("evt-1", "audio")
        assert outcome.ok and outcome.attempts == 1
        assert breaker.window_samples == 1

    async def test_aclose_delegates_to_inner_backend(self) -> None:
        closed = {"n": 0}

        class ClosableBackend:
            async def aclose(self) -> None:
                closed["n"] += 1

            async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
                return _ok()

        client = make_client(ClosableBackend(), make_breaker())
        await client.aclose()
        assert closed["n"] == 1
