"""Circuit breaker (M3-T07 state machine, M3-T08 window, M3-T09 permits).

Scope of this module: the CLOSED/OPEN/HALF_OPEN state machine, the sliding
window of **completed logical calls**, the trip thresholds, and explicit
HALF_OPEN probe permits.

Counting unit (SPECS §6.2, normative): **completed logical pipeline calls,
not retry attempts.** :meth:`CircuitBreaker.record_logical_outcome` is called
exactly once per logical call, after its retry sequence finishes. Attempt
failures never enter the window — the retry executor reports them only via
attempt metrics/logs.

HALF_OPEN probes (M3-T09): :meth:`CircuitBreaker.acquire_probe` reserves one
permit **per logical call** before the retry sequence starts. The permit is
held across every attempt and settles exactly once — :meth:`ProbePermit.finish`
with the final outcome, or :meth:`ProbePermit.release` (also via ``with
permit``) when the call ends without one (cancellation, unexpected
exception). Settlement frees the slot exactly once, so permits cannot leak and
a cancelled probe records no evidence. Permits are the single admission
currency, and each is tagged with the breaker ``epoch`` at acquisition so a
result from an earlier generation is inert (SPECS §6.2).

Concurrency: every method here is synchronous (no ``await`` in a critical
section), so on a single asyncio event loop each call is atomic and no lock
is needed. The breaker is deliberately per-process (PLAN D5).
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from enum import Enum
from typing import Self

from app.config import Settings
from app.metrics import Metrics

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Breaker states (SPECS §6.2)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ``circuit_state`` gauge encoding (SPECS §9, M3-T11).
_GAUGE_VALUES = {CircuitState.CLOSED: 0, CircuitState.OPEN: 1, CircuitState.HALF_OPEN: 2}


class ProbePermit:
    """Admission for ONE complete logical call (SPECS §6.2, M3-T09).

    Acquired before the retry sequence starts and held across every attempt,
    so the HALF_OPEN probe budget counts logical calls rather than attempts.
    Exactly one of the following settles it, and only the first one counts:

    - :meth:`finish` — the logical call reached a terminal outcome; the
      permit's probe evidence (success/failure of the **whole** sequence)
      is applied to the breaker.
    - :meth:`release` / ``with permit`` — the call ended without an outcome
      (cancellation, unexpected exception); the slot is freed and **no**
      evidence is recorded, so a cancelled probe can never close or reopen
      the breaker.

    M3-T10 tags each permit with the breaker ``epoch`` at acquisition. A
    result from an earlier generation frees its slot but is otherwise inert:
    it can never transition the breaker, touch the probe-success counter, or
    enter the window (SPECS §6.2 "stale results are inert").
    """
    __slots__ = ("_breaker", "_epoch", "_finished", "_is_probe", "_released")

    def __init__(self, breaker: CircuitBreaker, *, is_probe: bool, epoch: int) -> None:
        self._breaker = breaker
        self._is_probe = is_probe
        self._epoch = epoch
        self._released = False
        self._finished = False

    @property
    def epoch(self) -> int:
        """Breaker generation this permit was acquired in (M3-T10)."""
        return self._epoch

    @property
    def is_probe(self) -> bool:
        """True only for a permit acquired in HALF_OPEN (budget-consuming)."""
        return self._is_probe

    @property
    def released(self) -> bool:
        return self._released

    def finish(self, *, success: bool) -> None:
        """Settle with the final logical-call outcome (idempotent)."""
        if self._finished:
            return
        self._finished = True
        self._released = True
        self._breaker._settle_permit(self, success=success)

    def release(self) -> None:
        """Free the slot without recording evidence (idempotent)."""
        if self._released:
            return
        self._released = True
        self._finished = True
        self._breaker._settle_permit(self, success=None)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False


class CircuitBreaker:
    """Per-process circuit breaker over completed logical-call outcomes.

    Trip rules (SPECS §6.2):
    - ratio: failure ratio in the sliding window ≥ ``failure_threshold``
      **and** the window holds ≥ ``min_throughput`` outcomes;
    - consecutive: ``consecutive_failures`` logical failures in a row.

    Recovery: after ``cooldown_s`` in OPEN the breaker admits up to
    ``half_open_probes`` concurrent probes; ``half_open_success``
    successful probes close it (window and consecutive counter reset); any
    failed probe reopens it (cooldown restarts). The dispatch path reserves a
    probe with :meth:`acquire_probe` (a permit for one whole logical call,
    not one attempt).

    A rejection while OPEN/HALF_OPEN-over-budget is **not** an outcome: the
    caller must not record one, so rejected calls can never pollute breaker
    statistics (SPECS §6.2).
    """

    def __init__(
        self,
        window_size: int = 20,
        failure_threshold: float = 0.5,
        min_throughput: int = 10,
        consecutive_failures: int = 5,
        cooldown_s: float = 5.0,
        half_open_probes: int = 3,
        half_open_success: int = 2,
        *,
        clock: Callable[[], float] = time.monotonic,
        on_transition: Callable[[CircuitState, CircuitState], None] | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        if not 0 < failure_threshold <= 1:
            raise ValueError("failure_threshold must be in (0, 1]")
        if min_throughput < 1:
            raise ValueError("min_throughput must be >= 1")
        if consecutive_failures < 1:
            raise ValueError("consecutive_failures must be >= 1")
        if cooldown_s < 0:
            raise ValueError("cooldown_s must be >= 0")
        if half_open_probes < 1 or half_open_success < 1:
            raise ValueError("half-open probe/success counts must be >= 1")

        self._window_size = window_size
        self._failure_threshold = failure_threshold
        self._min_throughput = min_throughput
        self._consecutive_failures_threshold = consecutive_failures
        self._cooldown_s = cooldown_s
        self._half_open_probes = half_open_probes
        self._half_open_success_threshold = half_open_success
        self._clock = clock
        self._on_transition = on_transition
        self._metrics = metrics

        self._state = CircuitState.CLOSED
        self._window: deque[bool] = deque(maxlen=window_size)  # True = failure
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._last_transition_at: float | None = None
        self._half_open_in_flight = 0
        self._half_open_successes = 0
        self._epoch = 0
        self._publish_state()

    @classmethod
    def from_settings(
        cls, settings: Settings, *, clock: Callable[[], float] = time.monotonic,
        on_transition: Callable[[CircuitState, CircuitState], None] | None = None,
        metrics: Metrics | None = None,
    ) -> CircuitBreaker:
        """Build from the SPECS §8 knobs."""
        return cls(
            window_size=settings.cb_window_size,
            failure_threshold=settings.cb_failure_threshold,
            min_throughput=settings.cb_min_throughput,
            consecutive_failures=settings.cb_consecutive_failures,
            cooldown_s=settings.cb_cooldown_s,
            half_open_probes=settings.cb_half_open_probes,
            half_open_success=settings.cb_half_open_success,
            clock=clock,
            on_transition=on_transition,
            metrics=metrics,
        )

    # --- read-only state ---------------------------------------------------

    @property
    def state(self) -> CircuitState:
        self._maybe_advance_from_open()
        return self._state

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def window_failures(self) -> int:
        return sum(1 for failed in self._window if failed)

    @property
    def window_samples(self) -> int:
        return len(self._window)

    @property
    def failure_ratio(self) -> float:
        """Failure ratio over completed logical calls in the window (0.0 empty)."""
        if not self._window:
            return 0.0
        return self.window_failures / len(self._window)

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def opened_at(self) -> float | None:
        return self._opened_at

    @property
    def last_transition_at(self) -> float | None:
        return self._last_transition_at

    @property
    def half_open_in_flight(self) -> int:
        """Probe permits currently held in HALF_OPEN (SPECS §6.2 budget)."""
        return self._half_open_in_flight

    @property
    def epoch(self) -> int:
        """Monotonic breaker generation counter (M3-T10).

        Incremented on every state transition. Permits capture this value at
        acquisition, and a settlement whose epoch no longer matches frees its
        slot only — it never drives a transition, a probe-success count, or a
        window entry (SPECS §6.2 "stale results are inert").
        """
        return self._epoch

    def snapshot(self) -> dict[str, object]:
        """Live breaker state for ``GET /admin/circuit-breaker`` (M3-T11).

        Reporting a snapshot must not mutate the breaker beyond the normal
        cooldown check: reading after the cooldown has elapsed reports
        ``half_open`` exactly as :attr:`state` would.
        """
        self._maybe_advance_from_open()
        return {
            "state": self._state.value,
            "window_size": self._window_size,
            "failure_ratio": self.failure_ratio,
            "consecutive_failures": self._consecutive_failures,
            "opened_at": self._opened_at,
            "last_transition_at": self._last_transition_at,
        }

    # --- gate --------------------------------------------------------------

    def allow_request(self) -> bool:
        """May a new logical call be dispatched right now? (Gate only.)

        CLOSED → always. OPEN → only once the cooldown has elapsed (this
        call transitions to HALF_OPEN and admits a probe). HALF_OPEN → while
        the concurrent-probe budget is not exhausted.

        Prefer :meth:`acquire_probe` on the dispatch path: this predicate
        checks the gate without reserving anything, so it cannot by itself
        enforce the probe budget.
        """
        self._maybe_advance_from_open()
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            return False
        # HALF_OPEN: admit up to the probe budget.
        if self._half_open_in_flight < self._half_open_probes:
            self._half_open_in_flight += 1
            return True
        return False

    def acquire_probe(self) -> ProbePermit | None:
        """Reserve one logical call's admission and return its permit.

        **One permit = one complete logical call, not one attempt** (SPECS
        §6.2): acquire before dispatching the retry sequence, hold it across
        every attempt, and release it exactly once when the logical call
        finalizes. The permit is released by
        :meth:`ProbePermit.finish`/:meth:`ProbePermit.release` or by exiting
        ``with permit`` — so cancellation and unexpected exceptions cannot
        leak the slot. A double release is a no-op.

        - CLOSED → a passthrough permit (``is_probe is False``): admission
          is unconditional and the probe counter is untouched.
        - HALF_OPEN → a probe permit while the budget allows, else ``None``
          (the caller fails fast / defers exactly as in OPEN).
        - OPEN → ``None`` (cooldown not elapsed; the caller fails fast).
        """
        self._maybe_advance_from_open()
        if self._state is CircuitState.CLOSED:
            return ProbePermit(self, is_probe=False, epoch=self._epoch)
        if self._state is CircuitState.OPEN:
            return None
        if self._half_open_in_flight >= self._half_open_probes:
            return None
        self._half_open_in_flight += 1
        return ProbePermit(self, is_probe=True, epoch=self._epoch)

    # --- outcomes ----------------------------------------------------------

    def record_logical_outcome(self, success: bool) -> None:
        """Record the final outcome of ONE completed logical call.

        Must be called exactly once per dispatched logical call, after its
        retry sequence finishes — never per attempt, and never for a call
        that was rejected by :meth:`allow_request`.

        This is the direct/low-level entry point (it also frees one HALF_OPEN
        admission slot, matching the :meth:`allow_request` accounting). The
        dispatch path uses :meth:`acquire_probe` + :meth:`ProbePermit.finish`
        instead, which keeps the permit and the slot lifecycle in one place.
        """
        if self._state is CircuitState.HALF_OPEN:
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
        self._apply_outcome(success)

    def reset(self) -> None:
        """Clear the window and consecutive counter (e.g. after a close)."""
        self._window.clear()
        self._consecutive_failures = 0

    # --- internals ---------------------------------------------------------

    def _record_closed_outcome(self, success: bool) -> None:
        self._window.append(not success)
        self._consecutive_failures = 0 if success else self._consecutive_failures + 1
        if self._should_trip():
            self._transition(CircuitState.OPEN)
            self._opened_at = self._clock()

    def _should_trip(self) -> bool:
        if self._consecutive_failures >= self._consecutive_failures_threshold:
            return True
        return (
            len(self._window) >= self._min_throughput
            and self.failure_ratio >= self._failure_threshold
        )

    def _apply_outcome(self, success: bool) -> None:
        """Route one final logical-call outcome to the state-appropriate rule.

        Shared by :meth:`record_logical_outcome` (direct calls) and
        :meth:`_settle_permit` (permit-driven calls), so both paths count
        **one outcome per logical call** and agree on HALF_OPEN evaluation.
        """
        if self._state is CircuitState.HALF_OPEN:
            self._record_half_open_outcome(success)
            return
        if self._state is CircuitState.OPEN:
            # Outcomes from calls dispatched before the breaker opened are
            # stale (M3-T10 epoch guard) — they are never fresh evidence.
            return
        self._record_closed_outcome(success)

    def _record_half_open_outcome(self, success: bool) -> None:
        if not success:
            self._transition(CircuitState.OPEN)
            self._opened_at = self._clock()
            self._half_open_successes = 0
            return
        self._half_open_successes += 1
        if self._half_open_successes >= self._half_open_success_threshold:
            self._transition(CircuitState.CLOSED)
            # Stale outage failures must not immediately re-trip a
            # just-recovered breaker (SPECS §6.2).
            self.reset()
            self._half_open_successes = 0

    def _settle_permit(self, permit: ProbePermit, *, success: bool | None) -> None:
        """Apply a permit's settlement exactly once (called by the permit).

        ``success is None`` means "released without evidence": free the slot,
        never touch the probe-success counter or the window. Only HALF_OPEN
        permits consume a slot; a CLOSED passthrough permit has none.

        A permit acquired in an earlier breaker generation is **stale**: its
        slot is still freed (so the budget cannot leak) but its evidence is
        discarded — a late success must not close a breaker that has since
        reopened, and a late failure must not penalise the fresh generation.
        """
        if permit.is_probe:
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
        if success is None:
            return
        if permit.epoch != self._epoch:
            logger.info(
                "cb_stale_result",
                extra={
                    "json_fields": {
                        "permit_epoch": permit.epoch,
                        "current_epoch": self._epoch,
                        "success": success,
                    }
                },
            )
            return
        self._apply_outcome(success)

    def _maybe_advance_from_open(self) -> None:
        if self._state is not CircuitState.OPEN:
            return
        if self._opened_at is None:
            return
        if self._clock() - self._opened_at >= self._cooldown_s:
            self._transition(CircuitState.HALF_OPEN)
            self._half_open_in_flight = 0
            self._half_open_successes = 0

    def _transition(self, to_state: CircuitState) -> None:
        from_state = self._state
        if from_state is to_state:
            return
        self._state = to_state
        self._epoch += 1
        now = self._clock()
        self._last_transition_at = now
        if to_state is CircuitState.OPEN:
            self._opened_at = now
        logger.info(
            "cb_transition",
            extra={"json_fields": {"from": from_state.value, "to": to_state.value}},
        )
        self._publish_state()
        if self._metrics is not None:
            self._metrics.circuit_transitions_total.labels(
                **{"from": from_state.value, "to": to_state.value}
            ).inc()
        if self._on_transition is not None:
            self._on_transition(from_state, to_state)

    def _publish_state(self) -> None:
        """Expose the current state as the ``circuit_state`` gauge (M3-T11)."""
        if self._metrics is not None:
            self._metrics.circuit_state.set(_GAUGE_VALUES[self._state])
