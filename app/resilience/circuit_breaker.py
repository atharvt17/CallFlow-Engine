"""Circuit breaker (M3-T07 state machine, M3-T08 logical-call window).

Scope of this module at M3-T08: the CLOSED/OPEN/HALF_OPEN state machine,
the sliding window of **completed logical calls**, and the trip thresholds.
Probe permits are admitted here in their basic form (bounded concurrent
probes); M3-T09 refines them into explicit permits held across a retry
sequence, and M3-T10 adds the open-epoch/stale-result guard.

Counting unit (SPECS §6.2, normative): **completed logical pipeline calls,
not retry attempts.** :meth:`CircuitBreaker.record_logical_outcome` is called
exactly once per logical call, after its retry sequence finishes. Attempt
failures never enter the window — the retry executor reports them only via
attempt metrics/logs.

Concurrency: every method is synchronous (no ``await`` in a critical
section), so on a single asyncio event loop each call is atomic and no lock
is needed. The breaker is deliberately per-process (PLAN D5).
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from enum import Enum

from app.config import Settings

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Breaker states (SPECS §6.2)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Per-process circuit breaker over completed logical-call outcomes.

    Trip rules (SPECS §6.2):
    - ratio: failure ratio in the sliding window ≥ ``failure_threshold``
      **and** the window holds ≥ ``min_throughput`` outcomes;
    - consecutive: ``consecutive_failures`` logical failures in a row.

    Recovery: after ``cooldown_s`` in OPEN the breaker admits up to
    ``half_open_probes`` concurrent probe calls; ``half_open_success``
    successful probes close it (window and consecutive counter reset); any
    failed probe reopens it (cooldown restarts).

    A rejection while OPEN/HALF_OPEN-over-budget is **not** an outcome: the
    caller must not call :meth:`record_logical_outcome` for it, so rejected
    calls can never pollute breaker statistics (SPECS §6.2).
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

        self._state = CircuitState.CLOSED
        self._window: deque[bool] = deque(maxlen=window_size)  # True = failure
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._last_transition_at: float | None = None
        self._half_open_in_flight = 0
        self._half_open_successes = 0

    @classmethod
    def from_settings(
        cls, settings: Settings, *, clock: Callable[[], float] = time.monotonic,
        on_transition: Callable[[CircuitState, CircuitState], None] | None = None,
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

    # --- gate --------------------------------------------------------------

    def allow_request(self) -> bool:
        """May a new logical call be dispatched right now?

        CLOSED → always. OPEN → only once the cooldown has elapsed (this
        call transitions to HALF_OPEN and admits a probe). HALF_OPEN → while
        the concurrent-probe budget is not exhausted.
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

    # --- outcomes ----------------------------------------------------------

    def record_logical_outcome(self, success: bool) -> None:
        """Record the final outcome of ONE completed logical call.

        Must be called exactly once per dispatched logical call, after its
        retry sequence finishes — never per attempt, and never for a call
        that was rejected by :meth:`allow_request`.
        """
        if self._state is CircuitState.HALF_OPEN:
            self._record_half_open_outcome(success)
            return
        if self._state is CircuitState.OPEN:
            # Late results from calls dispatched before the breaker opened:
            # they are evidence from a dead period, not fresh probe data.
            # (M3-T10 formalises this with epochs.) Inert by design.
            return
        self._record_closed_outcome(success)

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

    def _record_half_open_outcome(self, success: bool) -> None:
        self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
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
        now = self._clock()
        self._last_transition_at = now
        if to_state is CircuitState.OPEN:
            self._opened_at = now
        logger.info(
            "cb_transition",
            extra={"json_fields": {"from": from_state.value, "to": to_state.value}},
        )
        if self._on_transition is not None:
            self._on_transition(from_state, to_state)
