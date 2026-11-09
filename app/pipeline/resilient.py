"""Resilience decorator over the pipeline boundary (M3-T14; SPECS §6).

:class:`ResilientPipelineClient` wraps *any* :class:`PipelineBackend` — HTTP or
inline, unchanged — with the Sprint 3 resilience policy. It is the only place
where retry, per-attempt timeout, and circuit-breaker admission meet; concrete
backends stay resilience-free (PLAN D1 scope row 3).

Per **logical call** (one event through its whole retry sequence):

1. the breaker gate is consulted — ``acquire_probe()`` reserves one HALF_OPEN
   permit (or a CLOSED passthrough), and a refusal returns a *deferred* outcome
   with **zero** downstream attempts and **no** breaker evidence;
2. the breaker state is re-checked **before every attempt dispatch**
   (SPECS §6.2): if the breaker went OPEN mid-retry the sequence stops and the
   call becomes deferred — not dead-lettered, not counted in the window;
3. the permit — acquired **before** the sequence — is settled exactly once with
   the sequence's final outcome (``with permit`` frees it on cancellation or
   an unexpected exception without recording evidence).

Breaker accounting stays logical-call based: one final outcome per call, never
one per attempt. Attempt metrics remain the retry executor's job.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable

from app.config import Settings
from app.metrics import Metrics
from app.pipeline.base import PipelineBackend, PipelineResult
from app.resilience.circuit_breaker import CircuitBreaker, ProbePermit
from app.resilience.retry import (
    AttemptRecord,
    RetryOutcome,
    RetryPolicy,
    Sleeper,
    execute_with_retry,
)

logger = logging.getLogger(__name__)

# Reason carried by a deferred (breaker-rejected) outcome; SPECS §5's 202 body
# uses the same vocabulary.
CIRCUIT_OPEN = "circuit_open"


class _CircuitOpenError(BaseException):
    """Internal signal: the breaker refused this attempt's dispatch.

    Deliberately a ``BaseException``, like cancellation: the retry executor
    maps any ``Exception`` from an attempt to a ``connection_error`` attempt
    (SPECS §6.1), but a dispatch the breaker refused never happened — it must
    not appear as an attempt, a metric, or breaker evidence. It is raised from
    inside ``op`` before the backend call and caught immediately around the
    executor, so it can never escape this module.
    """


def _open_result() -> PipelineResult:
    return PipelineResult(ok=False, reason=CIRCUIT_OPEN)


class ResilientPipelineClient:
    """``PipelineBackend`` decorator: entry/attempt gating + retry + breaker."""

    def __init__(
        self,
        backend: PipelineBackend,
        breaker: CircuitBreaker,
        *,
        policy: RetryPolicy,
        sleeper: Sleeper | None = None,
        clock: Callable[[], float] | None = None,
        rng: random.Random | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._backend = backend
        self._breaker = breaker
        self._policy = policy
        self._sleeper = sleeper
        self._clock = clock
        self._rng = rng
        self._metrics = metrics

    @classmethod
    def from_settings(
        cls,
        backend: PipelineBackend,
        breaker: CircuitBreaker,
        settings: Settings,
        *,
        metrics: Metrics | None = None,
    ) -> ResilientPipelineClient:
        """Production wiring: retry policy straight from the SPECS §8 knobs.

        ``metrics`` enables the executor's attempt-level series
        (``pipeline_attempts_total``, ``pipeline_attempt_latency_ms``,
        ``retries_total``) — they stay attempt-scoped and never reach the
        breaker window (SPECS §6.2).
        """
        return cls(
            backend,
            breaker,
            policy=RetryPolicy.from_settings(settings),
            metrics=metrics,
        )

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    async def aclose(self) -> None:
        """Uniform lifecycle: delegate to the decorated backend."""
        await self._backend.aclose()

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        """``PipelineBackend`` entry point — see :meth:`execute`."""
        outcome = await self.execute(event_id, audio_ref)
        return outcome.result

    async def execute(self, event_id: str, audio_ref: str) -> RetryOutcome:
        """Run one complete logical call and return its full outcome.

        Unlike :meth:`process`, this exposes attempts/records and the
        ``deferred`` signal the service needs for the 202 path (M3-T16).
        """
        permit = self._acquire()
        if permit is None:
            logger.info(
                "call_deferred",
                extra={
                    "json_fields": {"event_id": event_id, "reason": CIRCUIT_OPEN}
                },
            )
            return RetryOutcome(result=_open_result(), attempts=0, deferred=True)

        with permit:
            outcome = await self._run(event_id, audio_ref, permit)
            if not outcome.deferred:
                permit.finish(success=outcome.result.ok)
            return outcome

    # --- internals ---------------------------------------------------------

    def _acquire(self) -> ProbePermit | None:
        return self._breaker.acquire_probe()

    async def _run(
        self, event_id: str, audio_ref: str, permit: ProbePermit
    ) -> RetryOutcome:
        """Drive the retry sequence, gating **every** attempt's dispatch.

        The permit admits the whole logical call, so attempt 1 is already
        admitted by :meth:`_acquire`. Before each *retry* dispatch the breaker
        is re-checked (SPECS §6.2): if it transitioned since the permit was
        acquired — typically CLOSED → OPEN because other traffic tripped it —
        the sequence stops without another backend call and the call becomes
        deferred. The epoch test is the check: reading :attr:`state` first so
        an elapsed cooldown can advance OPEN → HALF_OPEN (itself a transition
        that retires our permit). The exception is raised from ``op`` itself,
        i.e. before dispatch, never after a real attempt.
        """
        dispatched: list[AttemptRecord] = []

        async def op() -> PipelineResult:
            if dispatched and permit.epoch != self._current_epoch():
                raise _CircuitOpenError
            return await self._backend.process(event_id, audio_ref)

        def record(attempt: AttemptRecord) -> None:
            dispatched.append(attempt)

        try:
            outcome = await execute_with_retry(
                op,
                self._policy,
                sleeper=self._sleeper,
                clock=self._clock,
                rng=self._rng,
                metrics=self._metrics,
                on_attempt=record,
            )
        except _CircuitOpenError:
            logger.info(
                "call_deferred",
                extra={
                    "json_fields": {
                        "event_id": event_id,
                        "reason": CIRCUIT_OPEN,
                        "attempts": len(dispatched),
                    }
                },
            )
            return RetryOutcome(
                result=_open_result(),
                attempts=len(dispatched),
                records=tuple(dispatched),
                deferred=True,
            )

        return outcome

    def _current_epoch(self) -> int:
        """Epoch after the cooldown check (OPEN → HALF_OPEN may bump it)."""
        _ = self._breaker.state  # property read performs the cooldown check
        return self._breaker.epoch
