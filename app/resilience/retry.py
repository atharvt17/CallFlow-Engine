"""Retry policy + retryable classification (M3-T01, M3-T02; SPECS §6.1).

:class:`RetryPolicy` is the single source of retry numbers: built once from
``Settings`` (SPECS §8) and injected everywhere else, so the executor,
breaker, and tests never read ``Settings`` directly.

Worst-case logical-call wall time at the shipped defaults (SPECS §6.1):
``(MAX_ATTEMPTS × PIPELINE_TIMEOUT_S) + backoff`` = ``(3 × 2.5s) + 0.1s +
0.2s = 7.8s`` nominal, ≈7.95s at maximum +50% jitter. The per-attempt timeout
is the only deadline in Round 1 (SPECS §13 decision).

:func:`classify` is the pure decision function over one attempt's
:class:`~app.pipeline.base.PipelineResult`: which outcomes warrant another
attempt. It is table-driven and side-effect free; its cases are asserted by
``tests/unit/test_retry.py`` (M3-T05).

:func:`backoff_delay` is the deterministic-under-seed delay calculator
(M3-T03): the capped exponential base from :meth:`RetryPolicy.base_delay_ms`
scaled by injected uniform jitter.

:func:`execute_with_retry` is the async attempt engine (M3-T04): it drives
any ``op`` (one downstream attempt) up to ``max_attempts`` times, wrapping
each attempt in the per-attempt timeout, sleeping the backoff between
attempts, and stopping early on a non-retryable result. Sleeper, clock, and
RNG are injected, so unit tests run with no real sleeping.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from app.config import Settings
from app.metrics import Metrics
from app.pipeline.base import PipelineResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    """Typed retry parameters (SPECS §6.1 defaults; SPECS §8 env wiring).

    ``timeout_s`` is the per-attempt deadline; ``backoff_*`` shape the delay
    before retry ``k`` (1-indexed), with **cap-before-jitter ordering**
    (SPECS §6.1, normative)::

        delay_k = min(base · factor^(k-1), backoff_max_ms) · U[1-j, 1+j]

    The cap bounds the *base* delay only — at ``j=0.5`` a capped 1000 ms
    delay may reach 1500 ms. The ordering is inert at the shipped defaults
    (the 100/200 ms delays never reach the cap); it only matters when the
    policy is overridden or ``max_attempts`` is raised. The two wall-time
    properties below use the same ordering.

    Range validation mirrors ``Settings`` (``__post_init__``) so a directly
    constructed policy can never hold values the env layer would reject.
    """

    max_attempts: int = 3
    timeout_s: float = 2.5
    backoff_base_ms: int = 100
    backoff_factor: float = 2.0
    backoff_max_ms: int = 1000
    backoff_jitter: float = 0.5

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {self.timeout_s}")
        if self.backoff_base_ms < 0:
            raise ValueError(f"backoff_base_ms must be >= 0, got {self.backoff_base_ms}")
        if self.backoff_factor < 1:
            raise ValueError(f"backoff_factor must be >= 1, got {self.backoff_factor}")
        if self.backoff_max_ms < 0:
            raise ValueError(f"backoff_max_ms must be >= 0, got {self.backoff_max_ms}")
        if not 0 <= self.backoff_jitter <= 1:
            raise ValueError(f"backoff_jitter must be in [0, 1], got {self.backoff_jitter}")

    @classmethod
    def from_settings(cls, settings: Settings) -> RetryPolicy:
        """Bind the SPECS §8 env knobs into one immutable policy object."""
        return cls(
            max_attempts=settings.max_attempts,
            timeout_s=settings.pipeline_timeout_s,
            backoff_base_ms=settings.backoff_base_ms,
            backoff_factor=settings.backoff_factor,
            backoff_max_ms=settings.backoff_max_ms,
            backoff_jitter=settings.backoff_jitter,
        )

    def base_delay_ms(self, retry_k: int) -> float:
        """Capped, un-jittered delay before retry ``k`` (1-indexed).

        ``min(backoff_base_ms · factor^(k-1), backoff_max_ms)`` — SPECS §6.1
        with cap-before-jitter ordering. This is the single formula shared
        by the wall-time properties and (M3-T03) the backoff calculator.
        """
        if retry_k < 1:
            raise ValueError(f"retry_k is 1-indexed, got {retry_k}")
        return min(
            self.backoff_base_ms * self.backoff_factor ** (retry_k - 1),
            self.backoff_max_ms,
        )

    def _total_backoff_s(self, *, max_jitter: bool) -> float:
        multiplier = 1 + self.backoff_jitter if max_jitter else 1.0
        return sum(
            self.base_delay_ms(k) * multiplier / 1000.0
            for k in range(1, self.max_attempts)
        )

    @property
    def worst_case_wall_time_s(self) -> float:
        """Nominal worst-case logical-call wall time (SPECS §6.1: 7.8s).

        All attempts time out (``max_attempts × timeout_s``) plus the
        un-jittered capped backoff sum before retries 1..max_attempts-1.
        At the shipped defaults: 3×2.5 + 0.1 + 0.2 = 7.8s. Derived from the
        policy fields (not hardcoded), so overrides are reflected exactly.
        """
        return self.max_attempts * self.timeout_s + self._total_backoff_s(max_jitter=False)

    @property
    def worst_case_wall_time_max_jitter_s(self) -> float:
        """Upper bound with every delay at maximum positive jitter.

        ``max_attempts × timeout_s + Σ (capped delay · (1 + jitter))`` —
        ≈7.95s at the shipped defaults (SPECS §6.1).
        """
        return self.max_attempts * self.timeout_s + self._total_backoff_s(max_jitter=True)


def backoff_delay(retry_k: int, policy: RetryPolicy, rng: random.Random) -> float:
    """Delay in **milliseconds** before retry ``retry_k`` (1-indexed).

    ``delay = base_delay_ms(k) · U[1−j, 1+j]`` — the capped exponential base
    (SPECS §6.1, cap-before-jitter) perturbed by uniform jitter. At the
    shipped defaults and ``j=0`` the sequence is exactly 100, 200 ms; at
    ``j=0.5`` each delay lies within ±50% of its base, and a capped delay
    may exceed ``backoff_max_ms`` by the jitter factor (see
    :meth:`RetryPolicy.base_delay_ms`).

    ``rng`` is injected (``random.Random``) so tests are deterministic under
    a seed and the executor owns the random stream — this function is pure
    with respect to everything else.
    """
    base = policy.base_delay_ms(retry_k)
    jitter = policy.backoff_jitter
    return base * rng.uniform(1 - jitter, 1 + jitter)


class RetryDecision(Enum):
    """What the executor should do with one finished attempt."""

    RETRY = "retry"  # another attempt is warranted
    STOP = "stop"  # terminal for this logical call (success or non-retryable)


# Reasons the HTTP client maps to a retryable class (SPECS §6.1: network /
# connect errors, read timeouts, the spec 503). Any 5xx is retryable even
# when its body is not the spec body — the client then reports
# ``reason=None`` with ``http_status`` set (app/pipeline/http_client.py).
_RETRYABLE_REASONS = frozenset({"downstream_timeout", "timeout", "connection_error"})


def classify(result: PipelineResult) -> RetryDecision:
    """Decide whether one attempt's result warrants another attempt.

    Retryable (SPECS §6.1): ``timeout``, ``connection_error``,
    ``downstream_timeout``, and any HTTP 5xx (``http_status >= 500``, which
    covers ``reason=None`` non-spec 5xx bodies). Not retryable:
    ``non_retryable_error`` (4xx, or 2xx with an unexpected body) and, of
    course, success.
    """
    if result.ok:
        return RetryDecision.STOP
    if result.reason in _RETRYABLE_REASONS:
        return RetryDecision.RETRY
    if result.reason == "non_retryable_error":
        return RetryDecision.STOP
    # reason is None: the only remaining producer is a non-spec 5xx
    # (http_status >= 500) — retryable per SPECS §6.1 ("any HTTP 5xx").
    if result.http_status is not None and result.http_status >= 500:
        return RetryDecision.RETRY
    # Defensive: a failure with no retryable signal at all must not loop.
    return RetryDecision.STOP


# --- M3-T04: the async attempt engine ---------------------------------------


class Sleeper(Protocol):
    """Awaitable delay seam — production sleeps, tests record."""

    async def sleep(self, seconds: float) -> None: ...


class AsyncioSleeper:
    """Production sleeper (real time)."""

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@dataclass(frozen=True)
class AttemptRecord:
    """One attempt's outcome, as the executor observed it.

    ``latency_ms`` is the backend-reported latency when available (HTTP
    success/failure bodies carry it) and the executor-measured wall time
    otherwise (timeouts, inline backends). ``delay_before_ms`` is the backoff
    actually slept before this attempt (0 for the first).
    """

    attempt: int  # 1-indexed
    result: PipelineResult
    latency_ms: float | None
    delay_before_ms: float
    timed_out: bool = False


@dataclass(frozen=True)
class RetryOutcome:
    """Final outcome of one logical call (the whole retry sequence)."""

    result: PipelineResult
    attempts: int
    records: tuple[AttemptRecord, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return self.result.ok

    @property
    def retry_count(self) -> int:
        """Retries after the first attempt (``attempts - 1``, SPECS §3)."""
        return max(0, self.attempts - 1)


async def execute_with_retry(
    op: Callable[[], Awaitable[PipelineResult]],
    policy: RetryPolicy,
    *,
    sleeper: Sleeper | None = None,
    clock: Callable[[], float] | None = None,
    rng: random.Random | None = None,
    metrics: Metrics | None = None,
    on_attempt: Callable[[AttemptRecord], None] | None = None,
) -> RetryOutcome:
    """Run ``op`` up to ``policy.max_attempts`` times (SPECS §6.1, M3-T04).

    Each attempt is wrapped in ``asyncio.wait_for`` with the policy's
    per-attempt timeout; a timeout becomes a ``reason="timeout"`` result.
    Between attempts the executor sleeps ``backoff_delay(k, policy, rng)``.
    Stops early on success or a non-retryable result.

    An unexpected exception from ``op`` (the backend protocol promises none)
    is treated as a retryable ``connection_error`` so accounting never loses
    the logical call; cancellation still propagates (``BaseException``).

    Metrics (M3-T06), when supplied, are attempt-level only — they never feed
    the breaker window (SPECS §6.2): each attempt increments
    ``pipeline_attempts_total{result}`` and observes
    ``pipeline_attempt_latency_ms``; each scheduled retry increments
    ``retries_total``.
    """
    sleeper = sleeper if sleeper is not None else AsyncioSleeper()
    clock = clock if clock is not None else time.perf_counter
    rng = rng if rng is not None else random.Random()

    records: list[AttemptRecord] = []
    delay_before_ms = 0.0

    for attempt in range(1, policy.max_attempts + 1):
        started = clock()
        timed_out = False
        try:
            result = await asyncio.wait_for(op(), timeout=policy.timeout_s)
        except TimeoutError:
            timed_out = True
            result = PipelineResult(
                ok=False, reason="timeout", latency_ms=policy.timeout_s * 1000
            )
        except Exception as exc:  # noqa: BLE001 - protocol says backends don't raise
            logger.warning(
                "pipeline attempt raised unexpectedly",
                extra={"json_fields": {"attempt": attempt, "error": str(exc)}},
            )
            result = PipelineResult(ok=False, reason="connection_error")
        measured_ms = (clock() - started) * 1000
        latency_ms = result.latency_ms if result.latency_ms is not None else measured_ms

        record = AttemptRecord(
            attempt=attempt,
            result=result,
            latency_ms=latency_ms,
            delay_before_ms=delay_before_ms,
            timed_out=timed_out,
        )
        records.append(record)

        # Attempt-level observability (never feeds breaker statistics).
        if metrics is not None:
            if result.ok:
                outcome_label = "success"
            elif result.reason == "timeout":
                outcome_label = "timeout"
            else:
                outcome_label = "error"
            metrics.pipeline_attempts_total.labels(result=outcome_label).inc()
            metrics.pipeline_attempt_latency_ms.observe(latency_ms)
        if on_attempt is not None:
            on_attempt(record)

        if classify(result) is RetryDecision.STOP:
            return RetryOutcome(result=result, attempts=attempt, records=tuple(records))
        if attempt == policy.max_attempts:
            break

        delay_before_ms = backoff_delay(attempt, policy, rng)
        if metrics is not None:
            metrics.retries_total.inc()
        logger.info(
            "retry_scheduled",
            extra={
                "json_fields": {
                    "attempt": attempt,
                    "delay_ms": round(delay_before_ms, 2),
                    "reason": result.reason,
                }
            },
        )
        await sleeper.sleep(delay_before_ms / 1000)

    return RetryOutcome(result=records[-1].result, attempts=len(records), records=tuple(records))

