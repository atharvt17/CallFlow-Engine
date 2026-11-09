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
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from app.config import Settings
from app.pipeline.base import PipelineResult


@dataclass(frozen=True)
class RetryPolicy:
    """Typed retry parameters (SPECS §6.1 defaults; SPECS §8 env wiring).

    ``timeout_s`` is the per-attempt deadline; ``backoff_*`` shape the delay
    before retry ``k`` (1-indexed): ``base · factor^(k-1)`` capped at
    ``backoff_max_ms``, scaled by uniform jitter in ``[1-j, 1+j]``.
    """

    max_attempts: int = 3
    timeout_s: float = 2.5
    backoff_base_ms: int = 100
    backoff_factor: float = 2.0
    backoff_max_ms: int = 1000
    backoff_jitter: float = 0.5

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

    @property
    def worst_case_wall_time_s(self) -> float:
        """Nominal worst-case logical-call wall time (SPECS §6.1: 7.8s).

        All attempts time out (``max_attempts × timeout_s``) plus the
        un-jittered backoff sum before retries 1..max_attempts-1
        (``base · (factor^(max_attempts-1) - 1) / (factor - 1)`` when
        factor ≠ 1, else ``base · (max_attempts - 1)``), both capped by
        ``backoff_max_ms`` per delay. At the shipped defaults this is
        3×2.5 + 0.1 + 0.2 = 7.8s (≈7.95s at maximum +50% jitter).
        """
        attempts_cost = self.max_attempts * self.timeout_s
        backoff_cost = 0.0
        for k in range(1, self.max_attempts):
            delay = min(self.backoff_base_ms * self.backoff_factor ** (k - 1), self.backoff_max_ms)
            backoff_cost += delay / 1000.0
        return attempts_cost + backoff_cost


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
