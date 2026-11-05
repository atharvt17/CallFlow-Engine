"""Model-agnostic pipeline boundary (SPECS §2, §6; PLAN scope table row 3).

The boundary is the ONLY extension point for swapping the dependency (SPECS
§13): the resilience layer (Sprint 3) decorates :class:`PipelineBackend`
implementations and never touches concrete backends, so swapping the mock for
a real (or scoring-harness) pipeline is a configuration change, not a code
change. HTTP types must not leak into the interface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

ReasonCode = Literal[
    "downstream_timeout",
    "timeout",
    "connection_error",
    "non_retryable_error",
]


@dataclass(frozen=True)
class PipelineResult:
    """Outcome of ONE downstream pipeline attempt (not a logical call).

    ``reason`` uses the SPECS §7 vocabulary; ``None`` means the attempt
    succeeded (``ok=True``) or the failure carried no classifiable reason
    (e.g. a 5xx whose body was not the spec body — ``http_status`` then
    carries the retryability signal for the Sprint 3 classifier).
    """

    ok: bool
    reason: ReasonCode | None = None
    http_status: int | None = None
    latency_ms: float | None = None


class PipelineBackend(Protocol):
    """Anything that can process one call event through the pipeline."""

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        """Process one call; must never raise for *service* failures —
        transport-level failures are mapped into :class:`PipelineResult`."""
        ...
