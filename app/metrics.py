"""Prometheus metrics surface (M2-T10; SPECS §9).

**Cardinality rule:** no event IDs, borrower IDs, or other unique identifiers
in Prometheus labels — identifiers live in structured logs only. The allowed
label vocabulary is exactly: ``outcome``/``status`` (calls_total),
``result`` (attempts, replays), ``from``/``to`` (breaker transitions),
``status`` (dlq_entries). ``tests/unit/test_metrics.py`` enforces it.

A fresh :class:`CollectorRegistry` per :class:`Metrics` bundle keeps
concurrent apps (compose) and test stacks isolated; ``GET /metrics`` renders
only its own app's registry.

Series registered now, populated by later sprints (SPECS §9):
``circuit_state`` / ``circuit_transitions_total`` (Sprint 3),
``retries_total`` (Sprint 3 executor), ``deferred_pending`` /
``deferred_queue_depth`` / ``deferred_expired_total`` (Sprint 4),
``dlq_entries`` / ``dlq_replays_total`` (Sprints 4–5).
"""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Millisecond buckets: attempt latencies center on the Appendix B lognormal
# (p50 300 / p90 700 / p99 1200 ms; per-attempt timeout 2500 ms).
_ATTEMPT_BUCKETS = (10.0, 50.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0, 2500.0)
# e2e spans submission -> terminal completion, incl. backoff and any deferral
# wait (Sprint 4); worst-case logical call ≈ 7.8 s at Round 1 settings.
_E2E_BUCKETS = (50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 4000.0, 8000.0, 16000.0, 32000.0)


class Metrics:
    """All SPECS §9 series for one app instance."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.calls_total = Counter(
            "calls_total",
            "Terminal call outcomes by service-owned outcome and event status.",
            ("outcome", "status"),
            registry=self.registry,
        )
        self.in_flight_calls = Gauge(
            "in_flight_calls",
            "Logical pipeline calls currently executing.",
            registry=self.registry,
        )
        self.pipeline_attempts_total = Counter(
            "pipeline_attempts_total",
            "Downstream attempt results (success / error / timeout).",
            ("result",),
            registry=self.registry,
        )
        self.retries_total = Counter(
            "retries_total",
            "Retry attempts scheduled (populated by the Sprint 3 retry executor).",
            registry=self.registry,
        )
        self.pipeline_attempt_latency_ms = Histogram(
            "pipeline_attempt_latency_ms",
            "Latency of one downstream attempt (ms).",
            buckets=_ATTEMPT_BUCKETS,
            registry=self.registry,
        )
        self.e2e_latency_ms = Histogram(
            "e2e_latency_ms",
            "Complete logical calls: submission to terminal completion (ms).",
            buckets=_E2E_BUCKETS,
            registry=self.registry,
        )
        # --- registered now, populated in Sprints 3-5 ---
        self.circuit_state = Gauge(
            "circuit_state",
            "Circuit breaker state (0 closed / 1 open / 2 half_open).",
            registry=self.registry,
        )
        self.circuit_state.set(0)  # closed until the breaker exists (Sprint 3)
        self.circuit_transitions_total = Counter(
            "circuit_transitions_total",
            "Circuit breaker transitions.",
            ("from", "to"),
            registry=self.registry,
        )
        self.deferred_pending = Gauge(
            "deferred_pending",
            "Deferred events pending in SQLite (source of truth).",
            registry=self.registry,
        )
        self.deferred_queue_depth = Gauge(
            "deferred_queue_depth",
            "In-memory deferred FIFO depth (processing mechanism only).",
            registry=self.registry,
        )
        self.deferred_expired_total = Counter(
            "deferred_expired_total",
            "Deferred events expired past DEFER_MAX_AGE_S into the DLQ.",
            registry=self.registry,
        )
        self.dlq_entries = Gauge(
            "dlq_entries",
            "Dead-letter rows by status (pending / replayed).",
            ("status",),
            registry=self.registry,
        )
        # Pre-initialize both statuses so the exposition carries explicit 0.0
        # placeholders before any DLQ activity (stable dashboards, M4 reports).
        self.dlq_entries.labels(status="pending")
        self.dlq_entries.labels(status="replayed")
        self.dlq_replays_total = Counter(
            "dlq_replays_total",
            "DLQ replay outcomes.",
            ("result",),
            registry=self.registry,
        )

    def render(self) -> tuple[bytes, str]:
        """Text exposition body + content type for ``GET /metrics``."""
        return generate_latest(self.registry), CONTENT_TYPE_LATEST
