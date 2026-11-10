"""Read-side reporting shared by HTTP and the CLI (M4-T02; SPECS §2, §5, §9).

The DLQ/stats surfaces exist twice — ``GET /admin/dlq`` over HTTP and
``python -m app.cli dlq list`` — and SPECS §2 forbids two implementations of
one operation. The serializers and the ``/stats`` body therefore live here,
imported by both entry points, so a field can never appear on one surface and
not the other.

Everything here is read-only and derives from the authoritative SQLite store
plus the process's Prometheus registry.
"""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Any

from app import store
from app.metrics import Metrics
from app.resilience.circuit_breaker import CircuitBreaker

# Latency percentiles reported by ``GET /stats`` (the ones the Appendix I
# runbook and the SPECS §10 run report ask for).
PERCENTILES = (("p50", 0.50), ("p90", 0.90), ("p99", 0.99))


def record_payload(row: sqlite3.Row) -> dict[str, object]:
    """Stored row → response record; ``event_json`` parsed under ``event``.

    Every column of the row is returned, which is what makes the DLQ surfaces
    (``reason``, ``attempts``, ``retry_count``, both failure timestamps, replay
    fields) complete per SPECS §7 without a second serialization path.
    """
    record = dict(row)
    record["event"] = json.loads(record.pop("event_json"))
    return record


def stats_body(
    conn: sqlite3.Connection,
    metrics: Metrics,
    *,
    breaker: CircuitBreaker | None = None,
    uptime_s: float | None = None,
) -> dict[str, object]:
    """The ``/stats`` body: durable counts + process-local measurements.

    Counts come from the authoritative store (they survive a restart); latency
    percentiles come from this process's histograms and reset with it, which is
    why they are labelled separately.

    ``breaker``/``uptime_s`` are optional because the CLI runs in its own
    process: it can read the same database but cannot observe the serving
    process's breaker, and reporting a freshly constructed (always closed)
    breaker would state something false about the running service. Those keys
    are omitted rather than fabricated.
    """
    body: dict[str, object] = {
        "events": store.count_events_by_status(conn),
        "dead_letter": store.count_dead_letters(conn),
        "deferred_pending": store.count_deferred(conn),
    }
    if breaker is not None:
        body["circuit_breaker"] = breaker.snapshot()
    body["latency_percentiles"] = latency_percentiles(metrics)
    if uptime_s is not None:
        body["uptime_s"] = uptime_s
    return body


def latency_percentiles(metrics: Metrics) -> dict[str, float | None]:
    """``p50``/``p90``/``p99`` of e2e latency, estimated from the histogram.

    Linear interpolation inside the bucket that contains the quantile, which
    is the standard Prometheus ``histogram_quantile`` behaviour. The values
    are therefore bucket-resolution — good enough for an operator summary,
    and explicitly *not* a replacement for the traffic generator's
    client-observed percentiles, which are exact (SPECS §10).

    Returns ``None`` per percentile until at least one call has completed.
    """
    metrics.e2e_latency_ms.collect()  # ensures the metric exists
    samples = histogram_samples(metrics, "e2e_latency_ms")
    if samples is None or not samples["count"]:
        return {name: None for name, _ in PERCENTILES}
    buckets = sorted(samples["buckets"].items())  # [(upper_bound, cumulative)]
    return {
        name: round(interpolate_quantile(buckets, samples["count"], q), 1)
        for name, q in PERCENTILES
    }


def histogram_samples(metrics: Metrics, name: str) -> dict[str, Any] | None:
    """Cumulative bucket counts + total for one histogram, or ``None``.

    Reads ``collect()`` rather than the private ``_buckets`` attribute so the
    report stays correct across prometheus-client versions. Bucket bounds
    arrive as labels (``"50.0"``, ``"+Inf"``), hence the float parsing.
    """
    for metric in metrics.registry.collect():
        if metric.name != name:
            continue
        buckets: dict[float, float] = {}
        count = 0.0
        for sample in metric.samples:
            if sample.name == f"{name}_bucket":
                le = sample.labels["le"]
                buckets[math.inf if le == "+Inf" else float(le)] = sample.value
            elif sample.name == f"{name}_count":
                count = sample.value
        return {"buckets": buckets, "count": count}
    return None


def interpolate_quantile(
    buckets: list[tuple[float, float]], count: float, q: float
) -> float:
    """Linear interpolation within the bucket containing quantile ``q``.

    ``buckets`` are ``(le, cumulative_count)`` sorted by bound. The final
    ``+Inf`` bucket is used as an open-ended upper bound only when no finite
    bucket contains the quantile — the estimate then falls back to the last
    finite bound, which is the least-wrong answer available.
    """
    target = q * count
    previous_le, previous_count = 0.0, 0.0
    for le, cumulative in buckets:
        if cumulative >= target:
            if le == math.inf:
                return previous_le
            span = cumulative - previous_count
            if span <= 0:
                return le
            return previous_le + (le - previous_le) * ((target - previous_count) / span)
        previous_le, previous_count = le, cumulative
    return previous_le
