"""Appendix C traffic generator and honest run report (M5-T02/M5-T03; SPECS §10).

    python -m tools.traffic_gen --speedup 60          # full compressed day
    python -m tools.traffic_gen --duration-sim-hours 0.2 --speedup 60   # smoke

The generator replays one simulated day of Appendix C traffic against a
running inference service. The schedule lives in :mod:`tools.schedule` (pure,
reviewable without firing a request); this module turns it into traffic and,
just as importantly, into evidence.

**Honesty rules encoded here** (SPECS §10):

- Every window reports its *target* rate next to the rate actually delivered,
  both in simulated and in observed real terms, plus the peak in-flight count
  and a ``saturated`` flag. A rate the run could not reach is never reported as
  achieved.
- ``silent_loss`` is derived from data, never asserted: accepted events minus
  the server's terminal delta minus its deferred delta. It is only meaningful
  once the deferred queue has drained, which the runner waits for.
- The compression factor is recorded in every report (Appendix C's "say so").

Exit codes: ``0`` the report passed its own verdict, ``1`` verdict failed or
the run aborted (service unreachable), ``2`` argparse's usage error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import sys
import time
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
from prometheus_client.parser import text_string_to_metric_families

from mock.latency import percentiles
from tools import schedule

# --- constants --------------------------------------------------------------

TICK_SIM_S = 0.25  # SPECS §10: scheduler tick, in simulated seconds
CAMPAIGN_ID = "CAMP-2026-09-A"  # Appendix A campaign used by every run
SIM_START = datetime(2026, 9, 1, tzinfo=UTC)  # the simulated day's 00:00
REQUEST_TIMEOUT_S = 30.0  # per-request client timeout (SPECS §10)
EXPECTED_FAILURE_RATE = 0.20  # Appendix B mock default
EXPECTED_DLQ_FRACTION = EXPECTED_FAILURE_RATE**3  # 0.008 — three failed attempts
SATURATION_TOLERANCE = 0.10  # delivered below 90% of target => saturated
DLQ_RELATIVE_TOLERANCE = 0.35  # minimum band, used when the sample is large
DLQ_SIGMA_BAND = 3.0  # band floor in binomial standard deviations
TRANSPORT_RETRIES = 3  # re-submissions of the same event_id on a transport failure
TRANSPORT_BACKOFF_S = 0.05  # linear backoff unit between those re-submissions
MIN_SAMPLE_FOR_DLQ_VERDICT = 5_000  # below this the fraction is sampling noise
SCHEDULE_TOLERANCE = 0.05  # generated vs schedule arithmetic
REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"

_BUCKETS = ("0-30 DPD", "31-60 DPD", "61-90 DPD", "90+ DPD")
_OUTCOMES = ("connected", "no_answer", "ptp_taken", "dispute")
_PRODUCTS = ("credit_card", "bnpl", "personal_loan")


class GeneratorError(RuntimeError):
    """The run cannot continue (unreachable service, queue never drained)."""


# --- pure helpers -----------------------------------------------------------


def sim_timestamp(sim_s: float) -> str:
    """RFC3339 timestamp on the simulated clock (Appendix A ``timestamp``)."""
    moment = SIM_START + timedelta(seconds=sim_s)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_payload(event_id: UUID, sim_s: float) -> dict[str, Any]:
    """One Appendix A event; variety derived from the UUID, never global RNG."""
    return {
        "event_id": str(event_id),
        "campaign_id": CAMPAIGN_ID,
        "borrower_bucket": _BUCKETS[event_id.int % len(_BUCKETS)],
        "timestamp": sim_timestamp(sim_s),
        "outcome": _OUTCOMES[(event_id.int >> 8) % len(_OUTCOMES)],
        "product_type": _PRODUCTS[(event_id.int >> 16) % len(_PRODUCTS)],
    }


def poisson(rng: random.Random, lam: float) -> int:
    """Poisson draw (Knuth); the schedule's per-tick λ never exceeds ~0.21."""
    if lam <= 0:
        return 0
    if lam > 30:  # unreachable by this schedule; keeps the helper safe anyway
        return max(0, round(rng.normalvariate(lam, math.sqrt(lam))))
    limit = math.exp(-lam)
    count, product = 0, 1.0
    while True:
        product *= rng.random()
        if product <= limit:
            return count
        count += 1


def sim_clock(seconds_into_day: float) -> str:
    """``HH:MM`` label for a simulated instant."""
    total = int(seconds_into_day)
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}"


def is_saturated(
    *,
    target_sim_cpm: float,
    achieved_sim_cpm: float,
    peak_in_flight: int,
    concurrency: int,
    target_real_cpm: float | None = None,
    observed_real_cpm: float | None = None,
    backpressure_waits: int = 0,
) -> bool:
    """Could the generator not keep up with this window?

    Three independent signals, any of which is disqualifying: the concurrency
    cap was actually hit, the producer had to throttle (backpressure), or the
    delivered rate — simulated *or* observed real — fell beyond the Poisson
    tolerance. Reporting the flag rather than hiding it is the point.
    """
    if peak_in_flight >= concurrency:
        return True
    if backpressure_waits > 0:
        return True
    if target_sim_cpm > 0 and achieved_sim_cpm < target_sim_cpm * (1 - SATURATION_TOLERANCE):
        return True
    return bool(
        target_real_cpm
        and observed_real_cpm is not None
        and observed_real_cpm < target_real_cpm * (1 - SATURATION_TOLERANCE)
    )


def window_rows(
    windows: tuple[schedule.Window, ...],
    records: list[AttemptRecord],
    *,
    speedup: float,
    concurrency: int,
    real_spans: list[tuple[float, float] | None] | None = None,
    backpressure: Counter | None = None,
) -> list[dict[str, Any]]:
    """Per-window target vs achieved throughput (the §10 table).

    ``achieved`` counts events the service actually answered, bucketed by the
    simulated instant they were scheduled for — an event generated but never
    delivered is not throughput; it surfaces in silent-loss accounting. The
    real rate is measured over the wall-clock span the producer actually spent
    in that window, so a stalled run cannot report a nominal rate.
    """
    starts = [window.start_s for window in windows]
    generated = [0] * len(windows)
    delivered = [0] * len(windows)
    peaks = [0] * len(windows)

    def index_of(sim_s: float) -> int:
        return min(max(bisect_right(starts, sim_s) - 1, 0), len(windows) - 1)

    for record in records:
        index = index_of(record.sim_s)
        generated[index] += 1
        if record.http_status is not None:
            delivered[index] += 1
            peaks[index] = max(peaks[index], record.concurrency_at_start)

    rows: list[dict[str, Any]] = []
    for index, window in enumerate(windows):
        target_sim = schedule.expected_calls(window.start_s, window.end_s) / window.duration_min
        achieved_sim = delivered[index] / window.duration_min
        target_real = schedule.compressed_rate(target_sim, speedup)
        span = real_spans[index] if real_spans and index < len(real_spans) else None
        elapsed_s = (span[1] - span[0]) if span and span[1] > span[0] else None
        observed_real = (delivered[index] / (elapsed_s / 60)) if elapsed_s else None
        waits = backpressure.get(index, 0) if backpressure else 0
        rows.append(
            {
                "label": window.label,
                "sim_start": sim_clock(window.start_s),
                "sim_end": sim_clock(window.end_s),
                "duration_sim_min": round(window.duration_min, 2),
                "target_calls_per_min_sim": round(target_sim, 2),
                "target_calls_per_min_real": round(target_real, 1),
                "achieved_calls_per_min_sim": round(achieved_sim, 2),
                "achieved_calls_per_min_real_nominal": round(
                    schedule.compressed_rate(achieved_sim, speedup), 1
                ),
                "achieved_calls_per_min_real_observed": (
                    round(observed_real, 1) if observed_real is not None else None
                ),
                "real_elapsed_s": round(elapsed_s, 3) if elapsed_s else None,
                "generated": generated[index],
                "delivered": delivered[index],
                "peak_in_flight": peaks[index],
                "backpressure_waits": waits,
                "saturated": is_saturated(
                    target_sim_cpm=target_sim,
                    achieved_sim_cpm=achieved_sim,
                    peak_in_flight=peaks[index],
                    concurrency=concurrency,
                    target_real_cpm=target_real,
                    observed_real_cpm=observed_real,
                    backpressure_waits=waits,
                ),
            }
        )
    return rows


def parse_metrics(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Prometheus text exposition → ``{sample_name: [(labels, value), ...]}``."""
    parsed: dict[str, list[tuple[dict[str, str], float]]] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            parsed.setdefault(sample.name, []).append((dict(sample.labels), sample.value))
    return parsed


def metrics_snapshot(parsed: dict[str, list[tuple[dict[str, str], float]]]) -> dict[str, Any]:
    """The §10 report's server-side counters, read from ``/metrics``."""
    return {
        "calls_total": _labeled(parsed, "calls_total", ("outcome", "status")),
        "pipeline_attempts_total": _labeled(parsed, "pipeline_attempts_total", ("result",)),
        "retries_total": _scalar(parsed, "retries_total"),
        "circuit_transitions_total": _labeled(
            parsed, "circuit_transitions_total", ("from", "to")
        ),
        "deferred_expired_total": _scalar(parsed, "deferred_expired_total"),
        "dlq_replays_total": _labeled(parsed, "dlq_replays_total", ("result",)),
    }


def _scalar(parsed: dict[str, list[tuple[dict[str, str], float]]], name: str) -> float | None:
    samples = parsed.get(name)
    return samples[0][1] if samples else None


def _labeled(
    parsed: dict[str, list[tuple[dict[str, str], float]]],
    name: str,
    keys: tuple[str, ...],
) -> dict[str, float]:
    out: dict[str, float] = {}
    for labels, value in parsed.get(name, []):
        key = "->".join(labels.get(k, "") for k in keys)
        out[key] = out.get(key, 0.0) + value
    return out


def _counter_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    keys = set(before) | set(after)
    return {key: round(after.get(key, 0.0) - before.get(key, 0.0), 3) for key in sorted(keys)}


def default_report_path(now: datetime | None = None) -> Path:
    """``reports/run_<ts>.json`` (SPECS §10)."""
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return REPORTS_DIR / f"run_{stamp}.json"


# --- runner state -----------------------------------------------------------


@dataclass(frozen=True)
class GeneratorConfig:
    """One run's controls (SPECS §8 traffic-gen variables)."""

    target_url: str
    speedup: float
    horizon_s: float
    concurrency: int
    seed: int | None
    out_path: Path
    drain_timeout_s: float = 300.0
    request_timeout_s: float = REQUEST_TIMEOUT_S
    max_pending_factor: int = 4  # queue cap = factor × concurrency (backpressure)


@dataclass
class AttemptRecord:
    """One generated event and what the service did with it."""

    event_id: UUID
    sim_s: float
    concurrency_at_start: int = 0
    http_status: int | None = None
    body_status: str | None = None
    outcome: str | None = None
    attempts: int | None = None
    retry_count: int | None = None
    latency_ms: float | None = None
    duplicate: bool = False
    error: str | None = None
    transport_retries: int = 0
    first_error: str | None = None


@dataclass
class WorkItem:
    """A scheduled event awaiting a worker slot."""

    record: AttemptRecord
    payload: dict[str, Any]


@dataclass
class RunOutcome:
    """Everything a report needs, gathered by the runner."""

    config: GeneratorConfig
    records: list[AttemptRecord]
    real_duration_s: float
    peak_pending: int
    window_spans: list[tuple[float, float] | None]
    backpressure: Counter
    server_before: dict[str, Any]
    server_after: dict[str, Any]
    breaker_final: dict[str, Any]
    metrics_before: dict[str, Any]
    metrics_after: dict[str, Any]
    notes: list[str] = field(default_factory=list)


# --- report assembly (pure) -------------------------------------------------


def build_report(outcome: RunOutcome) -> dict[str, Any]:
    """Assemble the SPECS §10 run report from collected measurements."""
    config = outcome.config
    records = outcome.records
    delivered = [record for record in records if record.http_status is not None]
    accepted = [record for record in delivered if record.http_status in (200, 202)]
    client_errors = [record for record in records if record.http_status is None]
    http_errors = [record for record in delivered if record.http_status not in (200, 202)]

    duplicates = sum(1 for record in delivered if record.duplicate)
    # A duplicate that answers *our own* re-submission is not a second event:
    # it is the proof that an ambiguous first POST landed, and its body carries
    # that event's real terminal state. Only duplicates of another generated
    # event may be subtracted from the unique count, or the retried event would
    # vanish from the accounting (and grow "silent loss" by one).
    self_duplicates = sum(
        1 for record in delivered if record.duplicate and record.transport_retries
    )
    accepted_unique = len(accepted) - duplicates + self_duplicates
    transport_retries = sum(record.transport_retries for record in records)
    transport_retried_calls = sum(1 for record in records if record.transport_retries)
    by_body = Counter(
        record.body_status for record in delivered if record.body_status is not None
    )

    server_after = outcome.server_after
    server_before = outcome.server_before
    events_after = server_after.get("events", {})
    events_before = server_before.get("events", {})
    terminal_after = int(events_after.get("processed", 0)) + int(
        events_after.get("failed_downstream", 0)
    )
    terminal_before = int(events_before.get("processed", 0)) + int(
        events_before.get("failed_downstream", 0)
    )
    deferred_after = int(server_after.get("deferred_pending", 0))
    deferred_before = int(server_before.get("deferred_pending", 0))
    processing_after = int(events_after.get("processing", 0))

    terminal_delta = terminal_after - terminal_before
    deferred_delta = deferred_after - deferred_before
    # §7.1: every accepted event must be terminal, or still durably deferred
    # (the runner waits for the latter to reach zero). Anything else is loss.
    silent_loss = accepted_unique - terminal_delta - deferred_delta

    windows = schedule.windows_upto(config.horizon_s)
    rows = window_rows(
        windows,
        records,
        speedup=config.speedup,
        concurrency=config.concurrency,
        real_spans=outcome.window_spans,
        backpressure=outcome.backpressure,
    )
    expected_generated = schedule.expected_calls(0.0, config.horizon_s)
    schedule_delta_pct = (
        (len(records) - expected_generated) / expected_generated * 100
        if expected_generated
        else 0.0
    )

    dlq_after = server_after.get("dead_letter", {})
    dlq_before = server_before.get("dead_letter", {})
    dlq_total_after = int(dlq_after.get("pending", 0)) + int(dlq_after.get("replayed", 0))
    dlq_total_before = int(dlq_before.get("pending", 0)) + int(dlq_before.get("replayed", 0))
    dlq_delta = dlq_total_after - dlq_total_before
    expected_dlq = EXPECTED_DLQ_FRACTION * accepted_unique
    observed_fraction = dlq_delta / accepted_unique if accepted_unique else 0.0
    # A run is either a ~0.8% three-strike outcome or it is not; the band has to
    # be wide enough that a correct run is not flaky. The DLQ count is binomial
    # with p=0.008, so the standard deviation is sqrt(n·p·(1-p)) — the band is
    # the wider of 35% relative and 3σ, which at 10,500 events is about ±1.7%.
    dlq_sigma = math.sqrt(accepted_unique * EXPECTED_DLQ_FRACTION * (1 - EXPECTED_DLQ_FRACTION))
    dlq_band = max(
        expected_dlq * DLQ_RELATIVE_TOLERANCE,
        DLQ_SIGMA_BAND * dlq_sigma,
    )
    dlq_band_fraction = dlq_band / accepted_unique if accepted_unique else 0.0
    if accepted_unique >= MIN_SAMPLE_FOR_DLQ_VERDICT:
        dlq_within_tolerance: bool | None = (
            expected_dlq - dlq_band <= dlq_delta <= expected_dlq + dlq_band
        )
    else:
        dlq_within_tolerance = None

    latency = _latency_summary(
        [record.latency_ms for record in delivered if record.latency_ms is not None]
    )

    attempts_known = [record for record in delivered if record.attempts is not None]
    retries = {
        "calls_with_attempt_data": len(attempts_known),
        "calls_with_retries": sum(1 for record in attempts_known if record.retry_count),
        "retry_count_total": sum(int(record.retry_count or 0) for record in attempts_known),
        "max_attempts_observed": max(
            (int(record.attempts or 0) for record in attempts_known), default=0
        ),
        "attempts_histogram": {
            str(key): value
            for key, value in sorted(
                Counter(int(record.attempts or 0) for record in attempts_known).items()
            )
        },
        "server_retries_total": _metric_delta_value(
            outcome.metrics_before.get("retries_total"),
            outcome.metrics_after.get("retries_total"),
        ),
        "server_attempts": _counter_delta(
            outcome.metrics_before.get("pipeline_attempts_total", {}),
            outcome.metrics_after.get("pipeline_attempts_total", {}),
        ),
    }

    transitions = _counter_delta(
        outcome.metrics_before.get("circuit_transitions_total", {}),
        outcome.metrics_after.get("circuit_transitions_total", {}),
    )
    opened = sum(value for key, value in transitions.items() if key.endswith("->open"))

    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "tool": "python -m tools.traffic_gen",
        "target_url": config.target_url,
        "compression": {
            "speedup": config.speedup,
            "sim_hours": round(config.horizon_s / 3600, 4),
            "sim_seconds": config.horizon_s,
            "real_seconds_nominal": round(
                schedule.compressed_duration_s(config.horizon_s, config.speedup), 3
            ),
            "real_seconds_observed": round(outcome.real_duration_s, 3),
            "statement": (
                f"time compressed {config.speedup:g}x — 1 simulated hour = "
                f"{3600 / config.speedup:.1f} real seconds (Appendix C 'say so')"
            ),
            "baseline": {
                "sim_calls_per_min": 10.0,
                "real_calls_per_min": schedule.compressed_rate(10.0, config.speedup),
            },
            "peak": {
                "sim_calls_per_min": 50.0,
                "real_calls_per_min": schedule.compressed_rate(50.0, config.speedup),
            },
        },
        "traffic": {
            "generated": len(records),
            "expected_by_schedule": round(expected_generated, 1),
            "schedule_delta_pct": round(schedule_delta_pct, 3),
            "concurrency": config.concurrency,
            "max_pending": config.max_pending_factor * config.concurrency,
            "tick_sim_s": TICK_SIM_S,
            "peak_in_flight": max(
                (record.concurrency_at_start for record in records), default=0
            ),
            "peak_pending": outcome.peak_pending,
            "backpressure_waits": int(sum(outcome.backpressure.values())),
            "seed": config.seed,
        },
        "windows": rows,
        "requests": {
            "submitted": len(records),
            "delivered": len(delivered),
            "accepted": len(accepted),
            "accepted_unique": accepted_unique,
            "processed": by_body.get("processed", 0),
            "failed_downstream": by_body.get("failed_downstream", 0),
            "deferred": by_body.get("deferred", 0),
            "duplicate": duplicates,
            "self_duplicates": self_duplicates,
            "http_errors": len(http_errors),
            "client_errors": len(client_errors),
            "client_error_reasons": dict(
                Counter(
                    (record.error or "unknown").split(":", 1)[0] for record in client_errors
                )
            ),
            "by_http_status": {
                str(key): value
                for key, value in sorted(
                    Counter(record.http_status for record in delivered).items()
                )
            },
            "transport_retries": transport_retries,
            "calls_with_transport_retries": transport_retried_calls,
            "transport_error_reasons": dict(
                Counter(
                    (record.first_error or "unknown").split(":", 1)[0]
                    for record in delivered
                    if record.transport_retries
                )
            ),
        },
        "retries": retries,
        "circuit_breaker": {
            "transitions": transitions,
            "opened_count": opened,
            "final": outcome.breaker_final,
        },
        "dlq": {
            "pending": int(dlq_after.get("pending", 0)),
            "replayed": int(dlq_after.get("replayed", 0)),
            "created_this_run": dlq_delta,
            "expected_fraction": EXPECTED_DLQ_FRACTION,
            "observed_fraction": round(observed_fraction, 5),
            "expected_count": round(expected_dlq, 1),
            "within_tolerance": dlq_within_tolerance,
            "tolerance_absolute": round(dlq_band, 1),
            "tolerance_relative_effective": round(dlq_band_fraction, 4),
            "tolerance_sigma_band": DLQ_SIGMA_BAND,
        },
        "latency_ms": {
            "client_observed": latency,
            "server_e2e_estimate": server_after.get("latency_percentiles"),
        },
        "silent_loss": {
            "submitted": len(records),
            "accepted": len(accepted),
            "accepted_unique": accepted_unique,
            "terminal_delta": terminal_delta,
            "deferred_delta": deferred_delta,
            "deferred_pending_at_end": deferred_after,
            "processing_at_end": processing_after,
            "count": silent_loss,
            "formula": (
                "accepted_unique - terminal_delta - deferred_delta; 0 means every "
                "accepted event is terminal once the deferred queue has drained (§7.1)"
            ),
        },
        "server_stats": server_after,
        "server_stats_before": server_before,
        "server_metrics_delta": {
            "retries_total": retries["server_retries_total"],
            "calls_total": _counter_delta(
                outcome.metrics_before.get("calls_total", {}),
                outcome.metrics_after.get("calls_total", {}),
            ),
            "deferred_expired_total": _metric_delta_value(
                outcome.metrics_before.get("deferred_expired_total"),
                outcome.metrics_after.get("deferred_expired_total"),
            ),
        },
    }
    report["verdict"] = _verdict(report)
    report["notes"] = outcome.notes
    return report


def _latency_summary(samples: list[float]) -> dict[str, Any]:
    if not samples:
        return {"count": 0, "p50": None, "p90": None, "p99": None, "min": None, "max": None}
    p50, p90, p99 = percentiles(samples, [0.5, 0.9, 0.99])
    return {
        "count": len(samples),
        "p50": round(p50, 2),
        "p90": round(p90, 2),
        "p99": round(p99, 2),
        "min": round(min(samples), 2),
        "max": round(max(samples), 2),
        "mean": round(sum(samples) / len(samples), 2),
    }


def _metric_delta_value(before: float | None, after: float | None) -> float:
    return round((after or 0.0) - (before or 0.0), 3)


def _verdict(report: dict[str, Any]) -> dict[str, Any]:
    """The report's own pass/fail, so a run can never be green by omission."""
    failures: list[str] = []
    checks: dict[str, bool] = {}
    silence = report["silent_loss"]

    checks["zero_silent_loss"] = silence["count"] == 0
    if not checks["zero_silent_loss"]:
        failures.append(
            f"silent loss is {silence['count']} (accepted events with no terminal state)"
        )

    checks["deferred_queue_drained"] = (
        silence["deferred_pending_at_end"] == 0 and silence["processing_at_end"] == 0
    )
    if not checks["deferred_queue_drained"]:
        failures.append(
            f"end state not terminal: deferred_pending={silence['deferred_pending_at_end']} "
            f"processing={silence['processing_at_end']}"
        )

    client_errors = report["requests"]["client_errors"]
    checks["no_client_errors"] = client_errors == 0
    if client_errors:
        reasons = report["requests"]["client_error_reasons"]
        failures.append(
            f"{client_errors} requests never resolved (transport failures after "
            f"{TRANSPORT_RETRIES} re-submissions of the same event_id; accounting "
            f"ambiguous): {reasons}"
        )

    rows = report["windows"]
    checks["windows_all_ran"] = bool(rows) and all(row["delivered"] > 0 for row in rows)
    if not checks["windows_all_ran"]:
        failures.append("at least one scheduled window delivered no traffic")

    dlq_ok = report["dlq"]["within_tolerance"]
    checks["dlq_fraction_within_tolerance"] = dlq_ok is not False
    if dlq_ok is False:
        dlq = report["dlq"]
        failures.append(
            f"DLQ fraction {dlq['observed_fraction']:.4f} outside "
            f"{EXPECTED_DLQ_FRACTION:.4f} "
            f"±{dlq['tolerance_relative_effective']:.2%} "
            f"({dlq['tolerance_absolute']} events)"
        )

    checks["schedule_volume_matched"] = (
        abs(report["traffic"]["schedule_delta_pct"]) <= SCHEDULE_TOLERANCE * 100
    )
    if not checks["schedule_volume_matched"]:
        failures.append(
            f"generated {report['traffic']['generated']} events vs schedule "
            f"{report['traffic']['expected_by_schedule']}"
        )

    return {
        "passed": not failures,
        "checks": checks,
        "failures": failures,
        "notes": [
            "saturation is recorded per window — an unreached rate is never claimed",
            (
                "the DLQ fraction verdict is skipped below "
                f"{MIN_SAMPLE_FOR_DLQ_VERDICT} accepted events (sampling noise)"
            ),
        ],
    }


# --- runner -----------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


async def _fetch_json(client: httpx.AsyncClient, path: str) -> Any:
    response = await client.get(path)
    response.raise_for_status()
    return response.json()


async def _fetch_metrics(client: httpx.AsyncClient) -> dict[str, Any]:
    response = await client.get("/metrics")
    response.raise_for_status()
    return metrics_snapshot(parse_metrics(response.text))


async def _await_terminal(
    client: httpx.AsyncClient, config: GeneratorConfig
) -> dict[str, Any]:
    """Poll (and nudge) until nothing is deferred or processing.

    Returns the final ``/stats`` body. Raises :class:`GeneratorError` if the
    backlog never clears — a report built against a non-terminal store would be
    meaningless. Three consecutive clean polls guard against a transient gap
    between a deferral commit and the drainer picking it up.
    """
    deadline = time.monotonic() + config.drain_timeout_s
    clean_polls = 0
    stats: dict[str, Any] = {}
    while True:
        try:
            await client.post("/admin/deferred/drain")
        except httpx.HTTPError:
            pass  # the background drainer keeps trying; keep polling
        stats = await _fetch_json(client, "/stats")
        pending = int(stats.get("deferred_pending", 0))
        processing = int(stats.get("events", {}).get("processing", 0))
        if pending == 0 and processing == 0:
            clean_polls += 1
            if clean_polls >= 3:
                return stats
        else:
            clean_polls = 0
        if time.monotonic() > deadline:
            raise GeneratorError(
                f"deferred queue did not drain within {config.drain_timeout_s:g}s "
                f"(deferred_pending={pending}, processing={processing})"
            )
        await asyncio.sleep(0.5)


async def _post_with_transport_retry(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
    record: AttemptRecord,
    config: GeneratorConfig,
) -> httpx.Response:
    """POST one event, re-submitting the *same* event_id on a transport failure.

    A ``ReadError``/``ConnectError`` means the client never saw a response, so
    it cannot know whether the service committed the event. Leaving that
    ambiguous would either fail an otherwise-clean run (as it did: one
    ``ReadError`` in 6,777 requests, silent loss already 0) or, worse, tempt a
    future change to discount client errors from the accounting.

    Re-submission is safe and is the same idempotency the public API promises
    (§7.2): ``event_id`` is fresh per generated event, so a second POST either
    processes it (the first attempt never landed) or answers ``duplicate: true``
    with the row's real terminal state (it did land). Both outcomes are
    evidence, not a guess — which is why ``duplicate`` counts here rather than
    being read as generator error.

    Only *transport* failures are retried. An HTTP status, including a 5xx, is
    a response and is recorded as delivered: it is the service's answer about
    the event, not the network's silence about it. Re-posting on a 5xx would
    turn one event into an unaccountable second submission.

    Bounded: ``TRANSPORT_RETRIES`` re-submissions with a short backoff. If the
    target is genuinely gone the existing abort logic (consecutive/total client
    errors) still fires; this only absorbs the single dropped keepalive that a
    long-lived 200-connection pool produces.
    """
    attempt = 0
    while True:
        try:
            return await client.post("/calls", json=payload)
        except httpx.HTTPError as exc:
            if attempt >= TRANSPORT_RETRIES:
                raise
            if record.first_error is None:
                record.first_error = f"{type(exc).__name__}: {exc}"[:200]
            record.transport_retries += 1
            attempt += 1
            await asyncio.sleep(TRANSPORT_BACKOFF_S * attempt)


async def run_generator(config: GeneratorConfig) -> RunOutcome:
    """Drive the compressed day and return everything the report needs."""
    limits = httpx.Limits(
        max_connections=config.concurrency + 32,
        max_keepalive_connections=config.concurrency + 32,
    )
    notes: list[str] = []
    windows = schedule.windows_upto(config.horizon_s)
    window_starts = [window.start_s for window in windows]
    real_spans: list[tuple[float, float] | None] = [None] * len(windows)

    async with httpx.AsyncClient(
        base_url=config.target_url.rstrip("/"),
        timeout=config.request_timeout_s,
        limits=limits,
        trust_env=False,
    ) as client:
        health = await client.get("/healthz")
        health.raise_for_status()
        server_before = await _fetch_json(client, "/stats")
        metrics_before = await _fetch_metrics(client)
        preexisting = sum(int(value) for value in server_before.get("events", {}).values())
        if preexisting:
            notes.append(
                f"store already held {preexisting} events at start; silent loss is "
                "computed from deltas and any pre-existing backlog was allowed to drain"
            )

        rng = random.Random(config.seed)
        records: list[AttemptRecord] = []
        queue: asyncio.Queue[WorkItem | None] = asyncio.Queue()
        gate = asyncio.Event()  # set when the run must stop early
        abort_reason: list[str] = []
        max_pending = config.max_pending_factor * config.concurrency
        backpressure: Counter = Counter()
        counters = {
            "in_flight": 0,
            "peak_in_flight": 0,
            "pending": 0,
            "peak_pending": 0,
            "delivered": 0,
            "client_errors": 0,
        }
        clock = {"start": time.perf_counter()}

        def elapsed() -> float:
            return time.perf_counter() - clock["start"]

        async def worker() -> None:
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return
                record = item.record
                counters["in_flight"] += 1
                counters["peak_in_flight"] = max(
                    counters["peak_in_flight"], counters["in_flight"]
                )
                record.concurrency_at_start = counters["in_flight"]
                started = time.perf_counter()
                try:
                    response = await _post_with_transport_retry(
                        client, item.payload, record, config
                    )
                    record.latency_ms = (time.perf_counter() - started) * 1000
                    record.http_status = response.status_code
                    try:
                        body = response.json()
                    except ValueError:
                        body = {}
                    record.body_status = body.get("status")
                    record.outcome = body.get("outcome")
                    record.attempts = body.get("attempts")
                    record.retry_count = body.get("retry_count")
                    record.duplicate = bool(body.get("duplicate"))
                    counters["delivered"] += 1
                except httpx.HTTPError as exc:
                    record.error = f"{type(exc).__name__}: {exc}"[:200]
                    counters["client_errors"] += 1
                    attempted = counters["delivered"] + counters["client_errors"]
                    if not gate.is_set() and (
                        (counters["delivered"] == 0 and counters["client_errors"] >= 10)
                        or (attempted >= 50 and counters["client_errors"] > 0.2 * attempted)
                    ):
                        abort_reason.append(
                            f"service unreachable: {counters['client_errors']} client errors "
                            f"of {attempted} requests ({record.error})"
                        )
                        gate.set()
                finally:
                    counters["in_flight"] -= 1
                    counters["pending"] -= 1
                    queue.task_done()

        async def wait_for_capacity(sim_s: float) -> float:
            """Backpressure: hold emission while the queue is at its cap.

            Without this the producer would dump a whole day into memory at high
            speedups and the "run" would become a bulk upload — the schedule's
            timing, and therefore the claim that the ramps were exercised, would
            be fiction. Returns the wall time spent waiting so the caller can
            re-anchor the simulated clock: the run stretches in real time (which
            the report shows) instead of banking a burst to replay later.
            """
            waited_from = time.perf_counter()
            index = min(max(bisect_right(window_starts, sim_s) - 1, 0), len(windows) - 1)
            while counters["pending"] >= max_pending and not gate.is_set():
                backpressure[index] += 1
                await asyncio.sleep(0.002)
            return time.perf_counter() - waited_from

        async def producer() -> None:
            """Emit ticks on the simulated clock, paced by the wall clock.

            SPECS §10 asks for 250 ms *simulated* ticks with Poisson-jittered
            counts. At high speedups one tick is far shorter than the OS sleep
            granularity (0.28 ms at 900x versus a ~15 ms tick on Windows), so
            the loop must not sleep per tick: it evaluates every tick the wall
            clock says is due — several per wake-up — which keeps the schedule
            exact in simulated time while pacing it in real time.

            The clock is anchored to a wall timestamp and re-anchored after any
            backpressure wait, so a stalled service slows the replay down (and
            shows up in the per-window observed rate) instead of causing a
            catch-up flood.
            """
            horizon = config.horizon_s
            last_progress = 0.0
            current_window = -1
            window_opened = 0.0
            sim_s = 0.0
            anchor = time.perf_counter()  # wall instant corresponding to sim_s

            def close_window() -> None:
                if current_window >= 0:
                    real_spans[current_window] = (window_opened, elapsed())

            while sim_s < horizon and not gate.is_set():
                due_sim = (time.perf_counter() - anchor) * config.speedup
                if due_sim < sim_s:
                    # Ahead of schedule: yield until the next tick is due. Below
                    # the platform's sleep granularity (~15 ms on Windows) a
                    # real sleep would oversleep by an order of magnitude, so
                    # short waits yield cooperatively instead: several ticks per
                    # wake-up is fine, *skipping* scheduled ticks is not.
                    wait_s = (sim_s - due_sim) / config.speedup
                    await asyncio.sleep(wait_s if wait_s >= 0.004 else 0)
                    continue
                while sim_s < horizon and sim_s <= due_sim and not gate.is_set():
                    index = min(max(bisect_right(window_starts, sim_s) - 1, 0), len(windows) - 1)
                    if index != current_window:
                        close_window()
                        current_window = index
                        window_opened = elapsed()
                    rate = schedule.rate_at(sim_s)
                    count = poisson(rng, rate * TICK_SIM_S / schedule.MINUTE_S)
                    for _ in range(count):
                        event_sim = min(sim_s + rng.uniform(0.0, TICK_SIM_S), horizon - 1e-6)
                        record = AttemptRecord(event_id=uuid4(), sim_s=event_sim)
                        queue.put_nowait(
                            WorkItem(record, event_payload(record.event_id, event_sim))
                        )
                        records.append(record)
                        counters["pending"] += 1
                        counters["peak_pending"] = max(
                            counters["peak_pending"], counters["pending"]
                        )
                    sim_s = min(sim_s + TICK_SIM_S, horizon)
                    if counters["pending"] >= max_pending:
                        # Re-anchor after the pause so the simulated clock
                        # resumes where it stopped rather than jumping ahead.
                        waited = await wait_for_capacity(sim_s)
                        anchor += waited
                        due_sim = (time.perf_counter() - anchor) * config.speedup
                if elapsed() - last_progress >= 15.0:
                    last_progress = elapsed()
                    print(
                        f"  ... {sim_clock(sim_s)} sim, {len(records)} generated, "
                        f"{counters['delivered']} answered, {counters['pending']} pending",
                        flush=True,
                    )
            close_window()

        clock["start"] = time.perf_counter()
        workers = [
            asyncio.create_task(worker(), name=f"gen-worker-{index}")
            for index in range(config.concurrency)
        ]
        try:
            await producer()
        finally:
            for _ in workers:
                await queue.put(None)
        if abort_reason:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise GeneratorError(abort_reason[0])
        await queue.join()
        await asyncio.gather(*workers)

        observed_real_s = elapsed()
        server_after = await _await_terminal(client, config)
        metrics_after = await _fetch_metrics(client)
        breaker_final = await _fetch_json(client, "/admin/circuit-breaker")

    return RunOutcome(
        config=config,
        records=records,
        real_duration_s=observed_real_s,
        peak_pending=counters["peak_pending"],
        window_spans=real_spans,
        backpressure=backpressure,
        server_before=server_before,
        server_after=server_after,
        breaker_final=breaker_final,
        metrics_before=metrics_before,
        metrics_after=metrics_after,
        notes=notes,
    )


# --- human summary ----------------------------------------------------------


def render_summary(report: dict[str, Any]) -> str:
    """Reviewer-facing text rendering of the JSON report."""
    compression = report["compression"]
    traffic = report["traffic"]
    requests = report["requests"]
    silence = report["silent_loss"]
    dlq = report["dlq"]
    verdict = report["verdict"]
    latency = report["latency_ms"]["client_observed"]

    lines = [
        (
            f"Appendix C replay — {compression['sim_hours']:g} simulated hours"
            f" at {compression['speedup']:g}x"
        ),
        f"  {compression['statement']}",
        (
            "  nominal "
            f"{compression['real_seconds_nominal'] / 60:.1f} real min, observed "
            f"{compression['real_seconds_observed'] / 60:.2f} real min"
        ),
        "",
        (
            "  window                     target sim/real      achieved sim/real(obs)"
            "   generated  peak  sat"
        ),
    ]
    for row in report["windows"]:
        observed = row["achieved_calls_per_min_real_observed"]
        observed_text = f"{observed:7.1f}" if observed is not None else "      -"
        lines.append(
            f"  {row['label']:<24}"
            f"{row['target_calls_per_min_sim']:>7.2f}/{row['target_calls_per_min_real']:<8.0f}"
            f"{row['achieved_calls_per_min_sim']:>10.2f}/{observed_text}  "
            f"{row['generated']:>9}  {row['peak_in_flight']:>4}  "
            f"{'YES' if row['saturated'] else '-'}"
        )
    dlq_verdict = (
        "PASS"
        if dlq["within_tolerance"]
        else "N/A" if dlq["within_tolerance"] is None else "FAIL"
    )
    lines += [
        "",
        (
            f"  generated {traffic['generated']} events"
            f" (schedule {traffic['expected_by_schedule']},"
            f" {traffic['schedule_delta_pct']:+.2f}%),"
            f" peak in-flight {traffic['peak_in_flight']}/{traffic['concurrency']},"
            f" peak pending {traffic['peak_pending']}"
        ),
        (
            f"  requests: processed {requests['processed']},"
            f" failed_downstream {requests['failed_downstream']},"
            f" deferred {requests['deferred']}, duplicate {requests['duplicate']},"
            f" http_errors {requests['http_errors']},"
            f" client_errors {requests['client_errors']}"
        ),
        (
            f"  retries: {report['retries']['retry_count_total']}"
            f" across {report['retries']['calls_with_retries']} calls"
            f" (max attempts {report['retries']['max_attempts_observed']})"
        ),
        (
            f"  transport: {requests['transport_retries']} re-submissions"
            f" across {requests['calls_with_transport_retries']} events,"
            f" {requests['self_duplicates']} answered duplicate"
        ),
        (
            f"  breaker: opened {report['circuit_breaker']['opened_count']}x,"
            f" final {report['circuit_breaker']['final'].get('state')}"
        ),
        (
            f"  DLQ: {dlq['created_this_run']} created"
            f" ({dlq['observed_fraction']:.3%} vs {dlq['expected_fraction']:.3%}"
            f" expected) -> {dlq_verdict}"
        ),
        (
            f"  latency (client): p50 {latency['p50']} ms,"
            f" p90 {latency['p90']} ms, p99 {latency['p99']} ms"
        ),
        (
            f"  silent loss: {silence['count']}"
            f" (accepted {silence['accepted_unique']},"
            f" terminal delta {silence['terminal_delta']},"
            f" deferred at end {silence['deferred_pending_at_end']})"
        ),
        "",
        f"  verdict: {'PASS' if verdict['passed'] else 'FAIL'}",
    ]
    for failure in verdict["failures"]:
        lines.append(f"    - {failure}")
    return "\n".join(lines)


# --- CLI --------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.traffic_gen",
        description="Replay the Appendix C day against the inference service.",
    )
    parser.add_argument(
        "--target-url",
        default=os.environ.get("TARGET_URL", "http://localhost:8000"),
        help="inference base URL (default: $TARGET_URL or http://localhost:8000)",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        default=float(os.environ.get("SPEEDUP", "60")),
        help="time compression factor (default: $SPEEDUP or 60)",
    )
    parser.add_argument(
        "--duration-sim-hours",
        type=float,
        default=float(os.environ.get("DURATION_SIM_HOURS", "24")),
        help="simulated hours to replay, 0 < h <= 24 (default: $DURATION_SIM_HOURS or 24)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("CONCURRENCY", "200")),
        help="max in-flight requests (default: $CONCURRENCY or 200)",
    )
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for the Poisson jitter")
    parser.add_argument(
        "--out", type=Path, default=None, help="report path (default: reports/run_<ts>.json)"
    )
    parser.add_argument(
        "--drain-timeout-s",
        type=float,
        default=300.0,
        help="max wait for the deferred queue to drain after the run (default: 300)",
    )
    parser.add_argument(
        "--request-timeout-s",
        type=float,
        default=REQUEST_TIMEOUT_S,
        help="per-request client timeout (default: 30)",
    )
    return parser


def _validate(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    try:
        schedule.windows_upto(args.duration_sim_hours * 3600)
    except ValueError as exc:
        parser.error(f"--duration-sim-hours: {exc}")
    if args.speedup <= 0:
        parser.error("--speedup must be positive")
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate(parser, args)

    config = GeneratorConfig(
        target_url=args.target_url,
        speedup=args.speedup,
        horizon_s=args.duration_sim_hours * 3600,
        concurrency=args.concurrency,
        seed=args.seed,
        out_path=args.out or default_report_path(),
        drain_timeout_s=args.drain_timeout_s,
        request_timeout_s=args.request_timeout_s,
    )
    print(
        f"traffic-gen: {args.duration_sim_hours:g} sim-h at {args.speedup:g}x -> "
        f"{schedule.compressed_duration_s(config.horizon_s, config.speedup) / 60:.1f} "
        f"real min against {config.target_url}",
        flush=True,
    )
    try:
        outcome = asyncio.run(run_generator(config))
    except (GeneratorError, httpx.HTTPError) as exc:
        print(f"traffic-gen: run aborted — {exc}", file=sys.stderr)
        return 1

    report = build_report(outcome)
    config.out_path.parent.mkdir(parents=True, exist_ok=True)
    config.out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(render_summary(report))
    print(f"\nwrote {config.out_path}")
    return 0 if report["verdict"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
