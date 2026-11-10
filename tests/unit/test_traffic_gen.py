"""Generator unit tests (M5-T04; SPECS §10, §11).

No service required: everything here is schedule arithmetic, report assembly
over synthetic records, and the saturation/verdict logic. The live proof is
``tests/integration/test_compressed_day.py`` (M5-T05).
"""

from __future__ import annotations

import itertools
import math
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from tools import schedule
from tools.traffic_gen import (
    EXPECTED_DLQ_FRACTION,
    EXPECTED_FAILURE_RATE,
    SATURATION_TOLERANCE,
    AttemptRecord,
    GeneratorConfig,
    RunOutcome,
    build_report,
    default_report_path,
    event_payload,
    is_saturated,
    poisson,
    render_summary,
    sim_timestamp,
    window_rows,
)


def hms(hours: int, minutes: int = 0, seconds: int = 0) -> float:
    return hours * 3600.0 + minutes * 60.0 + seconds


# --- M5-T01: schedule model -------------------------------------------------


@pytest.mark.parametrize(
    ("sim_s", "expected"),
    [
        (hms(0), 10.0),
        (hms(4), 10.0),
        (hms(8) - 0.001, 10.0),
        (hms(8), 10.0),  # ramp starts at its start rate
        (hms(8, 7, 30), 30.0),  # midpoint of the 10 -> 50 ramp
        (hms(8, 15) - 0.001, pytest.approx(50.0, abs=0.01)),
        (hms(8, 15), 50.0),  # boundary resolves to the next window
        (hms(9), 50.0),
        (hms(10), 10.0),
        (hms(12), 10.0),
        (hms(14), 10.0),
        (hms(14, 7, 30), 30.0),
        (hms(14, 15), 50.0),
        (hms(16) - 0.001, pytest.approx(50.0, abs=0.01)),
        (hms(16), 10.0),
        (hms(20), 10.0),
        (hms(24), 10.0),
    ],
)
def test_rate_at_boundaries_and_midpoints(sim_s: float, expected: float) -> None:
    assert schedule.rate_at(sim_s) == expected


def test_schedule_is_contiguous_with_no_gaps_or_overlaps() -> None:
    windows = schedule.WINDOWS
    assert windows[0].start_s == 0.0
    assert windows[-1].end_s == schedule.DAY_SECONDS
    for left, right in itertools.pairwise(windows):
        assert left.end_s == right.start_s, "windows must tile the day exactly"
    # The only rate steps are the two peaks ending (10:00, 16:00): a ramp hands
    # over to a hold at the ramp's end rate, so those boundaries are continuous.
    steps = (hms(10), hms(16))
    for boundary in (w.start_s for w in windows[1:] if w.start_s not in steps):
        assert schedule.rate_at(boundary - 1e-6) == pytest.approx(schedule.rate_at(boundary))
    assert schedule.rate_at(hms(10) - 1e-6) == pytest.approx(50.0)
    assert schedule.rate_at(hms(10)) == 10.0
    assert schedule.rate_at(hms(16) - 1e-6) == pytest.approx(50.0)
    assert schedule.rate_at(hms(16)) == 10.0


def test_day_volume_matches_appendix_c_arithmetic() -> None:
    # Appendix C / SPECS §10: 4,800 + 450 + 5,250 + 2,400 + 450 + 5,250 + 4,800.
    assert schedule.EXPECTED_CALLS_PER_DAY == pytest.approx(23_400.0)
    assert schedule.expected_calls() == pytest.approx(23_400.0)
    assert sum(window.expected_calls for window in schedule.WINDOWS) == pytest.approx(23_400.0)


def test_partial_horizon_and_windows_upto_clip_correctly() -> None:
    two_hours = schedule.expected_calls(0.0, hms(2))
    assert two_hours == pytest.approx(1_200.0)  # 2 h × 10/min
    clipped = schedule.windows_upto(hms(8, 7, 30))
    assert len(clipped) == 2
    assert clipped[-1].end_s == hms(8, 7, 30)
    assert clipped[-1].rate_at(clipped[-1].end_s) == pytest.approx(30.0)
    with pytest.raises(ValueError):
        schedule.windows_upto(0.0)
    with pytest.raises(ValueError):
        schedule.windows_upto(schedule.DAY_SECONDS + 1)


def test_rate_at_rejects_out_of_range_time() -> None:
    with pytest.raises(ValueError):
        schedule.rate_at(-1.0)
    with pytest.raises(ValueError):
        schedule.rate_at(schedule.DAY_SECONDS + 1.0)


# --- M5-T01: compression ----------------------------------------------------


def test_compression_amplification_at_60x() -> None:
    assert schedule.compressed_rate(10.0, 60) == 600.0  # baseline real cpm
    assert schedule.compressed_rate(50.0, 60) == 3_000.0  # peak real cpm
    assert schedule.compressed_duration_s(schedule.DAY_SECONDS, 60) == pytest.approx(1_440.0)
    assert schedule.compressed_duration_s(schedule.DAY_SECONDS, 60) / 60 == pytest.approx(24.0)


def test_compression_rejects_non_positive_speedup() -> None:
    with pytest.raises(ValueError):
        schedule.compressed_rate(10.0, 0)
    with pytest.raises(ValueError):
        schedule.compressed_duration_s(3600.0, -1)


# --- M5-T02 helpers ---------------------------------------------------------


def test_sim_timestamp_is_sim_clock_rfc3339() -> None:
    assert sim_timestamp(0.0) == "2026-09-01T00:00:00.000Z"
    assert sim_timestamp(hms(8, 7, 30)) == "2026-09-01T08:07:30.000Z"


def test_event_payload_is_schema_shaped_and_varies() -> None:
    from app.models import CallEvent

    payload = event_payload(uuid4(), hms(9, 3, 12))
    event = CallEvent.model_validate(payload)  # Appendix A validation, for free
    assert event.campaign_id == "CAMP-2026-09-A"
    assert isinstance(event.event_id, type(uuid4()))

    buckets = {event_payload(uuid4(), 0.0)["borrower_bucket"] for _ in range(64)}
    assert len(buckets) > 1, "borrower_bucket must not be constant"


def test_poisson_tracks_lambda() -> None:
    import random

    rng = random.Random(7)
    lam = 0.2
    draws = [poisson(rng, lam) for _ in range(20_000)]
    assert sum(draws) / len(draws) == pytest.approx(lam, rel=0.1)
    assert all(value >= 0 for value in draws)
    assert poisson(rng, 0.0) == 0


# --- M5-T03: saturation logic ----------------------------------------------


def test_saturation_flags_cap_backpressure_and_shortfall() -> None:
    base = {
        "target_sim_cpm": 50.0,
        "achieved_sim_cpm": 50.0,
        "peak_in_flight": 5,
        "concurrency": 200,
        "target_real_cpm": 3_000.0,
        "observed_real_cpm": 3_000.0,
    }
    assert not is_saturated(**base)
    assert is_saturated(**{**base, "peak_in_flight": 200})  # cap hit
    assert is_saturated(**{**base, "backpressure_waits": 3})
    assert is_saturated(**{**base, "achieved_sim_cpm": 20.0})  # simulated shortfall
    assert is_saturated(**{**base, "observed_real_cpm": 1_000.0})  # real shortfall
    # Exactly at the tolerance edge is not saturation.
    edge = 50.0 * (1 - SATURATION_TOLERANCE)
    assert not is_saturated(**{**base, "achieved_sim_cpm": edge})


def test_window_rows_report_target_and_achieved_per_window() -> None:
    windows = schedule.windows_upto(hms(10))
    records = []
    for _ in range(100):
        records.append(AttemptRecord(event_id=uuid4(), sim_s=hms(9), http_status=200))
    rows = window_rows(
        windows,
        records,
        speedup=60.0,
        concurrency=200,
        real_spans=[(0.0, 10.0)] * len(windows),
        backpressure=Counter(),
    )
    assert [row["label"] for row in rows] == [window.label for window in windows]
    peak_row = next(row for row in rows if row["label"] == "08:15-10:00 peak")
    assert peak_row["delivered"] == 100
    assert peak_row["target_calls_per_min_sim"] == 50.0
    assert peak_row["target_calls_per_min_real"] == 3_000.0
    assert peak_row["real_elapsed_s"] == 10.0
    assert peak_row["achieved_calls_per_min_real_observed"] == pytest.approx(600.0)


# --- M5-T03: report assembly -----------------------------------------------


def _config(tmp_path: Path, *, horizon_s: float = schedule.DAY_SECONDS) -> GeneratorConfig:
    return GeneratorConfig(
        target_url="http://inference.test",
        speedup=60.0,
        horizon_s=horizon_s,
        concurrency=200,
        seed=11,
        out_path=tmp_path / "run.json",
    )


def _outcome(
    config: GeneratorConfig,
    records: list[AttemptRecord],
    *,
    accepted: int,
    deferred_pending: int = 0,
    dlq: int | None = None,
) -> RunOutcome:
    """Synthetic run: ``accepted`` events, of which ``dlq`` died and some deferred.

    ``dlq=None`` (the default) means "the Appendix B expectation" — 0.8% of
    accepted events exhausted every attempt and were dead-lettered.
    """
    dlq_total = round(accepted * EXPECTED_DLQ_FRACTION) if dlq is None else dlq
    terminal = accepted - int(dlq_total) - deferred_pending
    assert terminal >= 0, "synthetic scenario is internally inconsistent"
    server_after = {
        "events": {
            "deferred": deferred_pending,
            "processing": 0,
            "processed": terminal,
            "failed_downstream": int(dlq_total),
        },
        "dead_letter": {"pending": int(dlq_total), "replayed": 0},
        "deferred_pending": deferred_pending,
        "latency_percentiles": {"p50": 300.0, "p90": 700.0, "p99": 1200.0},
    }
    body_statuses = Counter(record.body_status for record in records)
    assert sum(body_statuses.values()) == len(records)
    return RunOutcome(
        config=config,
        records=records,
        real_duration_s=schedule.compressed_duration_s(config.horizon_s, config.speedup),
        peak_pending=12,
        window_spans=[(0.0, 5.0)] * len(schedule.windows_upto(config.horizon_s)),
        backpressure=Counter(),
        server_before={
            "events": {"deferred": 0, "processing": 0, "processed": 0, "failed_downstream": 0},
            "dead_letter": {"pending": 0, "replayed": 0},
            "deferred_pending": 0,
        },
        server_after=server_after,
        breaker_final={"state": "closed"},
        metrics_before={
            "retries_total": 0.0,
            "circuit_transitions_total": {},
            "pipeline_attempts_total": {},
            "calls_total": {},
            "deferred_expired_total": 0.0,
        },
        metrics_after={
            "retries_total": 420.0,
            "circuit_transitions_total": {"closed->open": 2.0},
            "pipeline_attempts_total": {"success": 30_000.0, "error": 800.0, "timeout": 0.0},
            "calls_total": {"connected->processed": 30_000.0},
            "deferred_expired_total": 0.0,
        },
    )


def _records(count: int, *, start_sim: float = hms(9), span_s: float = 3600.0) -> list[AttemptRecord]:
    """``count`` delivered records spread across ``[start_sim, start_sim+span_s)``."""
    step = span_s / count
    records: list[AttemptRecord] = []
    for index in range(count):
        record = AttemptRecord(
            event_id=uuid4(),
            sim_s=start_sim + index * step,
            http_status=200,
            body_status="processed",
            outcome="connected",
            attempts=1,
            retry_count=0,
            latency_ms=300.0 + index % 50,
        )
        records.append(record)
    return records


def _scheduled_records(config: GeneratorConfig, *, failed: int = 0) -> list[AttemptRecord]:
    """Records at exactly the volume the schedule expects over the horizon.

    ``failed`` marks that many records as terminal failures (the DLQ path), so
    the client-side tallies agree with the synthetic ``/stats`` body.
    """
    count = round(schedule.expected_calls(0.0, config.horizon_s))
    records = _records(count, start_sim=0.0, span_s=config.horizon_s - 1.0)
    for record in records[:failed]:
        record.body_status = "failed_downstream"
        record.outcome = "failed_downstream"
        record.attempts = 3
        record.retry_count = 2
    return records


def test_report_contains_every_spec_mandated_field(tmp_path: Path) -> None:
    config = _config(tmp_path, horizon_s=hms(10))
    accepted = round(schedule.expected_calls(0.0, config.horizon_s))
    assert accepted == 10_500
    expected_dlq = round(accepted * EXPECTED_DLQ_FRACTION)  # 84
    records = _scheduled_records(config, failed=expected_dlq)
    report = build_report(_outcome(config, records, accepted=accepted))

    # §10 checklist, key by key.
    assert report["traffic"]["generated"] == accepted
    assert report["compression"]["speedup"] == 60.0
    assert report["compression"]["baseline"]["real_calls_per_min"] == 600.0
    assert report["compression"]["peak"]["real_calls_per_min"] == 3_000.0
    assert report["compression"]["real_seconds_nominal"] == 600.0
    assert report["compression"]["sim_hours"] == 10.0
    for row in report["windows"]:
        assert {
            "target_calls_per_min_sim",
            "target_calls_per_min_real",
            "achieved_calls_per_min_sim",
            "peak_in_flight",
            "saturated",
        } <= set(row)
        assert row["delivered"] > 0
    assert report["requests"]["processed"] == accepted - expected_dlq
    assert report["requests"]["failed_downstream"] == expected_dlq
    assert "deferred" in report["requests"]
    assert report["retries"]["retry_count_total"] == 2 * expected_dlq  # 3 attempts each
    assert report["retries"]["calls_with_retries"] == expected_dlq
    assert report["retries"]["max_attempts_observed"] == 3
    assert report["retries"]["server_retries_total"] == 420.0
    assert report["circuit_breaker"]["opened_count"] == 2
    assert report["dlq"]["created_this_run"] == 84  # 0.8% of 10,500
    assert report["latency_ms"]["client_observed"]["p50"] is not None
    assert report["silent_loss"]["count"] == 0
    assert report["server_stats"]["deferred_pending"] == 0
    assert report["verdict"]["passed"] is True


def test_silent_loss_is_derived_not_asserted(tmp_path: Path) -> None:
    """An accepted event with neither a terminal row nor a deferral is loss."""
    config = _config(tmp_path)
    records = _records(1_000)
    outcome = _outcome(config, records, accepted=1_000)
    outcome.server_after["events"]["processed"] = 994  # six vanished
    outcome.server_after["events"]["failed_downstream"] = 0
    outcome.server_after["dead_letter"] = {"pending": 0, "replayed": 0}
    report = build_report(outcome)
    assert report["silent_loss"]["count"] == 6
    assert report["verdict"]["passed"] is False
    assert any("silent loss" in failure for failure in report["verdict"]["failures"])


def test_undrained_deferred_queue_fails_the_verdict(tmp_path: Path) -> None:
    config = _config(tmp_path)
    records = _records(1_000)
    report = build_report(_outcome(config, records, accepted=1_000, deferred_pending=7))
    assert report["silent_loss"]["count"] == 0  # still accounted, just not terminal
    assert report["verdict"]["checks"]["deferred_queue_drained"] is False
    assert report["verdict"]["passed"] is False


def test_dlq_fraction_verdict_skips_small_samples_and_flags_bad_large_ones(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, horizon_s=hms(10))
    records = _scheduled_records(config)

    small = build_report(_outcome(config, _records(500), accepted=500, dlq=None))
    assert small["dlq"]["within_tolerance"] is None

    large_bad = build_report(_outcome(config, records, accepted=len(records), dlq=600))
    assert large_bad["dlq"]["within_tolerance"] is False
    assert large_bad["verdict"]["passed"] is False

    large_good = build_report(_outcome(config, records, accepted=len(records)))
    assert large_good["dlq"]["within_tolerance"] is True
    assert large_good["dlq"]["observed_fraction"] == pytest.approx(
        EXPECTED_DLQ_FRACTION, abs=1e-4
    )


def test_saturation_is_reported_per_window(tmp_path: Path) -> None:
    config = _config(tmp_path, horizon_s=hms(10))
    records = _scheduled_records(config)
    outcome = _outcome(config, records, accepted=len(records))
    # Wall-clock spans 10x the nominal compression: the delivered real rate is
    # far below target, which is exactly what the saturation flag must expose.
    outcome.window_spans = [(0.0, 1_000.0)] * len(outcome.window_spans)
    report = build_report(outcome)
    assert all(row["saturated"] for row in report["windows"])
    assert all(
        row["achieved_calls_per_min_real_observed"] < row["target_calls_per_min_real"]
        for row in report["windows"]
    ), "the observed rate is derived from the real span, not the nominal one"


def test_report_verdict_flags_client_errors(tmp_path: Path) -> None:
    config = _config(tmp_path)
    records = _records(50)
    records.append(AttemptRecord(event_id=uuid4(), sim_s=0.0, error="ConnectError: refused"))
    outcome = _outcome(config, records, accepted=50)
    report = build_report(outcome)
    assert report["requests"]["client_errors"] == 1
    assert report["verdict"]["passed"] is False
    assert report["requests"]["client_error_reasons"] == {"ConnectError": 1}


def test_report_rejects_schedule_volume_mismatch(tmp_path: Path) -> None:
    config = _config(tmp_path, horizon_s=hms(2))
    report = build_report(_outcome(config, _records(10), accepted=10))
    # Expected for 2 hours is 1,200 — generating 10 is a volume shortfall.
    assert report["verdict"]["checks"]["schedule_volume_matched"] is False


def test_transport_resolved_duplicate_is_not_silent_loss(tmp_path: Path) -> None:
    """A re-submitted event answered ``duplicate`` still counts as one event.

    This is the accounting rule that made a real 6,777-request run worth
    fixing rather than excusing: without it, re-submitting an ambiguous POST
    would subtract the event from ``accepted_unique`` and manufacture silent
    loss (or, in the other direction, hide a genuine duplicate).
    """
    config = _config(tmp_path)
    records = _records(1_000)
    # One event needed two re-submissions and came back as our own duplicate.
    retried = records[0]
    retried.transport_retries = 2
    retried.first_error = "ReadError: connection reset by peer"
    retried.duplicate = True
    outcome = _outcome(config, records, accepted=1_000)
    report = build_report(outcome)

    assert report["requests"]["duplicate"] == 1
    assert report["requests"]["self_duplicates"] == 1
    assert report["requests"]["accepted_unique"] == 1_000
    assert report["requests"]["transport_retries"] == 2
    assert report["requests"]["calls_with_transport_retries"] == 1
    assert report["requests"]["transport_error_reasons"] == {"ReadError": 1}
    assert report["silent_loss"]["count"] == 0, "a resolved retry must not read as loss"
    assert report["silent_loss"]["accepted_unique"] == 1_000


def test_duplicate_of_another_event_still_reduces_the_unique_count(tmp_path: Path) -> None:
    """The other direction: a plain duplicate is a second event that did not exist."""
    config = _config(tmp_path)
    records = _records(1_000)
    records[0].duplicate = True  # no transport retry => a genuine double submit
    outcome = _outcome(config, records, accepted=1_000)
    report = build_report(outcome)
    assert report["requests"]["self_duplicates"] == 0
    assert report["requests"]["accepted_unique"] == 999


def test_unresolved_client_error_names_the_transport_retry_bound(tmp_path: Path) -> None:
    """The failure message must say what was tried, not just that it failed."""
    from tools.traffic_gen import TRANSPORT_RETRIES

    config = _config(tmp_path)
    records = _records(50)
    records.append(AttemptRecord(event_id=uuid4(), sim_s=0.0, error="ReadError: closed"))
    report = build_report(_outcome(config, records, accepted=50))
    assert report["verdict"]["passed"] is False
    failure = next(f for f in report["verdict"]["failures"] if "never resolved" in f)
    assert str(TRANSPORT_RETRIES) in failure
    assert "ReadError" in failure


def test_render_summary_is_text_and_mentions_verdict(tmp_path: Path) -> None:
    config = _config(tmp_path, horizon_s=hms(10))
    records = _scheduled_records(config)
    report = build_report(_outcome(config, records, accepted=len(records)))
    summary = render_summary(report)
    assert "Appendix C replay" in summary
    assert "60x" in summary
    assert "verdict: PASS" in summary
    assert "silent loss: 0" in summary


def test_default_report_path_shape() -> None:
    path = default_report_path(datetime(2026, 11, 10, 12, 0, tzinfo=UTC))
    assert path.parent.name == "reports"
    assert path.name.startswith("run_20261110T120000Z")
    assert path.suffix == ".json"


def test_expectation_constants_are_the_documented_ones() -> None:
    assert EXPECTED_FAILURE_RATE == 0.20
    assert EXPECTED_DLQ_FRACTION == pytest.approx(0.008)
    assert math.isclose(EXPECTED_DLQ_FRACTION, 0.2**3)
