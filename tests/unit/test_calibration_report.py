"""Calibration report generator (M1-T06 follow-up).

The deliverable requires ``reports/latency_calibration.md``, but generating it
must not be a side effect of running the suite: a routine ``pytest -q`` has to
leave the working tree untouched. These tests pin that split — measuring is
pure, rendering is pure, and only :func:`tools.calibration_report.main` writes.
"""

from __future__ import annotations

from pathlib import Path

from tools import calibration_report

REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "latency_calibration.md"


def test_measure_scores_bands_without_touching_the_working_tree() -> None:
    before = REPORT_PATH.read_bytes()
    measured, verdicts = calibration_report.measure(n=20_000)
    after = REPORT_PATH.read_bytes()

    assert set(measured) == {"p50", "p90", "p99"}
    assert set(verdicts) == {"p50", "p90", "p99"}
    # Measuring is pure: no artifact is written as a side effect.
    assert after == before, "measure() must not write the report"


def test_render_emits_the_documented_table() -> None:
    measured = {"p50": 300.0, "p90": 700.0, "p99": 1200.0}
    verdicts = {name: "PASS" for name in measured}

    text = calibration_report.render(measured, verdicts, n=50_000)

    assert text.startswith("# Latency calibration")
    assert "n=50,000, mu=5.7, sigma=0.62" in text
    assert "| p50 | 300.0 | 300 | ±10% | PASS |" in text
    assert "| p90 | 700.0 | 700 | ±15% | PASS |" in text
    assert "| p99 | 1200.0 | 1200 | ±25% | PASS |" in text
    assert "make calibration-report" in text


def test_render_marks_out_of_band_values_as_fail() -> None:
    measured = {"p50": 999.0, "p90": 700.0, "p99": 1200.0}
    verdicts = {
        name: calibration_report.band_verdict(value, *calibration_report.TARGETS[name])
        for name, value in measured.items()
    }

    text = calibration_report.render(measured, verdicts, n=50_000)

    assert "| p50 | 999.0 | 300 | ±10% | FAIL |" in text


def test_main_writes_the_report_to_the_requested_path(tmp_path, monkeypatch) -> None:
    out = tmp_path / "nested" / "calibration.md"
    monkeypatch.setattr("sys.argv", ["calibration_report", "--out", str(out), "--n", "20000"])

    assert calibration_report.main() == 0

    text = out.read_text(encoding="utf-8")
    assert text.startswith("# Latency calibration")
    assert "| p50 |" in text and "| p99 |" in text


def test_main_exits_non_zero_when_a_band_fails(tmp_path, monkeypatch) -> None:
    """A refresh doubles as a re-verification of the shipped sigma."""
    out = tmp_path / "calibration.md"
    monkeypatch.setattr("sys.argv", ["calibration_report", "--out", str(out), "--n", "20000"])
    monkeypatch.setattr(
        calibration_report, "TARGETS", {"p50": (1.0, 0.01), "p90": (700.0, 0.15),
                                        "p99": (1200.0, 0.25)}
    )

    assert calibration_report.main() == 1
    assert "FAIL" in out.read_text(encoding="utf-8")
