"""Compressed-day acceptance test (M5-T05; SPECS §10, §11; PLAN M5 DoD).

The sprint's scoring criterion, machine-checked: one command plays a
compressed Appendix C day against a live inference service, and the resulting
report must show **100% terminal accounting after the deferred queue has
drained, zero silent losses, and a DLQ near the 0.8% expectation**.

Three real processes are involved — a live Appendix B mock, a live inference
server on loopback ports, and the generator as its own interpreter
(``python -m tools.traffic_gen``), which is exactly how the README tells a
reviewer to run it. The run uses the sanctioned **60x** compression and a
bounded 8.75-hour horizon so the test costs ~9 real minutes while still
crossing the 08:00 ramp and the 08:15 peak hold, and generating ~6,750
events — enough for the DLQ fraction verdict to be statistically meaningful
rather than skipped as a small sample. A bounded horizon is a legitimate demo
setting SPECS §10 permits; the full 24-hour day is the committed sample report
(M5-T06).

Why 60x and not a more extreme speedup: this layered stack (one Python
process serving the API, one serving the mock, SQLite between them)
sustains roughly 60-70 calls/s. SPECS §10's 60x profile peaks at
50 calls/min x 60 = **3,000 calls/min = 50 calls/s**, which fits with headroom
— measured peak in-flight is ~21 of the 200 permitted. Driving the same
schedule harder does not prove more about the service; it just converts the
run into an overload scenario where attempts exceed the 2.5 s per-attempt
timeout, the breaker trips, and the accounting being asserted here is masked
by a deferral storm. Saturation is still detected and reported honestly
(see ``test_saturation_flag_is_present_and_consistent``), so if this box were
too slow the run would say so rather than quietly under-deliver.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from app.config import Settings
from app.main import create_app
from mock.config import MockSettings
from mock.main import create_mock_app
from tools import schedule

REPO_ROOT = Path(__file__).resolve().parents[2]
STARTUP_TIMEOUT_S = 15.0
SIM_HORIZON_H = 8.75  # both 08:00 ramps and the 08:15 peak, ending mid-hold
SPEEDUP = 60.0  # the sanctioned profile: 3,000 calls/min peak = 50 calls/s
CONCURRENCY = 200
GENERATOR_TIMEOUT_S = 900

# Sub-millisecond success latency, as every HTTP-level test in this suite
# uses. The 300 ms Appendix B default is calibrated and asserted separately
# (M1-T06); here it would only make the run slower than its fixed horizon.
FAST_LATENCY_MU = -3.0


def reference_event(event_id: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
    }


class LiveServer:
    """A uvicorn server on an ephemeral loopback port, in a daemon thread.

    Same shape as ``tests/integration/test_replay_e2e.py``: the socket is bound
    before serving so the URL is known without racing uvicorn's bind, and
    ``stop`` is an orderly shutdown that runs the lifespan exit.
    """

    def __init__(self, factory: Any) -> None:
        self._factory = factory
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self.url = ""
        self._server: uvicorn.Server | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(STARTUP_TIMEOUT_S):
            raise RuntimeError("live server thread did not start")
        if self._error is not None:
            raise RuntimeError(f"live server failed to start: {self._error!r}")
        self._wait_for_health()

    def _run(self) -> None:
        try:
            app: FastAPI = self._factory()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", 0))
            sock.listen(512)
            self.url = f"http://127.0.0.1:{sock.getsockname()[1]}"
            self._server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
            self._ready.set()
            try:
                self._server.run(sockets=[sock])
            finally:
                sock.close()
        except Exception as exc:  # noqa: BLE001 - re-raised on the test thread
            self._error = exc
            self._ready.set()

    def _wait_for_health(self) -> None:
        deadline = time.monotonic() + STARTUP_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                response = httpx.get(f"{self.url}/healthz", timeout=1.0, trust_env=False)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.02)
        raise RuntimeError(f"server at {self.url} never answered /healthz")

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        self._thread.join(timeout=STARTUP_TIMEOUT_S)
        assert not self._thread.is_alive(), "live server did not shut down"


class DayStack:
    """Mock + inference servers, and a generator run against them."""

    def __init__(self, tmp_path: Path) -> None:
        self.db_path = tmp_path / "day.db"
        self.mock = LiveServer(
            lambda: create_mock_app(MockSettings(latency_mu=FAST_LATENCY_MU))
        )
        self.settings = Settings(
            db_path=str(self.db_path),
            pipeline_backend="http",
            pipeline_base_url=self.mock.url,
        )
        self.inference = LiveServer(lambda: create_app(self.settings))
        self.http = httpx.Client(base_url=self.inference.url, timeout=30.0, trust_env=False)
        self.report_path = tmp_path / "reports" / "run_test.json"

    def run_generator(self, *extra: str) -> subprocess.CompletedProcess[str]:
        """Run ``python -m tools.traffic_gen`` as its own process."""
        command = [
            sys.executable,
            "-m",
            "tools.traffic_gen",
            "--target-url",
            self.inference.url,
            "--speedup",
            str(SPEEDUP),
            "--duration-sim-hours",
            str(SIM_HORIZON_H),
            "--concurrency",
            str(CONCURRENCY),
            "--seed",
            "4242",  # deterministic Poisson jitter for the schedule-volume check
            "--out",
            str(self.report_path),
            "--drain-timeout-s",
            "180",
            *extra,
        ]
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=GENERATOR_TIMEOUT_S,
            check=False,
        )

    def report(self) -> dict[str, Any]:
        return json.loads(self.report_path.read_text(encoding="utf-8"))

    def stats(self) -> dict[str, Any]:
        return self.http.get("/stats").json()

    def close(self) -> None:
        self.http.close()
        self.inference.stop()
        self.mock.stop()


@pytest.fixture(scope="module")
def day(tmp_path_factory) -> DayStack:
    stack = DayStack(tmp_path_factory.mktemp("compressed_day"))
    try:
        yield stack
    finally:
        stack.close()


@pytest.fixture(scope="module")
def completed(day: DayStack) -> dict[str, Any]:
    """Run the compressed day once; every assertion reads the same report."""
    result = day.run_generator()
    assert result.returncode == 0, (
        f"generator failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert day.report_path.exists(), "the run must write its report file"
    return day.report()


def test_report_is_written_with_every_mandated_field(completed: dict[str, Any]) -> None:
    assert completed["compression"]["speedup"] == SPEEDUP
    assert completed["compression"]["sim_hours"] == pytest.approx(SIM_HORIZON_H)
    assert "simulated hour" in completed["compression"]["statement"]
    assert completed["traffic"]["generated"] > 0
    assert completed["traffic"]["peak_in_flight"] <= CONCURRENCY, "concurrency cap exceeded"
    assert completed["windows"], "per-window throughput table is empty"
    for row in completed["windows"]:
        assert row["target_calls_per_min_sim"] > 0
        assert row["delivered"] > 0, f"window {row['label']!r} delivered nothing"
        assert isinstance(row["saturated"], bool)
    for key in (
        "requests",
        "retries",
        "circuit_breaker",
        "dlq",
        "latency_ms",
        "silent_loss",
        "server_stats",
        "verdict",
    ):
        assert key in completed, f"report is missing {key!r}"


def test_every_accepted_event_is_terminal_and_nothing_is_silently_lost(
    completed: dict[str, Any], day: DayStack
) -> None:
    """The M5 DoD: terminal accounting once the deferred queue has drained."""
    silence = completed["silent_loss"]
    assert silence["count"] == 0, silence
    assert silence["deferred_pending_at_end"] == 0
    assert silence["processing_at_end"] == 0

    stats = day.stats()
    assert stats["deferred_pending"] == 0
    assert stats["events"]["processing"] == 0
    assert stats["events"]["deferred"] == 0
    assert stats["events"]["processed"] > 0

    # Client and server agree: every accepted event has exactly one durable
    # terminal row, and no accepted event was answered as an HTTP error.
    assert completed["requests"]["client_errors"] == 0
    assert completed["requests"]["http_errors"] == 0
    # The report's own unique count is the authority here: it accounts for a
    # duplicate produced by the generator's own transport re-submission, which
    # is the same event, not a second one (`self_duplicates`).
    accepted = completed["requests"]["accepted_unique"]
    assert accepted == (
        completed["requests"]["accepted"]
        - completed["requests"]["duplicate"]
        + completed["requests"]["self_duplicates"]
    )
    terminal = stats["events"]["processed"] + stats["events"]["failed_downstream"]
    assert terminal == accepted


def test_dlq_matches_the_three_strike_expectation(completed: dict[str, Any]) -> None:
    """Appendix B: 20% per attempt, three attempts => ~0.8% dead-lettered."""
    dlq = completed["dlq"]
    assert dlq["expected_fraction"] == pytest.approx(0.008)
    assert dlq["within_tolerance"] is True, dlq
    assert dlq["created_this_run"] == completed["requests"]["failed_downstream"]
    assert dlq["pending"] + dlq["replayed"] == dlq["created_this_run"]
    assert completed["requests"]["failed_downstream"] > 0


def test_schedule_volume_and_ramp_were_actually_exercised(completed: dict[str, Any]) -> None:
    horizon_s = SIM_HORIZON_H * 3600
    expected = schedule.expected_calls(0.0, horizon_s)
    assert completed["traffic"]["expected_by_schedule"] == pytest.approx(expected, rel=0.01)
    assert completed["traffic"]["generated"] == pytest.approx(expected, rel=0.05)

    # Baseline, ramp and peak must be distinguishable: that is what "handles
    # the ramp and hold correctly" means. The ramp's achieved rate is the mean
    # of its 10 -> 50 endpoints, so it sits strictly between the two holds.
    by_label = {row["label"]: row for row in completed["windows"]}
    baseline = by_label["00:00-08:00 baseline"]["achieved_calls_per_min_sim"]
    ramp = by_label["08:00-08:15 ramp"]["achieved_calls_per_min_sim"]
    peak = by_label["08:15-10:00 peak"]["achieved_calls_per_min_sim"]
    assert baseline == pytest.approx(10.0, rel=0.15)
    assert ramp == pytest.approx(30.0, rel=0.20)
    assert peak == pytest.approx(50.0, rel=0.15)
    assert baseline < ramp < peak

    # The last window is a clipped partial one at the horizon: its end rate is
    # the rate the schedule had reached there, not the full window's 50/min.
    partial = completed["windows"][-1]
    assert partial["duration_sim_min"] < 120.0, "a full peak hold would mean the clip failed"
    assert partial["target_calls_per_min_sim"] == pytest.approx(50.0)

    # The peak window's target is reported in real terms too (SPECS §10's
    # "never claim a rate the generator could not reach" cuts both ways).
    assert by_label["08:15-10:00 peak"]["target_calls_per_min_real"] == 3_000.0


def test_saturation_flag_is_present_and_consistent(completed: dict[str, Any]) -> None:
    """Every window carries an honest saturation verdict backed by its numbers."""
    for row in completed["windows"]:
        assert isinstance(row["saturated"], bool)
        assert "peak_in_flight" in row and "backpressure_waits" in row
        if row["peak_in_flight"] >= CONCURRENCY:
            assert row["saturated"] is True, "hitting the in-flight cap must be reported"
    # This profile fits in capacity: at least the hold windows ran unsaturated,
    # so the run is evidence about the service rather than about the box.
    holds = [
        row
        for row in completed["windows"]
        if row["label"] in {"00:00-08:00 baseline", "08:15-10:00 peak"}
    ]
    assert holds and any(not row["saturated"] for row in holds), completed["windows"]


def test_report_verdict_passed_and_was_not_asserted_blindly(completed: dict[str, Any]) -> None:
    verdict = completed["verdict"]
    assert verdict["passed"] is True, verdict
    assert set(verdict["checks"]) >= {
        "zero_silent_loss",
        "deferred_queue_drained",
        "no_client_errors",
        "windows_all_ran",
        "dlq_fraction_within_tolerance",
        "schedule_volume_matched",
    }
    assert all(verdict["checks"].values()), verdict
    assert completed["latency_ms"]["client_observed"]["p50"] is not None
    assert completed["retries"]["calls_with_retries"] >= completed["requests"]["failed_downstream"]


def test_generator_aborts_cleanly_when_the_service_is_unreachable(tmp_path: Path) -> None:
    """A dead target is a clean non-zero exit with a diagnostic, not a stall."""
    report_path = tmp_path / "unreachable.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.traffic_gen",
            "--target-url",
            "http://127.0.0.1:1",  # nothing listens here
            "--speedup",
            "60",
            "--duration-sim-hours",
            "0.1",
            "--concurrency",
            "4",
            "--out",
            str(report_path),
            "--request-timeout-s",
            "1",
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=180,
        check=False,
    )
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "aborted" in result.stderr.lower()
    assert not report_path.exists(), "no report from an aborted run"


def test_duplicate_submission_is_still_idempotent_on_the_live_stack(day: DayStack) -> None:
    """The generator's events are ordinary Appendix A events (§7.2 intact)."""
    event = reference_event(str(uuid4()))
    first = day.http.post("/calls", json=event)
    assert first.status_code == 200, first.text
    second = day.http.post("/calls", json=event)
    assert second.status_code == 200, second.text
    assert second.json()["duplicate"] is True
    assert second.json()["event_id"] == event["event_id"]
