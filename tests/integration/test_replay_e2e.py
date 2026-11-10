"""End-to-end DLQ replay proof (M4-T07; SPECS §11, §12; PLAN M4 DoD).

The scored demo path as one automated test, across **real process
boundaries**: a live Appendix B mock, a live inference server, and the operator
CLI in a separate interpreter, all sharing one SQLite file.

The loop, in the order the challenge describes it:

1. a forced failure window drives N events into the DLQ (``200`` +
   ``failed_downstream``, not a breaker deferral);
2. the backlog is inspected over HTTP (``GET /admin/dlq`` and its detail
   route) **and** over the CLI, in both its table and JSON forms;
3. a CLI replay against the still-dead dependency proves replay is genuinely
   resilient — three attempts per event, the same rows updated in place, the
   rows still ``pending`` and still replayable;
4. the mock recovers; the backlog is drained by an HTTP replay (named
   ``event_ids``) and a CLI replay (``--limit``) — the two required surfaces;
5. every replayed row is terminal, the DLQ reads ``replayed`` with the outcome
   and event link recorded, row counts never moved, and §7.1's accounting
   invariant holds on the store file itself.

Live servers (rather than ``ASGITransport``) are what make the CLI half
meaningful: ``python -m app.cli`` builds its own runtime from ``DB_PATH`` /
``PIPELINE_BASE_URL`` exactly as it does for an operator, and reaches the same
dependency over TCP. The same setup therefore also proves that a replay from a
second process against the serving process's database is safe — the durable
claim in :func:`app.store.claim_event_for_replay` is what stops it from
double-processing (M4-T03).

Determinism: the mock is pinned to ``failure_rate=1.0`` for the failure window
(immediate 503s, no latency draws) and restored to ``0.0`` for the replay, so
neither phase depends on a random draw.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
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

from app.cli import EXIT_INCOMPLETE
from app.config import Settings
from app.main import create_app
from mock.config import MockSettings
from mock.main import create_mock_app

FAST_LATENCY_MU = -3.0
EVENT_COUNT = 4
HTTP_REPLAY_COUNT = 2
STARTUP_TIMEOUT_S = 15.0

REPO_ROOT = Path(__file__).resolve().parents[2]


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


class LiveServer:
    """A uvicorn server on an ephemeral loopback port, in a daemon thread.

    The socket is bound *before* the server thread starts serving, so the URL
    is known without racing uvicorn's bind. Signals are not captured (uvicorn
    skips that off the main thread), and ``stop`` is an orderly shutdown:
    ``should_exit`` runs the lifespan exit, which closes the store connection
    and the backend pool.
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
            sock.listen(128)
            self.url = f"http://127.0.0.1:{sock.getsockname()[1]}"
            self._server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
            self._ready.set()
            try:
                self._server.run(sockets=[sock])
            finally:
                sock.close()
        except Exception as exc:  # noqa: BLE001 - re-raised on the test thread, not swallowed
            self._error = exc
            self._ready.set()

    def _wait_for_health(self) -> None:
        deadline = time.monotonic() + STARTUP_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"{self.url}/healthz", timeout=1.0, trust_env=False).status_code == 200:
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


def run_cli(
    db_path: Path,
    mock_url: str,
    *argv: str,
    as_json: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run ``python -m app.cli`` in a separate interpreter against this stack.

    Environment, not injection, is how an operator configures the CLI, so that
    is how this test configures it too. ``cwd`` is the repo root so the CLI
    imports the working tree rather than a stale editable install.
    """
    env = os.environ.copy()
    env.update(
        {
            "DB_PATH": str(db_path),
            "PIPELINE_BACKEND": "http",
            "PIPELINE_BASE_URL": mock_url,
            "PIPELINE_TIMEOUT_S": "2.0",
            "BACKOFF_BASE_MS": "0",
            "CB_CONSECUTIVE_FAILURES": "1000",
            "CB_MIN_THROUGHPUT": "1000",
        }
    )
    command = ("--json", *argv) if as_json else argv
    return subprocess.run(
        [sys.executable, "-m", "app.cli", *command],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
        timeout=120,
        check=False,
    )


def read_rows(db_path: Path, table: str) -> list[dict[str, Any]]:
    """Read one table straight from the store file (the §7.1 audit view)."""
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(f"SELECT * FROM {table}")]
    finally:
        conn.close()


def row_counts(db_path: Path) -> tuple[int, int]:
    """``(call_events, dead_letter)`` row counts — the no-new-rows invariant."""
    return len(read_rows(db_path, "call_events")), len(read_rows(db_path, "dead_letter"))


class E2EStack:
    """Mock server + inference server + a CLI that runs in its own process."""

    def __init__(self, tmp_path: Path) -> None:
        self.db_path = tmp_path / "e2e.db"
        # Failure window: 100% failures, immediate 503s (no latency draw).
        self.mock = LiveServer(
            lambda: create_mock_app(MockSettings(latency_mu=FAST_LATENCY_MU, failure_rate=1.0))
        )
        self.settings = Settings(
            db_path=str(self.db_path),
            pipeline_backend="http",
            pipeline_base_url=self.mock.url,
            backoff_base_ms=0,
            # Keep the breaker out of this scenario's way: the DLQ is populated
            # by terminal failures, which only happen while the breaker admits
            # calls. Breaker-refused replay (`deferred`) has its own tests
            # (tests/unit/test_replay.py).
            cb_consecutive_failures=1000,
            cb_min_throughput=1000,
        )
        self.inference = LiveServer(lambda: create_app(self.settings))
        self.http = httpx.Client(base_url=self.inference.url, timeout=30.0, trust_env=False)
        self.mock_http = httpx.Client(base_url=self.mock.url, timeout=10.0, trust_env=False)

    def set_mock_failure_rate(self, rate: float) -> None:
        resp = self.mock_http.post("/admin/config", json={"failure_rate": rate})
        assert resp.status_code == 200, resp.text
        assert resp.json()["failure_rate"] == rate

    def submit(self, count: int) -> list[dict[str, Any]]:
        """Drive ``count`` distinct events into the DLQ through the public API."""
        bodies: list[dict[str, Any]] = []
        for _ in range(count):
            resp = self.http.post("/calls", json=event_payload())
            assert resp.status_code == 200, resp.text
            body = resp.json()
            # 200 + failed_downstream is a terminal failure that writes a DLQ
            # row; a 202 would be a breaker deferral (not what this window is).
            assert body["status"] == "failed_downstream", body
            assert body["dlq_id"] is not None
            bodies.append(body)
        return bodies

    def cli(self, *argv: str, as_json: bool = True) -> subprocess.CompletedProcess[str]:
        return run_cli(self.db_path, self.mock.url, *argv, as_json=as_json)

    def close(self) -> None:
        self.http.close()
        self.mock_http.close()
        self.inference.stop()
        self.mock.stop()


@pytest.fixture
def e2e(tmp_path):
    stack = E2EStack(tmp_path)
    try:
        yield stack
    finally:
        stack.close()


def test_forced_failure_window_is_inspectable_on_both_surfaces(e2e) -> None:
    """Steps 1–2: the backlog exists and both surfaces report the same rows."""
    submitted = e2e.submit(EVENT_COUNT)
    event_ids = [str(body["event_id"]) for body in submitted]
    dlq_ids = [int(body["dlq_id"]) for body in submitted]
    assert len(set(event_ids)) == EVENT_COUNT

    http_rows = e2e.http.get("/admin/dlq").json()
    assert [row["id"] for row in http_rows] == sorted(dlq_ids, reverse=True)
    for row in http_rows:
        assert row["status"] == "pending"
        assert row["reason"] == "downstream_timeout"
        assert row["attempts"] == 3
        assert row["retry_count"] == row["attempts"] - 1
        assert row["first_failed_at"] <= row["last_failed_at"]
        assert row["replayed_at"] is None
        assert row["replay_outcome"] is None
        assert "event_json" not in row
        assert row["event"]["event_id"] == row["event_id"]

    detail = e2e.http.get(f"/admin/dlq/{dlq_ids[0]}").json()
    assert detail == next(row for row in http_rows if row["id"] == dlq_ids[0])

    listed = e2e.cli("dlq", "list")
    assert listed.returncode == 0, listed.stderr
    assert json.loads(listed.stdout) == http_rows, "CLI and HTTP must agree row for row"

    shown = e2e.cli("dlq", "show", str(dlq_ids[0]))
    assert shown.returncode == 0, shown.stderr
    assert json.loads(shown.stdout) == detail

    table = e2e.cli("dlq", "list", as_json=False)
    assert table.returncode == 0, table.stderr
    for dlq_id in dlq_ids:
        assert str(dlq_id) in table.stdout
    assert "downstream_timeout" in table.stdout

    assert row_counts(e2e.db_path) == (EVENT_COUNT, EVENT_COUNT)


def test_full_dlq_replay_loop_on_both_surfaces(e2e) -> None:
    """The scored path: fail → inspect → replay (CLI + HTTP) → terminal."""
    submitted = e2e.submit(EVENT_COUNT)
    event_ids = [str(body["event_id"]) for body in submitted]
    dlq_ids = [int(body["dlq_id"]) for body in submitted]
    counts_before = row_counts(e2e.db_path)
    assert counts_before == (EVENT_COUNT, EVENT_COUNT)

    # --- a replay while the dependency is still dead changes no row's identity
    failing = e2e.cli("dlq", "replay", "--limit", str(EVENT_COUNT))
    assert failing.returncode == EXIT_INCOMPLETE, failing.stderr
    failing_body = json.loads(failing.stdout)
    assert failing_body["attempted"] == EVENT_COUNT
    assert failing_body["failed"] == EVENT_COUNT
    assert failing_body["succeeded"] == 0
    assert {result["status"] for result in failing_body["results"]} == {"pending"}

    pending = e2e.http.get("/admin/dlq").json()
    assert {row["id"] for row in pending} == set(dlq_ids)
    assert {row["attempts"] for row in pending} == {6}, "same rows, cumulative attempts"
    assert row_counts(e2e.db_path) == counts_before

    # --- the dependency recovers; drain the backlog on both required surfaces
    e2e.set_mock_failure_rate(0.0)
    by_http, by_cli = event_ids[:HTTP_REPLAY_COUNT], event_ids[HTTP_REPLAY_COUNT:]
    assert len(by_http) == HTTP_REPLAY_COUNT and by_cli

    over_http = e2e.http.post("/admin/dlq/replay", json={"event_ids": by_http}).json()
    assert over_http["attempted"] == len(by_http)
    assert over_http["succeeded"] == len(by_http)
    assert over_http["failed"] == 0
    assert {result["status"] for result in over_http["results"]} == {"replayed"}
    assert {result["event_id"] for result in over_http["results"]} == set(by_http)

    over_cli = e2e.cli("dlq", "replay", "--limit", "10")
    assert over_cli.returncode == 0, over_cli.stderr
    cli_body = json.loads(over_cli.stdout)
    assert cli_body["attempted"] == len(by_cli)
    assert cli_body["succeeded"] == len(by_cli)
    assert cli_body["failed"] == 0
    assert set(cli_body) == {"attempted", "succeeded", "failed", "results"}
    assert {result["event_id"] for result in cli_body["results"]} == set(by_cli)

    # --- nothing was duplicated, and every row is terminal (§7.1) ------------
    assert row_counts(e2e.db_path) == counts_before

    events = {row["event_id"]: row for row in read_rows(e2e.db_path, "call_events")}
    letters = {row["event_id"]: row for row in read_rows(e2e.db_path, "dead_letter")}
    assert set(events) == set(event_ids)
    assert set(letters) == set(event_ids)

    for event_id in event_ids:
        event, letter = events[event_id], letters[event_id]
        # §7.1: exactly one terminal state per accepted event — never left
        # deferred/processing, never silently lost.
        assert event["status"] == "processed"
        assert event["outcome"] in {"connected", "ptp_taken"}
        assert event["failure_reason"] is None
        assert event["retry_count"] == event["attempts"] - 1
        # In place: the original event row and the original DLQ row, linked
        # both ways, were updated — no second row of either kind.
        assert event["dlq_id"] == letter["id"]
        assert letter["id"] in dlq_ids
        # §7.2: the DLQ row records the replay outcome and the event link.
        assert letter["status"] == "replayed"
        assert letter["replay_outcome"] == "processed"
        assert letter["replay_event_id"] == event_id
        assert letter["replayed_at"] is not None
        assert letter["attempts"] == event["attempts"]
        assert letter["retry_count"] == letter["attempts"] - 1

    # --- the surfaces agree after the drain ---------------------------------
    stats = e2e.http.get("/stats").json()
    assert stats["events"] == {
        "deferred": 0,
        "processing": 0,
        "processed": EVENT_COUNT,
        "failed_downstream": 0,
    }
    assert stats["dead_letter"] == {"pending": 0, "replayed": EVENT_COUNT}
    assert stats["deferred_pending"] == 0

    cli_stats = e2e.cli("stats")
    assert cli_stats.returncode == 0, cli_stats.stderr
    cli_stats_body = json.loads(cli_stats.stdout)
    for key in ("events", "dead_letter", "deferred_pending"):
        assert cli_stats_body[key] == stats[key]

    assert e2e.http.get("/admin/dlq").json() == []
    replayed_rows = e2e.http.get("/admin/dlq", params={"status": "replayed"}).json()
    assert [row["id"] for row in replayed_rows] == sorted(dlq_ids, reverse=True)
    assert json.loads(e2e.cli("dlq", "list", "--status", "replayed").stdout) == replayed_rows

    # --- the public duplicate short-circuit is still intact (§7.2) ----------
    again = e2e.http.post("/calls", json=event_payload(event_ids[0]))
    assert again.status_code == 200, again.text
    duplicate = again.json()
    assert duplicate["duplicate"] is True
    assert duplicate["status"] == "processed"
    assert row_counts(e2e.db_path) == counts_before
