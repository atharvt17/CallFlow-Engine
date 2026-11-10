"""CLI surface (M4-T02; SPECS §2, §5).

The CLI is a graded surface in its own right ("HTTP-free operations"), and its
whole point is that an operator can use it when the service is unreachable.
These tests therefore drive ``main(argv)`` end to end against a **real temp
store**: rows are seeded through the same service path the API uses, then read,
replayed and counted through the CLI — and the resulting database state is
asserted with both the store API and the HTTP endpoints, so the two surfaces
are proven to agree rather than merely to run.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from app import store
from app.cli import EXIT_INCOMPLETE, EXIT_OK, EXIT_STORE_ERROR, main
from app.config import Settings
from app.main import create_app
from app.models import CallEvent
from app.pipeline.base import PipelineResult


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


def run(argv: list[str], capsys) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def seed_failed_events(db_path: str, count: int) -> list[tuple[str, int]]:
    """Insert ``failed_downstream`` events + dead letters via the store API.

    Uses the authoritative write path (a terminal flip and its DLQ row in one
    transaction) rather than raw INSERTs, so the rows are shaped exactly like
    production ones, ``event_json`` included.
    """
    conn = store.connect(db_path)
    store.init_db(conn)
    seeded = []
    try:
        stamp = datetime.now(UTC).isoformat()
        for _ in range(count):
            event = CallEvent.model_validate(event_payload())
            event_id = str(event.event_id)
            with store.begin_immediate(conn):
                store.insert_event(
                    conn, event, status="processing", created_at=stamp, updated_at=stamp
                )
                store.update_event_terminal(
                    conn,
                    event_id,
                    status="failed_downstream",
                    outcome="failed_downstream",
                    attempts=3,
                    retry_count=2,
                    pipeline_latency_ms=None,
                    e2e_latency_ms=12.5,
                    failure_reason="downstream_timeout",
                    updated_at=stamp,
                )
                dlq_id = store.record_dead_letter(
                    conn,
                    event_id,
                    event.model_dump_json(),
                    reason="downstream_timeout",
                    attempts=3,
                    retry_count=2,
                    first_failed_at=stamp,
                    last_failed_at=stamp,
                )
            seeded.append((event_id, dlq_id))
    finally:
        conn.close()
    return seeded


@pytest.fixture
def cli_db(tmp_path, monkeypatch) -> Path:
    """Temp store pointed at by ``DB_PATH``; leaves env clean for other tests.

    ``PIPELINE_BACKEND=inline`` keeps ``dlq replay`` off the network: the CLI
    builds its own runtime from settings (exactly like the service), so the
    backend has to be configured rather than injected.
    """
    db_path = tmp_path / "cli.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("PIPELINE_BACKEND", "inline")
    monkeypatch.setenv("BACKOFF_BASE_MS", "0")
    return db_path


class ToggleBackend:
    def __init__(self, *, failing: bool = True) -> None:
        self.failing = failing

    async def aclose(self) -> None:
        pass

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        if self.failing:
            return PipelineResult(
                ok=False, reason="downstream_timeout", http_status=503, latency_ms=5.0
            )
        return PipelineResult(ok=True, http_status=200, latency_ms=5.0)


# --- dlq list ---------------------------------------------------------------


def test_dlq_list_prints_seeded_rows(cli_db, capsys) -> None:
    seeded = seed_failed_events(str(cli_db), 2)

    code, out, err = run(["dlq", "list"], capsys)

    assert code == EXIT_OK
    assert err == ""
    # Newest first, and both ids visible in the table.
    assert out.splitlines()[2].split()[0] == str(seeded[1][1])
    for event_id, dlq_id in seeded:
        assert event_id in out
        assert str(dlq_id) in out
    assert "downstream_timeout" in out


def test_dlq_list_json_carries_the_full_spec7_row(cli_db, capsys) -> None:
    seeded = seed_failed_events(str(cli_db), 1)

    code, out, _ = run(["--json", "dlq", "list"], capsys)

    assert code == EXIT_OK
    rows = json.loads(out)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == seeded[0][1]
    assert row["event_id"] == seeded[0][0]
    assert row["event"]["event_id"] == seeded[0][0]
    assert row["attempts"] == 3 and row["retry_count"] == 2
    assert row["status"] == "pending"
    assert "event_json" not in row


def test_dlq_list_filters_and_limits(cli_db, capsys) -> None:
    seed_failed_events(str(cli_db), 3)

    code, out, _ = run(["--json", "dlq", "list", "--limit", "2"], capsys)
    assert code == EXIT_OK
    assert len(json.loads(out)) == 2

    code, out, _ = run(["--json", "dlq", "list", "--status", "replayed"], capsys)
    assert code == EXIT_OK
    assert json.loads(out) == []


def test_dlq_list_on_empty_store_is_a_success(cli_db, capsys) -> None:
    code, out, _ = run(["dlq", "list"], capsys)
    assert code == EXIT_OK
    assert out.strip() == "no dead letters"


def test_dlq_list_rejects_non_positive_limit(cli_db, capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["dlq", "list", "--limit", "0"])
    assert exc.value.code == 2
    assert "--limit" in capsys.readouterr().err


# --- dlq show ---------------------------------------------------------------


def test_dlq_show_prints_one_row(cli_db, capsys) -> None:
    seeded = seed_failed_events(str(cli_db), 1)

    code, out, _ = run(["--json", "dlq", "show", str(seeded[0][1])], capsys)

    assert code == EXIT_OK
    row = json.loads(out)
    assert row["id"] == seeded[0][1]
    assert row["event_id"] == seeded[0][0]


def test_dlq_show_of_unknown_id_fails_with_a_message(cli_db, capsys) -> None:
    seed_failed_events(str(cli_db), 1)

    code, out, err = run(["dlq", "show", "99999"], capsys)

    assert code == EXIT_INCOMPLETE
    assert out == ""
    assert "99999" in err


# --- dlq replay -------------------------------------------------------------


def test_dlq_replay_with_a_healthy_dependency_completes(cli_db, capsys) -> None:
    """Replay must use the real runtime; ``inline`` makes the dependency healthy."""
    seeded = seed_failed_events(str(cli_db), 2)

    code, out, _ = run(["--json", "dlq", "replay"], capsys)
    assert code == EXIT_OK
    body = json.loads(out)
    assert body["attempted"] == 2
    assert body["succeeded"] == 2
    assert body["failed"] == 0
    assert set(body) == {"attempted", "succeeded", "failed", "results"}

    conn = store.connect(str(cli_db))
    try:
        for event_id, dlq_id in seeded:
            assert store.get_event(conn, event_id)["status"] == "processed"
            row = store.get_dead_letter(conn, dlq_id)
            assert row["status"] == "replayed"
            assert row["replay_event_id"] == event_id
        # No new rows: replay updates in place (SPECS §7.2).
        assert conn.execute("SELECT COUNT(*) AS n FROM call_events").fetchone()["n"] == 2
        assert conn.execute("SELECT COUNT(*) AS n FROM dead_letter").fetchone()["n"] == 2
    finally:
        conn.close()


def test_dlq_replay_against_a_dead_dependency_is_incomplete(cli_db, capsys, monkeypatch) -> None:
    """An all-failing boundary leaves every row pending and exits non-zero."""
    seeded = seed_failed_events(str(cli_db), 1)
    db_path = str(cli_db)

    # ``inline`` always succeeds, so point the runtime at a dead HTTP endpoint
    # instead: connection refused is a retryable failure, so the event lands
    # back in the DLQ — the CLI must report that as incomplete.
    monkeypatch.setenv("PIPELINE_BACKEND", "http")
    monkeypatch.setenv("PIPELINE_BASE_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("BACKOFF_BASE_MS", "0")
    monkeypatch.setenv("PIPELINE_TIMEOUT_S", "0.2")

    code, out, _ = run(["--json", "dlq", "replay"], capsys)

    assert code == EXIT_INCOMPLETE
    body = json.loads(out)
    assert body["attempted"] == 1
    assert body["failed"] == 1
    assert body["succeeded"] == 0

    conn = store.connect(db_path)
    try:
        row = store.get_dead_letter(conn, seeded[0][1])
        assert row["status"] == "pending"
        assert row["id"] == seeded[0][1]  # same row, updated
    finally:
        conn.close()


def test_dlq_replay_by_event_ids_only_touches_the_named_event(cli_db, capsys) -> None:
    seeded = seed_failed_events(str(cli_db), 2)
    chosen, untouched = seeded[0], seeded[1]

    code, out, _ = run(
        ["--json", "dlq", "replay", "--event-ids", chosen[0]], capsys
    )

    assert code == EXIT_OK
    body = json.loads(out)
    assert body["succeeded"] == 1
    assert [r["event_id"] for r in body["results"]] == [chosen[0]]

    conn = store.connect(str(cli_db))
    try:
        assert store.get_event(conn, chosen[0])["status"] == "processed"
        assert store.get_event(conn, untouched[0])["status"] == "failed_downstream"
        assert store.get_dead_letter(conn, untouched[1])["status"] == "pending"
    finally:
        conn.close()


def test_dlq_replay_reports_an_unknown_event_id(cli_db, capsys) -> None:
    ghost = str(uuid4())

    code, out, _ = run(["--json", "dlq", "replay", "--event-ids", ghost], capsys)

    assert code == EXIT_INCOMPLETE
    body = json.loads(out)
    assert body["attempted"] == 0
    assert body["results"][0]["status"] == "missing"


# --- bounds ------------------------------------------------------------------


def test_dlq_replay_rejects_one_id_above_the_cap(cli_db, capsys) -> None:
    """A huge ``--event-ids`` is a usage error, not a driver failure (item D)."""
    ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS + 1)]

    with pytest.raises(SystemExit) as excinfo:
        main(["--json", "dlq", "replay", "--event-ids", *ids])

    assert excinfo.value.code == 2  # argparse usage error
    err = capsys.readouterr().err
    assert str(store.MAX_REPLAY_EVENT_IDS) in err


def test_dlq_replay_accepts_event_ids_exactly_at_the_cap(cli_db, capsys) -> None:
    """Exactly at the cap is allowed; nothing is replayed, so exit is incomplete."""
    ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS)]

    code, out, _ = run(["--json", "dlq", "replay", "--event-ids", *ids], capsys)

    assert code == EXIT_INCOMPLETE  # every id is unknown → `missing`
    body = json.loads(out)
    assert body["attempted"] == 0
    assert {r["status"] for r in body["results"]} == {"missing"}


def test_the_static_cap_is_below_the_legacy_sqlite_variable_limit() -> None:
    """999 host parameters was SQLite's default until 3.32.0 (item D).

    The cap must be genuinely safe there — a value of 1_000 would exceed it.
    The id list is the only bound parameter in the replay query, so the cap
    alone is what has to fit.
    """
    assert store.MAX_REPLAY_EVENT_IDS < 999
    assert store.MAX_REPLAY_EVENT_IDS <= store._LEGACY_VARIABLE_LIMIT - (
        store._REPLAY_PARAM_HEADROOM
    )


# --- stats ------------------------------------------------------------------


def test_stats_prints_durable_counts(cli_db, capsys) -> None:
    seed_failed_events(str(cli_db), 2)

    code, out, _ = run(["stats"], capsys)

    assert code == EXIT_OK
    assert "deferred_pending  0" in out
    assert '"failed_downstream": 2' in out
    assert '"pending": 2' in out


def test_stats_json_shape_matches_the_http_endpoint(cli_db, tmp_path, capsys) -> None:
    """The CLI and ``GET /stats`` must emit the same durable keys."""
    seed_failed_events(str(cli_db), 1)

    code, out, _ = run(["--json", "stats"], capsys)
    assert code == EXIT_OK
    cli_body = json.loads(out)
    assert "circuit_breaker" not in cli_body  # not observable from the CLI
    assert "uptime_s" not in cli_body
    # Percentiles are process-local and this process has no traffic yet.
    assert cli_body["latency_percentiles"] == {"p50": None, "p90": None, "p99": None}


async def test_cli_and_http_agree_on_the_same_store(cli_db, capsys) -> None:
    """Same DB, both surfaces: identical counts and identical rows."""
    seeded = seed_failed_events(str(cli_db), 2)
    settings = Settings(db_path=str(cli_db), pipeline_backend="inline")
    app = create_app(settings, ToggleBackend(failing=False))
    # Not a context manager: the CLI runs synchronously inside this test (its
    # own ``asyncio.run``), so the HTTP client must stay open across those calls.
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://inference.test"
    )
    try:
        http_rows = (await client.get("/admin/dlq")).json()
        http_stats = (await client.get("/stats")).json()

        code, out, _ = run(["--json", "dlq", "list"], capsys)
        assert code == EXIT_OK
        assert json.loads(out) == http_rows

        code, out, _ = run(["--json", "stats"], capsys)
        cli_stats = json.loads(out)
        for key in ("events", "dead_letter", "deferred_pending"):
            assert cli_stats[key] == http_stats[key]
        assert (await client.get(f"/admin/dlq/{seeded[0][1]}")).json()["status"] == "pending"

        # HTTP replay then CLI inspection: the CLI sees the server's write.
        body = (await client.post("/admin/dlq/replay", json={"limit": 10})).json()
        assert body["succeeded"] == 2

        code, out, _ = run(["--json", "dlq", "list", "--status", "replayed"], capsys)
        assert code == EXIT_OK
        replayed = json.loads(out)
        assert {row["id"] for row in replayed} == {dlq_id for _, dlq_id in seeded}
        assert {row["replay_outcome"] for row in replayed} == {"processed"}
    finally:
        await client.aclose()
        await app.state.backend.aclose()
        app.state.conn.close()


# --- failure modes ----------------------------------------------------------


def test_unopenable_store_exits_with_the_store_code(tmp_path, monkeypatch, capsys) -> None:
    """A path that cannot be a database must not raise a traceback."""
    bogus = tmp_path / "not-a-dir"
    bogus.write_text("x", encoding="utf-8")  # a FILE where a directory is needed
    monkeypatch.setenv("DB_PATH", str(bogus / "cli.db"))

    code, out, err = run(["dlq", "list"], capsys)

    assert code == EXIT_STORE_ERROR
    assert out == ""
    assert "cannot use store" in err


def test_incompatible_schema_exits_with_the_store_code(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            "CREATE TABLE call_events (event_id TEXT PRIMARY KEY);"
            "CREATE TABLE dead_letter (id INTEGER PRIMARY KEY);"
        )
    finally:
        conn.close()
    monkeypatch.setenv("DB_PATH", str(db_path))

    code, _, err = run(["stats"], capsys)

    assert code == EXIT_STORE_ERROR
    assert "incompatible" in err or "missing columns" in err
