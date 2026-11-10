"""Replay behaviour (M4-T06; SPECS §7.2, §11).

Every §7.2 rule is asserted here as an observable end-state, not as an
implementation detail:

- a successful replay moves the **original** event to ``processed`` and its DLQ
  row to ``replayed`` with the outcome and the event link recorded;
- a replay that fails again updates that same row (attempts, timestamps,
  reason) and leaves it ``pending`` — row counts never move;
- replay bypasses the public duplicate short-circuit while the public
  ``POST /calls`` short-circuit itself stays intact;
- a breaker that refuses the dispatch changes nothing at all: no downstream
  call, no status flip, the row remains replayable.

The backend is toggled in-process (``ToggleBackend``) so "the dependency
recovers" is a deterministic state change rather than mock timing.
"""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from app import store
from app.config import Settings
from app.main import create_app
from app.pipeline.base import PipelineResult
from app.reporting import interpolate_quantile
from app.resilience.circuit_breaker import CircuitState


def event_payload(event_id: str | None = None) -> dict[str, object]:
    return {
        "event_id": event_id or str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


class ToggleBackend:
    """In-process boundary whose health the test flips."""

    def __init__(self, *, failing: bool = True) -> None:
        self.failing = failing
        self.dispatches = 0

    async def aclose(self) -> None:
        pass

    async def process(self, event_id: str, audio_ref: str) -> PipelineResult:
        self.dispatches += 1
        if self.failing:
            return PipelineResult(
                ok=False, reason="downstream_timeout", http_status=503, latency_ms=5.0
            )
        return PipelineResult(ok=True, http_status=200, latency_ms=250.0)


class ReplayStack:
    """Inference app over ASGI with its state and a toggleable dependency."""

    def __init__(self, tmp_path, *, failing: bool = True, **overrides) -> None:
        self.backend = ToggleBackend(failing=failing)
        knobs: dict[str, object] = {
            "db_path": str(tmp_path / "replay.db"),
            "pipeline_backend": "inline",  # unused: the backend is injected
            "backoff_base_ms": 0,
            # Keep the breaker out of the way unless a test is about the
            # breaker: each failing replay would otherwise pile onto the
            # consecutive-failure counter and trip it mid-test.
            "cb_consecutive_failures": 50,
            "cb_min_throughput": 50,
        }
        knobs.update(overrides)
        self.settings = Settings(**knobs)  # type: ignore[arg-type]
        self.app = create_app(self.settings, self.backend)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://inference.test"
        )

    async def fail_events(self, count: int) -> list[dict[str, object]]:
        """Drive ``count`` events into the DLQ through the real write path."""
        bodies = []
        for _ in range(count):
            resp = await self.client.post("/calls", json=event_payload())
            assert resp.status_code == 200
            body = resp.json()
            assert body["status"] == "failed_downstream"
            bodies.append(body)
        return bodies

    async def aclose(self) -> None:
        await self.client.aclose()
        await self.backend.aclose()
        self.app.state.conn.close()


@pytest.fixture
def replay_stack(tmp_path):
    stacks: list[ReplayStack] = []

    def _factory(**kwargs) -> ReplayStack:
        stack = ReplayStack(tmp_path, **kwargs)
        stacks.append(stack)
        return stack

    yield _factory
    for stack in stacks:
        stack.app.state.conn.close()


def row_counts(conn) -> tuple[int, int]:
    """``(call_events, dead_letter)`` row counts — the no-new-rows invariant."""
    events = conn.execute("SELECT COUNT(*) AS n FROM call_events").fetchone()["n"]
    dead = conn.execute("SELECT COUNT(*) AS n FROM dead_letter").fetchone()["n"]
    return int(events), int(dead)


# --- success path -----------------------------------------------------------


async def test_replay_success_updates_event_and_dlq_row_in_place(replay_stack) -> None:
    stack = replay_stack()
    try:
        submitted = (await stack.fail_events(1))[0]
        event_id = submitted["event_id"]
        dlq_id = submitted["dlq_id"]
        assert row_counts(stack.app.state.conn) == (1, 1)

        stack.backend.failing = False  # the dependency recovers
        resp = await stack.client.post("/admin/dlq/replay")
        assert resp.status_code == 200
        body = resp.json()
        assert body["attempted"] == 1
        assert body["succeeded"] == 1
        assert body["failed"] == 0
        assert "deferred" not in body and "skipped" not in body, (
            "§5 fixes the top-level body; per-row detail belongs in results[]"
        )
        assert body["results"] == [
            {
                "event_id": event_id,
                "dlq_id": dlq_id,
                "status": "replayed",
                "attempts": 1,
            }
        ]

        # Event row: terminal success, in place (still one row).
        record = (await stack.client.get(f"/calls/{event_id}")).json()
        assert record["status"] == "processed"
        assert record["outcome"] in ("connected", "ptp_taken")
        assert record["failure_reason"] is None
        assert record["dlq_id"] == dlq_id  # §7.2: the link is kept
        assert record["e2e_latency_ms"] is not None

        # DLQ row: replayed + outcome + event link.
        dlq = (await stack.client.get(f"/admin/dlq/{dlq_id}")).json()
        assert dlq["status"] == "replayed"
        assert dlq["replay_outcome"] == "processed"
        assert dlq["replay_event_id"] == event_id
        assert dlq["replayed_at"] is not None

        assert row_counts(stack.app.state.conn) == (1, 1)
        assert (await stack.client.get("/stats")).json()["dead_letter"] == {
            "pending": 0,
            "replayed": 1,
        }
    finally:
        await stack.aclose()


async def test_replay_accounts_attempts_across_the_whole_lifecycle(replay_stack) -> None:
    """§3/§7: ``retry_count == attempts - 1`` spans first pass + replay."""
    stack = replay_stack()
    try:
        submitted = (await stack.fail_events(1))[0]
        assert submitted["attempts"] == 3  # three failing attempts, no backoff

        stack.backend.failing = False
        await stack.client.post("/admin/dlq/replay")

        record = (await stack.client.get(f"/calls/{submitted['event_id']}")).json()
        assert record["attempts"] == 4
        assert record["retry_count"] == 3
        dlq = (await stack.client.get(f"/admin/dlq/{submitted['dlq_id']}")).json()
        assert dlq["attempts"] == record["attempts"]
        assert dlq["retry_count"] == record["retry_count"]
    finally:
        await stack.aclose()


# --- repeated failures ------------------------------------------------------


async def test_repeated_replay_failures_update_the_same_row(replay_stack) -> None:
    """The UNIQUE ``event_id`` must force updates, never new rows (§7.2)."""
    stack = replay_stack()
    try:
        submitted = (await stack.fail_events(1))[0]
        event_id = submitted["event_id"]
        dlq_id = submitted["dlq_id"]
        conn = stack.app.state.conn
        first = store.get_dead_letter(conn, dlq_id)
        first_failed_at = first["first_failed_at"]

        for expected_attempts in (6, 9):
            resp = await stack.client.post("/admin/dlq/replay")
            body = resp.json()
            assert body["results"][0]["status"] == "pending"
            assert body["attempted"] == 1
            assert body["failed"] == 1
            assert body["succeeded"] == 0

            # Same rows, same id — nothing was duplicated.
            assert row_counts(conn) == (1, 1)
            row = store.get_dead_letter(conn, dlq_id)
            assert row["id"] == dlq_id
            assert row["status"] == "pending"
            assert row["attempts"] == expected_attempts  # cumulative across replays
            assert row["retry_count"] == expected_attempts - 1
            assert row["first_failed_at"] == first_failed_at  # preserved
            assert row["reason"] == "downstream_timeout"
            event = store.get_event(conn, event_id)
            assert event["attempts"] == expected_attempts
            assert event["status"] == "failed_downstream"

        # ``last_failed_at`` moved forward, ``first_failed_at`` did not.
        assert store.get_dead_letter(conn, dlq_id)["last_failed_at"] >= first_failed_at
        assert (await stack.client.get(f"/admin/dlq/{dlq_id}")).json()["replay_outcome"] is None
    finally:
        await stack.aclose()


# --- idempotency, both ways -------------------------------------------------


async def test_replay_bypasses_the_public_duplicate_short_circuit(replay_stack) -> None:
    """An authorized replay must re-process, not answer "duplicate" (§7.2)."""
    stack = replay_stack()
    try:
        submitted = (await stack.fail_events(1))[0]
        event_id = submitted["event_id"]
        dlq_id = submitted["dlq_id"]

        # The public path would short-circuit here: the row already exists and
        # no downstream call is made.
        dispatches_before = stack.backend.dispatches
        duplicate = (await stack.client.post("/calls", json=event_payload(event_id))).json()
        assert duplicate["duplicate"] is True
        assert duplicate["status"] == "failed_downstream"
        assert stack.backend.dispatches == dispatches_before

        # Replay is not fooled by that: it dispatches and re-processes.
        stack.backend.failing = False
        body = (await stack.client.post("/admin/dlq/replay")).json()
        assert body["results"][0]["status"] == "replayed"
        assert stack.backend.dispatches > dispatches_before
        assert (await stack.client.get(f"/calls/{event_id}")).json()["status"] == "processed"
        assert (await stack.client.get(f"/admin/dlq/{dlq_id}")).json()["status"] == "replayed"
    finally:
        await stack.aclose()


async def test_public_post_idempotency_is_unaffected_by_replay(replay_stack) -> None:
    """After a replay, ``POST /calls`` still answers duplicate — no re-dispatch."""
    stack = replay_stack()
    try:
        submitted = (await stack.fail_events(1))[0]
        event_id = submitted["event_id"]

        stack.backend.failing = False
        await stack.client.post("/admin/dlq/replay")
        dispatches = stack.backend.dispatches

        again = await stack.client.post("/calls", json=event_payload(event_id))
        assert again.status_code == 200
        body = again.json()
        assert body["duplicate"] is True
        assert body["status"] == "processed"
        assert stack.backend.dispatches == dispatches
        assert row_counts(stack.app.state.conn) == (1, 1)
    finally:
        await stack.aclose()


# --- the breaker is not bypassed -------------------------------------------


async def test_replay_respects_an_open_breaker(replay_stack) -> None:
    """An open breaker refuses replays exactly like first-pass traffic.

    ``cb_min_throughput=50`` (the fixture default) exceeds the window size, so
    the ratio path never trips and the breaker opens purely on the documented
    consecutive-failure condition — after the 5th failing logical call, which
    is also the 5th dead letter.
    """
    stack = replay_stack(cb_cooldown_s=3600.0, cb_consecutive_failures=5)
    try:
        await stack.fail_events(5)
        assert stack.app.state.breaker.state is CircuitState.OPEN
        pending_before = store.count_dead_letters(stack.app.state.conn)["pending"]
        assert pending_before == 5
        dispatches = stack.backend.dispatches

        body = (await stack.client.post("/admin/dlq/replay", json={"limit": 10})).json()
        assert body["attempted"] == 0
        assert body["succeeded"] == 0
        assert body["failed"] == 0
        # A refusal is visible in results[] (`deferred`), not as a top-level key.
        assert {r["status"] for r in body["results"]} == {"deferred"}
        # The refusal is a real refusal: nothing reached the dependency.
        assert stack.backend.dispatches == dispatches

        # Every row is still pending and therefore still replayable.
        pending = (await stack.client.get("/admin/dlq")).json()
        assert len(pending) == 5
        assert {row["status"] for row in pending} == {"pending"}
        for row in pending:
            event = (await stack.client.get(f"/calls/{row['event_id']}")).json()
            assert event["status"] == "failed_downstream"
            assert event["attempts"] == 3
    finally:
        await stack.aclose()


# --- selection --------------------------------------------------------------


async def test_replay_by_explicit_event_ids_only_touches_named_events(
    replay_stack,
) -> None:
    stack = replay_stack()
    try:
        bodies = await stack.fail_events(2)
        chosen, untouched = bodies[0], bodies[1]
        stack.backend.failing = False

        body = (
            await stack.client.post(
                "/admin/dlq/replay", json={"event_ids": [chosen["event_id"]]}
            )
        ).json()
        assert body["succeeded"] == 1
        assert [r["event_id"] for r in body["results"]] == [chosen["event_id"]]
        assert (await stack.client.get(f"/calls/{chosen['event_id']}")).json()["status"] == (
            "processed"
        )
        assert (await stack.client.get(f"/calls/{untouched['event_id']}")).json()["status"] == (
            "failed_downstream"
        )
    finally:
        await stack.aclose()


async def test_replay_reports_unknown_event_ids_instead_of_dropping_them(
    replay_stack,
) -> None:
    """A mistyped id must not look like a successful replay."""
    stack = replay_stack()
    try:
        stack.backend.failing = False
        ghost = str(uuid4())
        body = (
            await stack.client.post("/admin/dlq/replay", json={"event_ids": [ghost]})
        ).json()
        assert body["attempted"] == 0
        assert body["succeeded"] == 0
        assert body["results"] == [
            {"event_id": ghost, "dlq_id": None, "status": "missing", "attempts": 0}
        ]
    finally:
        await stack.aclose()


async def test_replay_of_an_already_replayed_event_is_not_claimed(replay_stack) -> None:
    stack = replay_stack()
    try:
        submitted = (await stack.fail_events(1))[0]
        stack.backend.failing = False
        first = (await stack.client.post("/admin/dlq/replay")).json()
        assert first["succeeded"] == 1

        second = (await stack.client.post("/admin/dlq/replay")).json()
        assert second["attempted"] == 0
        assert second["succeeded"] == 0
        assert second["results"] == []

        # Naming it explicitly is answered honestly rather than silently.
        named = (
            await stack.client.post(
                "/admin/dlq/replay", json={"event_ids": [submitted["event_id"]]}
            )
        ).json()
        assert named["attempted"] == 0
        assert named["results"][0]["status"] == "missing"
    finally:
        await stack.aclose()


def test_concurrent_claims_cannot_double_process(tmp_db) -> None:
    """The claim is a conditional UPDATE: exactly one caller wins (§7.2)."""
    event_id = str(uuid4())
    with store.begin_immediate(tmp_db):
        tmp_db.execute(
            "INSERT INTO call_events (event_id, campaign_id, borrower_bucket, timestamp,"
            " input_outcome, event_json, status, attempts, retry_count, created_at, updated_at)"
            " VALUES (?, 'c', '0-30 DPD', '2026-09-01T09:03:12Z', 'connected', '{}',"
            " 'failed_downstream', 3, 2, 'x', 'x')",
            (event_id,),
        )

    assert store.claim_event_for_replay(tmp_db, event_id) is True
    assert store.claim_event_for_replay(tmp_db, event_id) is False
    assert store.get_event(tmp_db, event_id)["status"] == "processing"


async def test_replay_rejects_one_id_above_the_cap(replay_stack) -> None:
    """Bounded request, not an opaque driver error (item D)."""
    stack = replay_stack(failing=False)
    try:
        ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS + 1)]
        resp = await stack.client.post("/admin/dlq/replay", json={"event_ids": ids})
        assert resp.status_code == 422
        assert "event_ids" in resp.text
    finally:
        await stack.aclose()


async def test_replay_accepts_event_ids_exactly_at_the_cap(replay_stack) -> None:
    """The boundary is inclusive: at the cap is a valid request."""
    stack = replay_stack(failing=False)
    try:
        ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS)]
        resp = await stack.client.post("/admin/dlq/replay", json={"event_ids": ids})
        assert resp.status_code == 200
        body = resp.json()
        # All unknown → reported, not dropped; the request still succeeded.
        assert body["attempted"] == 0
        assert len(body["results"]) == store.MAX_REPLAY_EVENT_IDS
    finally:
        await stack.aclose()


def test_select_replayable_rejects_one_id_above_the_cap(tmp_db) -> None:
    """A programmatic caller bypassing HTTP is still bounded (item D)."""
    ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS + 1)]
    with pytest.raises(store.ReplayBoundsError, match="accepts at most"):
        store.select_replayable(tmp_db, limit=10, event_ids=ids)


def test_select_replayable_accepts_exactly_the_cap(tmp_db) -> None:
    ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS)]
    assert store.select_replayable(tmp_db, limit=10, event_ids=ids) == []


def test_rejection_happens_before_the_query_executes(tmp_db) -> None:
    """The bound is enforced in Python, so no SQL is ever prepared (item D)."""

    class ExplodingConnection:
        """Fails the test if the store reaches the database at all."""

        def execute(self, *args, **kwargs):  # pragma: no cover - must not run
            raise AssertionError("the query must not execute")

        getlimit = None

    ids = [str(uuid4()) for _ in range(store.MAX_REPLAY_EVENT_IDS + 1)]
    with pytest.raises(store.ReplayBoundsError):
        store.select_replayable(ExplodingConnection(), limit=10, event_ids=ids)  # type: ignore[arg-type]


def test_the_cap_honours_a_legacy_999_variable_connection(tmp_db) -> None:
    """A connection limited to 999 host parameters tightens the cap (item D).

    ``MAX_REPLAY_EVENT_IDS`` is already below 999, so this pins the *dynamic*
    path: whatever the connection reports, ``check_replay_event_ids`` uses it,
    and the result fits the budget with headroom to spare.
    """
    budget = 999

    class LimitedConnection:
        getlimit = staticmethod(lambda which: budget)

    effective = store.replay_event_id_limit(LimitedConnection())  # type: ignore[arg-type]
    assert effective == store.MAX_REPLAY_EVENT_IDS  # static cap already fits
    assert effective + store._REPLAY_PARAM_HEADROOM <= budget

    # At the cap: allowed. One above: rejected without touching SQL.
    store.check_replay_event_ids(["x"] * effective, LimitedConnection())  # type: ignore[arg-type]
    with pytest.raises(store.ReplayBoundsError):
        store.check_replay_event_ids(
            ["x"] * (effective + 1), LimitedConnection()  # type: ignore[arg-type]
        )


def test_a_low_variable_limit_tightens_the_cap_below_the_static_default() -> None:
    """A configured limit under 900 must win, or the driver would fail (item D)."""

    class TinyLimitConnection:
        getlimit = staticmethod(lambda which: 300)

    conn = TinyLimitConnection()  # type: ignore[assignment]
    assert store.replay_event_id_limit(conn) == 300 - store._REPLAY_PARAM_HEADROOM
    store.check_replay_event_ids(["x"] * 292, conn)
    with pytest.raises(store.ReplayBoundsError):
        store.check_replay_event_ids(["x"] * 293, conn)


def test_variable_limit_falls_back_when_getlimit_is_unavailable() -> None:
    """CPython < 3.11 has no ``getlimit``; the legacy default is the answer."""

    class OldConnection:
        pass

    assert store.variable_limit(OldConnection()) == store._LEGACY_VARIABLE_LIMIT  # type: ignore[arg-type]
    assert store.replay_event_id_limit(OldConnection()) == store.MAX_REPLAY_EVENT_IDS  # type: ignore[arg-type]


async def test_route_applies_a_tighter_connection_limit_than_the_static_cap(
    replay_stack,
) -> None:
    """The route re-checks against the live connection (item D).

    ``ReplayRequest.max_length`` is the static, portable cap. A connection
    configured with fewer host parameters must still be honoured — that check
    can only happen against the real connection, so it is the route's job.
    """
    stack = replay_stack(failing=False)
    real_conn = stack.app.state.conn

    class LimitedConnection:
        getlimit = staticmethod(lambda which: 300)

        def __getattr__(self, name):  # delegate anything else to the real store
            return getattr(real_conn, name)

    stack.app.state.conn = LimitedConnection()
    try:
        # 500 ids: under the static cap (900), over this connection's 292.
        ids = [str(uuid4()) for _ in range(500)]
        resp = await stack.client.post("/admin/dlq/replay", json={"event_ids": ids})
        assert resp.status_code == 422
        assert "at most 292" in resp.text
    finally:
        stack.app.state.conn = real_conn
        await stack.aclose()


# --- /stats ----------------------------------------------------------------


async def test_stats_reports_durable_counts_and_breaker_state(replay_stack) -> None:
    stack = replay_stack(failing=False)
    try:
        processed = (await stack.client.post("/calls", json=event_payload())).json()
        assert processed["status"] == "processed"
        stack.backend.failing = True
        await stack.fail_events(1)

        body = (await stack.client.get("/stats")).json()
        assert body["events"]["processed"] == 1
        assert body["events"]["failed_downstream"] == 1
        assert body["events"]["deferred"] == 0
        assert body["dead_letter"] == {"pending": 1, "replayed": 0}
        assert body["deferred_pending"] == 0
        assert body["circuit_breaker"]["state"] == "closed"
        assert body["uptime_s"] >= 0

        percentiles = body["latency_percentiles"]
        assert set(percentiles) == {"p50", "p90", "p99"}
        assert percentiles["p50"] is not None
        assert percentiles["p99"] >= percentiles["p50"]
    finally:
        await stack.aclose()


def test_interpolate_quantile_handles_finite_and_open_ended_buckets() -> None:
    """The histogram estimate: interpolate in-bucket, clamp at ``+Inf``."""
    buckets = [(50.0, 1.0), (100.0, 3.0), (float("inf"), 4.0)]
    # q=0.5 → target 2.0, inside the (50, 100] bucket, half-way through it.
    assert interpolate_quantile(buckets, 4.0, 0.5) == pytest.approx(75.0)
    # q=0.9 → target 3.6, only the open bucket holds it: last finite bound.
    assert interpolate_quantile(buckets, 4.0, 0.9) == pytest.approx(100.0)
