"""Admin breaker snapshot endpoint (M3-T11; SPECS §5, Appendix I runbook).

``GET /admin/circuit-breaker`` must return the live breaker snapshot so an
operator can answer "is the breaker open, and when did that happen?" without
reading logs. Transition *evidence* (counters, forced-open scenarios) is
asserted end-to-end in ``tests/integration/test_resilience.py`` (M3-T15).
"""

from __future__ import annotations

SNAPSHOT_KEYS = {
    "state",
    "window_size",
    "failure_ratio",
    "consecutive_failures",
    "opened_at",
    "last_transition_at",
}


async def test_admin_snapshot_shape_and_initial_state(make_stack) -> None:
    async with make_stack() as (client, _db_path):
        resp = await client.get("/admin/circuit-breaker")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body) == SNAPSHOT_KEYS
        assert body["state"] == "closed"
        assert body["window_size"] == 20
        assert body["failure_ratio"] == 0.0
        assert body["consecutive_failures"] == 0
        assert body["opened_at"] is None
        assert body["last_transition_at"] is None


async def test_healthz_reports_live_breaker_state(make_stack) -> None:
    """The health placeholder is replaced by the real breaker (M3-T11)."""
    async with make_stack() as (client, _db_path):
        resp = await client.get("/healthz")
        assert resp.json()["circuit_state"] == "closed"
