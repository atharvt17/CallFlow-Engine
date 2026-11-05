"""Byte-level contract lock on the graded mock endpoint (M1-T04, SPECS §4).

No tolerance on body shape: success responses carry exactly the four contract
keys with the exact literal values; failure responses are compared with full
dict equality.
"""

from __future__ import annotations

_SUCCESS_KEYS = {"status", "transcript", "response_audio_ref", "latency_ms"}


async def test_success_body_exact(make_mock_client) -> None:
    async with make_mock_client(failure_rate=0.0) as client:
        resp = await client.post(
            "/pipeline/process", json={"event_id": "e-1", "audio_ref": "synthetic"}
        )
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == _SUCCESS_KEYS  # exactly the four keys — no extras, none missing
    assert body["status"] == "ok"
    assert body["transcript"] == "synthetic transcript"
    assert body["response_audio_ref"] == "synthetic"
    assert isinstance(body["latency_ms"], (int, float))  # int-valued float ok
    assert body["latency_ms"] > 0


async def test_forced_failure_body_exact(make_mock_client) -> None:
    async with make_mock_client(failure_rate=1.0) as client:
        resp = await client.post(
            "/pipeline/process", json={"event_id": "e-1", "audio_ref": "synthetic"}
        )
    assert resp.status_code == 503
    assert resp.json() == {"status": "error", "reason": "downstream_timeout"}


async def test_malformed_json_is_400(make_mock_client) -> None:
    async with make_mock_client() as client:
        resp = await client.post(
            "/pipeline/process",
            content=b"{not json",
            headers={"content-type": "application/json"},
        )
    assert resp.status_code == 400
    assert "detail" in resp.json()


async def test_missing_event_id_is_400(make_mock_client) -> None:
    async with make_mock_client() as client:
        resp = await client.post("/pipeline/process", json={"audio_ref": "synthetic"})
    assert resp.status_code == 400
    assert "detail" in resp.json()


async def test_missing_audio_ref_is_400(make_mock_client) -> None:
    async with make_mock_client() as client:
        resp = await client.post("/pipeline/process", json={"event_id": "e-1"})
    assert resp.status_code == 400
    assert "detail" in resp.json()


async def test_unknown_route_is_404(make_mock_client) -> None:
    async with make_mock_client() as client:
        resp = await client.get("/definitely/not/here")
    assert resp.status_code == 404


async def test_defaults_are_appendix_b_exact(make_mock_client) -> None:
    """The shipped mock defaults must be untouched SPECS §4/§8 values.

    ``fast_latency=False`` builds the app from pure defaults; this test only
    reads ``/admin/config`` (no pipeline traffic), so no real latency sleeps.
    """
    async with make_mock_client(fast_latency=False) as client:
        resp = await client.get("/admin/config")
    assert resp.json() == {
        "failure_rate": 0.2,
        "latency_mu": 5.7,
        "latency_sigma": 0.62,
        "failure_latency_ms": 0.0,
    }


async def test_admin_config_forces_stats_and_resets(make_mock_client) -> None:
    """Documented extension behavior (M1-T03): force → confirm → stats → reset."""
    async with make_mock_client() as client:
        resp = await client.post("/admin/config", json={"failure_rate": 1.0})
        assert resp.status_code == 200
        assert resp.json()["failure_rate"] == 1.0

        codes = set()
        for i in range(20):
            resp = await client.post(
                "/pipeline/process", json={"event_id": str(i), "audio_ref": "synthetic"}
            )
            codes.add(resp.status_code)
        assert codes == {503}  # every request fails under the forced config

        stats = (await client.get("/admin/stats")).json()
        assert stats["requests_total"] == 20
        assert stats["failure"] == 20
        assert stats["success"] == 0
        assert stats["observed_failure_rate"] == 1.0

        resp = await client.post("/admin/config", json={"failure_rate": 0.20})
        assert resp.status_code == 200
        assert resp.json()["failure_rate"] == 0.2  # reset round-trips


async def test_admin_config_rejects_invalid_and_unknown(make_mock_client) -> None:
    async with make_mock_client() as client:
        resp = await client.post("/admin/config", json={"failure_rate": 1.5})
        assert resp.status_code in (400, 422)
        resp = await client.post("/admin/config", json={"failure_rate": 0.5, "bogus": 1})
        assert resp.status_code in (400, 422)
