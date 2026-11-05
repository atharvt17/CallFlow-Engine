"""Statistical proof of the 20% independent failure injection (M1-T05).

Fires 5,000 sequential requests at the shipped default ``FAILURE_RATE=0.20``
(sub-millisecond success latency via the conftest fixture) and asserts:

- observed failure rate within 0.20 ± 0.03 (≈ ±5.3σ at n=5,000, σ≈0.0057);
- independence: the longest consecutive failure run stays well below
  correlated-burst levels (loose bound: < 15; typical max ≈ 5–10 draws).
"""

from __future__ import annotations

import time

import pytest

N_REQUESTS = 5000
FAILURE_RATE_BAND = 0.03
MAX_EXPECTED_RUN = 15


def _longest_run(codes: list[int], code: int) -> int:
    longest = current = 0
    for c in codes:
        current = current + 1 if c == code else 0
        longest = max(longest, current)
    return longest


async def test_failure_rate_within_band(make_mock_client, capsys: pytest.CaptureFixture[str]) -> None:
    started = time.perf_counter()
    codes: list[int] = []
    async with make_mock_client() as client:  # default config: failure_rate=0.20
        for i in range(N_REQUESTS):
            resp = await client.post(
                "/pipeline/process", json={"event_id": str(i), "audio_ref": "synthetic"}
            )
            codes.append(resp.status_code)
    elapsed = time.perf_counter() - started

    successes = codes.count(200)
    failures = codes.count(503)
    assert successes + failures == N_REQUESTS, "unexpected status codes observed"
    rate = failures / N_REQUESTS
    longest = _longest_run(codes, 503)

    # Evidence goes into the test output (explicit Sprint 1 requirement).
    print(
        f"\nobserved failure rate over {N_REQUESTS} requests: {rate:.4f} "
        f"(band 0.20 ± {FAILURE_RATE_BAND}); longest consecutive failure run: {longest}"
    )
    print(f"test wall time: {elapsed:.1f}s (must stay well under 60s)")

    assert 0.20 - FAILURE_RATE_BAND <= rate <= 0.20 + FAILURE_RATE_BAND
    assert longest < MAX_EXPECTED_RUN, f"failure run of {longest} suggests correlated bursts"
    assert elapsed < 60, "statistical test must run fast (sub-millisecond latency fixture)"
