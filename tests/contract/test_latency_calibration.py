"""Latency calibration against the Appendix B targets (M1-T06).

Layers:

1. Pure sampler statistics (no sleeping): n=50,000 draws with the shipped
   defaults (mu=5.7, sigma=0.62) must land inside the SPECS §4 bands
   (p50 300±10%, p90 700±15%, p99 1200±25%).
2. Endpoint wiring check: the same sigma through the HTTP endpoint at a
   sub-millisecond mu (fast) — percentile RATIOS must match the lognormal
   shape (p90/p50 = exp(sigma*z90) ≈ 2.214, p99/p50 = exp(sigma*z99) ≈ 4.231
   for sigma=0.62), proving LATENCY_MU/LATENCY_SIGMA reach the sampler
   through the route (mu cancels in the ratios, so the shape check is
   mu-independent).

These tests are **read-only**: running the suite never modifies the working
tree. The measured table in ``reports/latency_calibration.md`` is produced by
``make calibration-report`` (``tools/calibration_report.py``), which owns the
targets, bands and sampler configuration below — the test imports them from
there, so the artifact and the assertions can never disagree.

If a band ever fails, SPECS §4 instructs tuning sigma and updating both the
shipped default and this report; today sigma=0.62 passes all three bands.
"""

from __future__ import annotations

from mock.latency import percentiles, sample_latency_ms
from tools.calibration_report import MU, N_SAMPLER, SIGMA, TARGETS, band_verdict

# Endpoint wiring check: n=10,000 keeps the p99-ratio standard error ≈ 2.5%,
# so the ±15% shape band is a ~6σ margin. At n=3,000 the p99 estimator (only
# ~30 samples beyond the quantile) is heavy-tailed and the band flaked
# (observed p99/p50 = 5.153 vs target 4.231 on one run).
N_ENDPOINT = 10_000

# Lognormal shape ratios for sigma=0.62: exp(sigma * z_q), z90≈1.2816, z99≈2.3264.
RATIO_TARGETS: dict[str, float] = {"p90/p50": 2.214, "p99/p50": 4.231}
RATIO_BAND = 0.15


def test_sampler_percentiles_within_appendix_b_bands() -> None:
    samples = [sample_latency_ms(MU, SIGMA) for _ in range(N_SAMPLER)]
    p50, p90, p99 = percentiles(samples, [0.5, 0.9, 0.99])
    measured = {"p50": p50, "p90": p90, "p99": p99}

    for name, value in measured.items():
        target, band = TARGETS[name]
        verdict = band_verdict(value, target, band)
        print(
            f"{name}: measured {value:.1f} ms vs target {target:g} "
            f"±{band * 100:.0f}% -> {verdict}"
        )
        assert verdict == "PASS", (
            f"{name}={value:.1f} outside {target:g}±{band * 100:.0f}% "
            f"(SPECS §4 would require tuning sigma)"
        )


async def test_endpoint_latency_shape_ratios(make_mock_client) -> None:
    """Endpoint percentiles must follow the lognormal shape (wiring proof)."""
    latencies: list[float] = []
    async with make_mock_client(failure_rate=0.0) as client:
        for i in range(N_ENDPOINT):
            resp = await client.post(
                "/pipeline/process", json={"event_id": str(i), "audio_ref": "synthetic"}
            )
            assert resp.status_code == 200
            latencies.append(resp.json()["latency_ms"])

    p50, p90, p99 = percentiles(latencies, [0.5, 0.9, 0.99])
    ratios = {"p90/p50": p90 / p50, "p99/p50": p99 / p50}
    for name, value in ratios.items():
        target = RATIO_TARGETS[name]
        print(
            f"endpoint {name}: {value:.3f} "
            f"(shape target {target:.3f} ±{RATIO_BAND * 100:.0f}%)"
        )
        assert target * (1 - RATIO_BAND) <= value <= target * (1 + RATIO_BAND), (
            f"endpoint {name}={value:.3f} outside shape band — "
            f"LATENCY_MU/LATENCY_SIGMA may not reach the sampler"
        )
