"""Success-path latency sampling for the Appendix B mock (SPECS §4).

``sample_latency_ms`` is a pure function over ``random.lognormvariate`` so the
endpoint (M1-T02) and the calibration tests (M1-T06) share one definition.
The shipped defaults (``mu=5.7``, ``sigma=0.62``) are the calibrated
approximation of Appendix B's targets (p50 ≈ 300 ms, p90 ≈ 700 ms, p99 ≈
1200 ms); Appendix B explicitly sanctions adjusting ``sigma`` after checking
observed percentiles.
"""

from __future__ import annotations

import math
import random
from collections.abc import Sequence


def sample_latency_ms(mu: float, sigma: float) -> float:
    """Sample one success-path latency in milliseconds (always positive)."""
    return random.lognormvariate(mu, sigma)


def percentiles(samples: Sequence[float], qs: Sequence[float]) -> list[float]:
    """Empirical percentiles of ``samples`` at fractions ``qs`` (0.0–1.0).

    Linear interpolation between adjacent order statistics (numpy's default
    method), so results are stable for small and large sample sizes alike.
    """
    ordered = sorted(samples)
    n = len(ordered)
    if n == 0:
        raise ValueError("percentiles() requires at least one sample")
    values: list[float] = []
    for q in qs:
        if not 0.0 <= q <= 1.0:
            raise ValueError(f"quantile out of range [0, 1]: {q!r}")
        pos = q * (n - 1)
        lo = math.floor(pos)
        hi = math.ceil(pos)
        if lo == hi:
            values.append(float(ordered[lo]))
        else:
            frac = pos - lo
            values.append(float(ordered[lo] + (ordered[hi] - ordered[lo]) * frac))
    return values
