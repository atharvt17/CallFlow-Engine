"""Appendix C traffic schedule as pure data + pure functions (M5-T01; SPECS §10).

The schedule is one simulated day. Window boundaries are half-open
``[start_s, end_s)`` and contiguous from 00:00 to 24:00, so there is no gap
and no double-counted minute at an edge. Rates are **calls per simulated
minute**; ramps are linear between the window's start and end rate.

Appendix C's table is written as ramps *inside* larger windows
("08:00–10:00 | ramp 10 → 50 over 15 min, hold at 50"); SPECS §10 expands
those into the seven contiguous windows below. The expansion is not a
re-interpretation: integrating it reproduces Appendix C's own arithmetic
(4,800 + 450 + 5,250 + 2,400 + 450 + 5,250 + 4,800 = 23,400 calls/day).

Everything here is pure and independent of the generator, so the schedule can
be verified (and reviewed) without firing a single request.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass

# One simulated day, in seconds. Appendix C's table is minute-granular but the
# generator ticks at sub-second resolution, so seconds are the natural unit.
DAY_SECONDS = 24 * 3600.0
MINUTE_S = 60.0


@dataclass(frozen=True)
class Window:
    """One contiguous schedule segment with a linear rate between its ends.

    ``start_rate == end_rate`` describes a hold (baseline or peak); a differing
    pair describes a ramp. Rates are calls per simulated minute.
    """

    label: str
    start_s: float
    end_s: float
    start_rate: float
    end_rate: float

    def __post_init__(self) -> None:
        if self.end_s <= self.start_s:
            raise ValueError(f"window {self.label!r} has non-positive duration")
        if self.start_rate < 0 or self.end_rate < 0:
            raise ValueError(f"window {self.label!r} has a negative rate")

    @property
    def duration_s(self) -> float:
        return self.end_s - self.start_s

    @property
    def duration_min(self) -> float:
        return self.duration_s / MINUTE_S

    @property
    def mean_rate(self) -> float:
        """Average calls/min over the window (linear ⇒ arithmetic mean)."""
        return (self.start_rate + self.end_rate) / 2

    @property
    def expected_calls(self) -> float:
        """Integral of the rate over the window (exact for a linear ramp)."""
        return self.mean_rate * self.duration_min

    def rate_at(self, seconds_into_day: float) -> float:
        """Rate at an instant inside ``[start_s, end_s]`` (linear interpolation).

        The end point is included so boundary queries have a continuous answer;
        :func:`rate_at` resolves a shared boundary to the *next* window, whose
        start rate equals this window's end rate by construction.
        """
        if seconds_into_day <= self.start_s:
            return self.start_rate
        if seconds_into_day >= self.end_s:
            return self.end_rate
        fraction = (seconds_into_day - self.start_s) / self.duration_s
        return self.start_rate + (self.end_rate - self.start_rate) * fraction


# SPECS §10, verbatim, as contiguous half-open windows. Labels are for the run
# report's per-window throughput table.
WINDOWS: tuple[Window, ...] = (
    Window("00:00-08:00 baseline", 0 * 3600, 8 * 3600, 10.0, 10.0),
    Window("08:00-08:15 ramp", 8 * 3600, 8 * 3600 + 15 * 60, 10.0, 50.0),
    Window("08:15-10:00 peak", 8 * 3600 + 15 * 60, 10 * 3600, 50.0, 50.0),
    Window("10:00-14:00 baseline", 10 * 3600, 14 * 3600, 10.0, 10.0),
    Window("14:00-14:15 ramp", 14 * 3600, 14 * 3600 + 15 * 60, 10.0, 50.0),
    Window("14:15-16:00 peak", 14 * 3600 + 15 * 60, 16 * 3600, 50.0, 50.0),
    Window("16:00-24:00 baseline", 16 * 3600, DAY_SECONDS, 10.0, 10.0),
)

# Day volume used throughout COST.md and the run report (SPECS §10).
EXPECTED_CALLS_PER_DAY = sum(window.expected_calls for window in WINDOWS)

_WINDOW_STARTS = tuple(window.start_s for window in WINDOWS)


def _resolve(seconds_into_day: float) -> Window:
    """Window containing ``seconds_into_day``; last window at exactly 24:00."""
    if not 0.0 <= seconds_into_day <= DAY_SECONDS:
        raise ValueError(
            f"sim time out of range [0, {DAY_SECONDS:g}]: {seconds_into_day!r}"
        )
    # bisect_right maps an exact boundary to the window that starts there
    # (half-open semantics), except at the day's end where it maps past the
    # last window — clamp to the final one.
    index = bisect_right(_WINDOW_STARTS, seconds_into_day) - 1
    return WINDOWS[min(index, len(WINDOWS) - 1)]


def rate_at(seconds_into_day: float) -> float:
    """Target rate (calls per simulated minute) at a simulated instant."""
    return _resolve(seconds_into_day).rate_at(seconds_into_day)


def expected_calls(start_s: float = 0.0, end_s: float = DAY_SECONDS) -> float:
    """Exact expected calls in ``[start_s, end_s)`` (integral of the schedule).

    Used to compare a run's achieved volume against the schedule's own
    arithmetic, independent of Poisson noise.
    """
    if not 0.0 <= start_s <= end_s <= DAY_SECONDS:
        raise ValueError(f"invalid horizon [{start_s!r}, {end_s!r}]")
    total = 0.0
    for window in WINDOWS:
        lo = max(start_s, window.start_s)
        hi = min(end_s, window.end_s)
        if hi <= lo:
            continue
        # Reuse the window's own interpolation so a partial window integrates
        # exactly what rate_at() would have produced inside it.
        total += (window.rate_at(lo) + window.rate_at(hi)) / 2 * (hi - lo) / MINUTE_S
    return total


def windows_upto(horizon_s: float = DAY_SECONDS) -> tuple[Window, ...]:
    """Schedule windows clipped to ``[0, horizon_s)`` — for truncated runs.

    A run shorter than a day is a legitimate demo setting (SPECS §10 permits
    compression; the integration test uses a bounded horizon), and a report
    must not claim throughput for unrun windows.
    """
    if not 0.0 < horizon_s <= DAY_SECONDS:
        raise ValueError(f"horizon out of range (0, {DAY_SECONDS:g}]: {horizon_s!r}")
    clipped: list[Window] = []
    for window in WINDOWS:
        if window.start_s >= horizon_s:
            break
        if window.end_s <= horizon_s:
            clipped.append(window)
        else:
            # Keep the partial segment's true start rate; end rate is the
            # rate the schedule had reached at the horizon.
            clipped.append(
                Window(
                    window.label,
                    window.start_s,
                    horizon_s,
                    window.start_rate,
                    window.rate_at(horizon_s),
                )
            )
    return tuple(clipped)


def compressed_rate(rate_per_min: float, speedup: float) -> float:
    """Simulated calls/min → real calls/min at ``speedup`` (SPECS §10).

    At 60× the 10/min baseline becomes 600/min and the 50/min peak 3,000/min.
    """
    if speedup <= 0:
        raise ValueError(f"speedup must be positive: {speedup!r}")
    return rate_per_min * speedup


def compressed_duration_s(sim_seconds: float, speedup: float) -> float:
    """Simulated seconds → real seconds at ``speedup`` (24 sim-h ⇒ 24 min at 60×)."""
    if speedup <= 0:
        raise ValueError(f"speedup must be positive: {speedup!r}")
    return sim_seconds / speedup
