"""Guarantees on the committed sample corpus (M2-T12; SPECS §3, §12).

Appendix A coverage is proven, not claimed: every record in the committed
``data/sample_events.json`` validates through the same ``CallEvent`` model
the API uses, timestamps stay inside the Appendix C campaign day, and the
outcome distribution matches the documented normalization (48/16/12/4/20)
within ±5 percentage points.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.models import CallEvent

SAMPLE_PATH = Path(__file__).resolve().parents[2] / "data" / "sample_events.json"
CAMPAIGN_DAY_START = datetime(2026, 9, 1, tzinfo=UTC)
CAMPAIGN_DAY_END = datetime(2026, 9, 2, tzinfo=UTC)

EXPECTED_SHARES = {
    "connected": 0.48,
    "no_answer": 0.16,
    "ptp_taken": 0.12,
    "dispute": 0.04,
    "failed_downstream": 0.20,
}
SHARE_TOLERANCE = 0.05  # ±5 percentage points (SPECS §11)


@pytest.fixture(scope="module")
def records() -> list[dict[str, object]]:
    return json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))


def test_exactly_500_records(records: list[dict[str, object]]) -> None:
    assert len(records) == 500


def test_every_record_validates_via_call_event(
    records: list[dict[str, object]],
) -> None:
    for record in records:
        event = CallEvent.model_validate(record)  # raises on any schema violation
        assert event.event_id is not None


def test_all_uuids_unique(records: list[dict[str, object]]) -> None:
    ids = [record["event_id"] for record in records]
    assert len(set(ids)) == len(ids), "event_id duplicates would break idempotency"


def test_all_timestamps_within_campaign_window(
    records: list[dict[str, object]],
) -> None:
    for record in records:
        # CallEvent's own validation already parsed these (RFC3339).
        ts = CallEvent.model_validate(record).timestamp
        assert CAMPAIGN_DAY_START <= ts < CAMPAIGN_DAY_END


def test_campaign_id_is_fixed(records: list[dict[str, object]]) -> None:
    assert {record["campaign_id"] for record in records} == {"CAMP-2026-09-A"}


def test_outcome_distribution_matches_documented_interpretation(
    records: list[dict[str, object]],
) -> None:
    counts = Counter(record["outcome"] for record in records)
    total = len(records)
    for outcome, share in EXPECTED_SHARES.items():
        actual = counts[outcome] / total
        assert math.isclose(actual, share, abs_tol=SHARE_TOLERANCE), (
            f"{outcome}: {actual:.3f} vs expected {share:.2f} (±{SHARE_TOLERANCE})"
        )
    assert sum(counts.values()) == total
