"""Generate synthetic Appendix A call events -> ``data/sample_events.json``.

**Synthetic data only — never real borrower information.**

Documented choices (SPECS §3, PLAN M2):

- **Outcome distribution** follows the documented normalization of the
  overlapping Appendix A instructions: ``failed_downstream`` is exactly 20%
  (mirroring Appendix B's injection rate) and the 60/20/15/5 split applies to
  the remaining 80% → connected 48% / no_answer 16% / ptp_taken 12% /
  dispute 4% of the total. Counts are exact integers (no sampling noise).
- ``borrower_bucket`` and ``product_type`` are sampled **uniformly**.
- ``call_duration_s`` is outcome-aware for realism: answered calls
  (connected / ptp_taken) draw 30–600 s, unanswered/others 0–15 s.
- ``retry_count`` mirrors Appendix A's sample record: 2 for
  ``failed_downstream`` (⇒ 3 attempts), 0 otherwise. Audit data only — the
  service never trusts it.
- Timestamps are RFC3339 ``isoformat()`` instants randomized uniformly across
  the Appendix C campaign day (fixed ``CAMP-2026-09-A`` day: 2026-09-01 UTC).
- Determinism: the RNG (and the derived v4 UUIDs) are seeded from ``--seed``,
  so the same seed reproduces the exact same file. Every record is validated
  through the same ``CallEvent`` model the API uses — any invalid record fails
  loudly instead of being written.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from app.models import BorrowerBucket, CallEvent, EventOutcome, ProductType

CAMPAIGN_ID = "CAMP-2026-09-A"
CAMPAIGN_DAY = datetime(2026, 9, 1, tzinfo=UTC)
DAY_SECONDS = 24 * 3600

OUTCOME_SHARES: dict[EventOutcome, float] = {
    EventOutcome.CONNECTED: 0.48,
    EventOutcome.NO_ANSWER: 0.16,
    EventOutcome.PTP_TAKEN: 0.12,
    EventOutcome.DISPUTE: 0.04,
}
FAILED_DOWNSTREAM_SHARE = 0.20

ANSWERED_OUTCOMES = (EventOutcome.CONNECTED, EventOutcome.PTP_TAKEN)


def outcome_counts(count: int) -> dict[EventOutcome, int]:
    """Exact integer counts per outcome for ``count`` records.

    ``OUTCOME_SHARES`` are fractions of the *total*; after carving off the
    20% ``failed_downstream`` share, the remaining 80% is split
    proportionally (connected 60% of remaining, no_answer 20%, ptp_taken
    15%, dispute 5% remainder).
    """
    counts: dict[EventOutcome, int] = {}
    remaining = count
    failed = round(count * FAILED_DOWNSTREAM_SHARE)
    counts[EventOutcome.FAILED_DOWNSTREAM] = failed
    remaining -= failed
    assigned = 0
    shares = list(OUTCOME_SHARES.items())
    for outcome, share in shares[:-1]:
        n = round(remaining * share / (1 - FAILED_DOWNSTREAM_SHARE))
        counts[outcome] = n
        assigned += n
    counts[shares[-1][0]] = remaining - assigned  # remainder keeps the sum exact
    return counts


def random_timestamp(rng: random.Random) -> datetime:
    return CAMPAIGN_DAY + timedelta(seconds=rng.randrange(DAY_SECONDS))


def random_uuid(rng: random.Random) -> UUID:
    """Seeded RFC4122 v4 UUID (uuid.uuid4() uses OS entropy and would break determinism)."""
    return UUID(int=rng.getrandbits(128), version=4)


def build_record(rng: random.Random, outcome: EventOutcome) -> dict[str, object]:
    answered = outcome in ANSWERED_OUTCOMES
    return {
        "event_id": str(random_uuid(rng)),
        "campaign_id": CAMPAIGN_ID,
        "borrower_bucket": rng.choice(list(BorrowerBucket)).value,
        "timestamp": random_timestamp(rng).isoformat(),
        "outcome": outcome.value,
        "product_type": rng.choice(list(ProductType)).value,
        "call_duration_s": rng.randrange(30, 601) if answered else rng.randrange(0, 16),
        "retry_count": 2 if outcome is EventOutcome.FAILED_DOWNSTREAM else 0,
    }


def generate(count: int, seed: int) -> list[dict[str, object]]:
    """Build ``count`` synthetic records, validated through the API model."""
    rng = random.Random(seed)
    records: list[dict[str, object]] = []
    for outcome, n in outcome_counts(count).items():
        records.extend(build_record(rng, outcome) for _ in range(n))
    rng.shuffle(records)

    validated: list[dict[str, object]] = []
    for record in records:
        event = CallEvent.model_validate(record)  # fail loudly on any invalid record
        validated.append(event.model_dump(mode="json"))
    return validated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic Appendix A call events (synthetic data only)."
    )
    parser.add_argument("--count", type=int, default=500, help="number of records")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (deterministic output)")
    parser.add_argument(
        "--out", type=Path, default=Path("data/sample_events.json"), help="output JSON path"
    )
    args = parser.parse_args()

    records = generate(args.count, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(records, indent=2), encoding="utf-8")
    print(f"wrote {len(records)} synthetic events to {args.out} (seed={args.seed})")


if __name__ == "__main__":
    main()
