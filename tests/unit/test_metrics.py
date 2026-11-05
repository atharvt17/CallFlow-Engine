"""Metrics surface tests (M2-T10, SPECS §9): series, types, cardinality rule."""

from __future__ import annotations

import re
from uuid import uuid4

ALLOWED_LABELS = {"outcome", "status", "result", "from", "to"}
FORBIDDEN_LABELS = {"event_id", "campaign_id", "borrower_bucket", "product_type", "timestamp"}

_ALL_TYPE_LINES = [
    "# TYPE calls_total counter",
    "# TYPE in_flight_calls gauge",
    "# TYPE pipeline_attempts_total counter",
    "# TYPE retries_total counter",
    "# TYPE pipeline_attempt_latency_ms histogram",
    "# TYPE e2e_latency_ms histogram",
    "# TYPE circuit_state gauge",
    "# TYPE circuit_transitions_total counter",
    "# TYPE deferred_pending gauge",
    "# TYPE deferred_queue_depth gauge",
    "# TYPE deferred_expired_total counter",
    "# TYPE dlq_entries gauge",
    "# TYPE dlq_replays_total counter",
]


def event_payload() -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
        "product_type": "bnpl",
    }


async def test_all_series_present_and_populated(make_stack) -> None:
    """All §9 series exist with correct types; traffic populates the live ones."""
    async with make_stack(mock_overrides={"failure_rate": 0.0}) as (client, _db_path):
        for _ in range(3):
            resp = await client.post("/calls", json=event_payload())
            assert resp.status_code == 200

        scrape = await client.get("/metrics")
        assert scrape.status_code == 200
        assert scrape.headers["content-type"].startswith("text/plain")
        text = scrape.text

        for type_line in _ALL_TYPE_LINES:
            assert type_line in text, f"missing series declaration: {type_line}"

        # Synchronous processing leaves zero calls in flight.
        assert re.search(r"^in_flight_calls 0\.0$", text, re.MULTILINE)
        # Processed traffic populates outcome/status split and attempt stats.
        assert re.search(
            r'^calls_total\{outcome="connected",status="processed"\} 3\.0$', text, re.MULTILINE
        )
        assert re.search(r'^pipeline_attempts_total\{result="success"\} 3\.0$', text, re.MULTILINE)
        assert re.search(r"^pipeline_attempt_latency_ms_count 3\.0$", text, re.MULTILINE)
        assert re.search(r"^e2e_latency_ms_count 3\.0$", text, re.MULTILINE)
        assert re.search(r"^retries_total 0\.0$", text, re.MULTILINE)  # Sprint 3 populates
        # Breaker/deferral/DLQ gauges registered with safe placeholders.
        assert re.search(r"^circuit_state 0\.0$", text, re.MULTILINE)
        assert re.search(r"^deferred_pending 0\.0$", text, re.MULTILINE)
        assert re.search(r"^deferred_queue_depth 0\.0$", text, re.MULTILINE)
        assert re.search(r"^dlq_entries\{status=\"pending\"\} 0\.0$", text, re.MULTILINE)


async def test_failed_calls_populate_failure_series(make_stack) -> None:
    async with make_stack(mock_overrides={"failure_rate": 1.0}) as (client, _db_path):
        for _ in range(2):
            resp = await client.post("/calls", json=event_payload())
            assert resp.status_code == 200  # terminal failure, not HTTP error

        text = (await client.get("/metrics")).text
        assert re.search(
            r'^calls_total\{outcome="failed_downstream",status="failed_downstream"\} 2\.0$',
            text,
            re.MULTILINE,
        )
        assert re.search(r'^pipeline_attempts_total\{result="error"\} 2\.0$', text, re.MULTILINE)


def test_cardinality_rule_no_identifiers_in_labels() -> None:
    """No event IDs or borrower attributes may ever become metric labels."""
    from app.metrics import Metrics

    metrics = Metrics()
    labeled = (
        metrics.calls_total,
        metrics.pipeline_attempts_total,
        metrics.circuit_transitions_total,
        metrics.dlq_entries,
        metrics.dlq_replays_total,
    )
    for collector in labeled:
        labelnames = set(collector._labelnames)  # tuple attribute in prometheus_client
        assert labelnames, "labeled collectors must declare their labels"
        assert labelnames <= ALLOWED_LABELS, f"{collector._name}: {labelnames}"
        assert not labelnames & FORBIDDEN_LABELS
