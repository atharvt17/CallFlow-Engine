"""Validation contract lock for the Appendix A model (M2-T02, SPECS §3)."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from app.models import BorrowerBucket, CallEvent, EventOutcome, ProductType

EVENT_ID = "a1f2c3d4-e89b-12d3-a456-426614174000"


def _base_payload() -> dict[str, object]:
    """Minimal valid Appendix A event (required fields only)."""
    return {
        "event_id": EVENT_ID,
        "campaign_id": "CAMP-2026-09-A",
        "borrower_bucket": "0-30 DPD",
        "timestamp": "2026-09-01T09:03:12Z",
        "outcome": "connected",
    }


def test_minimal_valid_event_parses() -> None:
    event = CallEvent.model_validate(_base_payload())
    assert event.event_id == UUID(EVENT_ID)
    assert event.campaign_id == "CAMP-2026-09-A"
    assert event.borrower_bucket is BorrowerBucket.DPD_0_30
    assert event.outcome is EventOutcome.CONNECTED
    assert event.timestamp.isoformat() == "2026-09-01T09:03:12+00:00"  # RFC3339 preserved as instant
    assert event.product_type is None
    assert event.call_duration_s is None
    assert event.retry_count is None


def test_full_valid_event_parses() -> None:
    payload = _base_payload() | {
        "borrower_bucket": "90+ DPD",
        "outcome": "failed_downstream",
        "product_type": "credit_card",
        "call_duration_s": 112,
        "retry_count": 2,
    }
    event = CallEvent.model_validate(payload)
    assert event.borrower_bucket is BorrowerBucket.DPD_90_PLUS
    assert event.outcome is EventOutcome.FAILED_DOWNSTREAM
    assert event.product_type is ProductType.CREDIT_CARD
    assert event.call_duration_s == 112
    assert event.retry_count == 2


def test_zero_call_duration_is_valid() -> None:
    event = CallEvent.model_validate(_base_payload() | {"call_duration_s": 0})
    assert event.call_duration_s == 0


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("borrower_bucket", "31-70 DPD"),
        ("borrower_bucket", "0-30dpd"),
        ("borrower_bucket", 30),
        ("product_type", "mortgage"),
        ("product_type", "CREDIT_CARD"),
        ("outcome", "hung_up"),
        ("outcome", "no answer"),
    ],
)
def test_out_of_enum_values_rejected(field: str, bad_value: object) -> None:
    payload = _base_payload() | {field: bad_value}
    with pytest.raises(ValidationError) as excinfo:
        CallEvent.model_validate(payload)
    assert field in str(excinfo.value), "error must name the offending field"


def test_malformed_uuid_rejected() -> None:
    # Appendix A's illustrative sample ids ("a1f2c3d4-0002") are not UUIDs.
    payload = _base_payload() | {"event_id": "a1f2c3d4-0002"}
    with pytest.raises(ValidationError) as excinfo:
        CallEvent.model_validate(payload)
    assert "event_id" in str(excinfo.value)


def test_malformed_timestamp_rejected() -> None:
    payload = _base_payload() | {"timestamp": "not-a-date"}
    with pytest.raises(ValidationError) as excinfo:
        CallEvent.model_validate(payload)
    assert "timestamp" in str(excinfo.value)


def test_unknown_field_rejected() -> None:
    payload = _base_payload() | {"extra": 1}
    with pytest.raises(ValidationError) as excinfo:
        CallEvent.model_validate(payload)
    assert "extra" in str(excinfo.value)


def test_missing_required_field_rejected() -> None:
    for field in ("event_id", "campaign_id", "borrower_bucket", "timestamp", "outcome"):
        payload = _base_payload()
        del payload[field]
        with pytest.raises(ValidationError) as excinfo:
            CallEvent.model_validate(payload)
        assert field in str(excinfo.value)


@pytest.mark.parametrize("field", ["call_duration_s", "retry_count"])
def test_negative_minimums_rejected(field: str) -> None:
    payload = _base_payload() | {field: -1}
    with pytest.raises(ValidationError) as excinfo:
        CallEvent.model_validate(payload)
    assert field in str(excinfo.value)


def test_incoming_retry_count_accepted_but_not_authoritative() -> None:
    """`retry_count` parses (audit data) but the service owns attempt accounting.

    The authoritative value is always computed as `attempts - 1` by the
    service (SPECS §3); the model merely preserves what arrived.
    """
    event = CallEvent.model_validate(_base_payload() | {"retry_count": 5})
    assert event.retry_count == 5  # preserved for audit
    # ...and the model carries no behavior deriving attempts from it.
