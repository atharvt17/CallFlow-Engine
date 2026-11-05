"""Appendix A call-event model (SPECS §3) — the single source of truth.

One pydantic model shared by API validation, the sample-data generator, and
the tests, so generated data and live validation are definitionally
compatible. Required/optional split and enums verified against the challenge
brief (Appendix A JSON Schema: ``event_id`` is ``format: uuid`` — the brief's
illustrative sample ids are not conformant; SPECS §3 mandates UUID parsing).

``retry_count`` is accepted for Appendix A compatibility but is **never
authoritative**: the service computes its own attempt accounting
(``retry_count = attempts - 1``); incoming values are preserved as audit data
only. ``timestamp`` is parsed into a tz-aware datetime and stored in
normalized RFC3339 (``isoformat``) — the instant is preserved.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class BorrowerBucket(str, Enum):
    """Delinquency buckets (Appendix A enum, verbatim)."""

    DPD_0_30 = "0-30 DPD"
    DPD_31_60 = "31-60 DPD"
    DPD_61_90 = "61-90 DPD"
    DPD_90_PLUS = "90+ DPD"


class ProductType(str, Enum):
    """Product types (Appendix A enum, verbatim)."""

    CREDIT_CARD = "credit_card"
    BNPL = "bnpl"
    PERSONAL_LOAN = "personal_loan"


class EventOutcome(str, Enum):
    """Call outcomes (Appendix A enum, verbatim)."""

    CONNECTED = "connected"
    NO_ANSWER = "no_answer"
    PTP_TAKEN = "ptp_taken"
    DISPUTE = "dispute"
    FAILED_DOWNSTREAM = "failed_downstream"


class CallEvent(BaseModel):
    """One collections call event, exactly per Appendix A / SPECS §3."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    campaign_id: str
    borrower_bucket: BorrowerBucket
    timestamp: datetime
    outcome: EventOutcome
    product_type: ProductType | None = None
    call_duration_s: int | None = Field(default=None, ge=0)
    retry_count: int | None = Field(default=None, ge=0)
