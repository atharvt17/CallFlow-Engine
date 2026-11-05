"""Shared test fixtures for the collections-voice-agent test suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest

from mock.config import MockSettings
from mock.main import create_mock_app

# Sub-millisecond success latency for HTTP-level tests: contract and
# statistical tests are shape/rate-focused; absolute latency is calibrated
# separately (M1-T06) with the pure sampler.
FAST_LATENCY_MU = -3.0


@pytest.fixture
def make_mock_client() -> Any:
    """Factory fixture: in-process httpx client against a configured mock app.

    Keyword overrides map onto ``MockSettings``. By default (``fast_latency=
    True``) ``latency_mu`` is shifted sub-millisecond so tests make no real
    sleeps; pass ``fast_latency=False`` to build an app from untouched
    defaults (safe as long as no ``/pipeline/process`` traffic is driven).
    Note: ``httpx.ASGITransport`` is async-only, hence the ``AsyncClient``.
    """

    @asynccontextmanager
    async def _client(
        fast_latency: bool = True, **settings_overrides: float
    ) -> AsyncIterator[httpx.AsyncClient]:
        if fast_latency:
            settings = MockSettings(latency_mu=FAST_LATENCY_MU, **settings_overrides)
        else:
            settings = MockSettings(**settings_overrides)
        app = create_mock_app(settings)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://mock.test") as client:
            yield client

    return _client
