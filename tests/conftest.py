"""Shared test fixtures for the collections-voice-agent test suite."""

from __future__ import annotations

import atexit
import sys
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

if sys.platform == "win32":
    # Windows' default system timer tick (~15.6 ms) floors tiny asyncio.sleep
    # calls at ~12 ms regardless of loop type, which would add ~50 s to the
    # 5,000-request statistical test even with a sub-millisecond latency
    # budget. Raising the process timer resolution to 1 ms floors them at
    # ~1.4 ms instead. Test-infra only: production keeps OS defaults, and the
    # resolution change is process-local (undone at interpreter exit).
    import ctypes

    _timer_resolution_set = ctypes.windll.winmm.timeBeginPeriod(1)
    atexit.register(ctypes.windll.winmm.timeEndPeriod, 1)


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
