"""Shared test fixtures for the collections-voice-agent test suite."""

from __future__ import annotations

import atexit
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

from app import store
from app.config import Settings
from app.main import create_app
from app.pipeline.http_client import HttpPipelineClient
from mock.config import MockSettings
from mock.main import create_mock_app

# Sub-millisecond success latency for HTTP-level tests: contract and
# statistical tests are shape/rate-focused; absolute latency is calibrated
# separately (M1-T06) with the pure sampler.
FAST_LATENCY_MU = -3.0


@pytest.fixture
def make_stack(tmp_path):
    """Factory fixture: inference ASGI client wired to an in-process mock app.

    The inference's HTTP backend talks to the mock through transport
    injection (``HttpPipelineClient(settings, transport=ASGITransport(mock))``),
    so the full POST /calls -> backend -> mock -> store flow runs without any
    live server. Yields ``(httpx client, db_path)`` for accounting asserts.
    """

    @asynccontextmanager
    async def _stack(
        mock_overrides: dict[str, float] | None = None,
        **inference_overrides: float | str,
    ) -> AsyncIterator[tuple[httpx.AsyncClient, Path]]:
        mock_app = create_mock_app(
            MockSettings(latency_mu=FAST_LATENCY_MU, **(mock_overrides or {}))
        )
        db_path = tmp_path / "calls.db"
        settings = Settings(
            db_path=str(db_path),
            pipeline_backend="http",
            pipeline_base_url="http://mock.test",
            **inference_overrides,
        )
        backend = HttpPipelineClient(settings, transport=httpx.ASGITransport(app=mock_app))
        app = create_app(settings, backend)
        conn = app.state.conn
        try:
            # Note: httpx.ASGITransport does NOT run the app lifespan, so the
            # store connection and backend pool are closed here explicitly —
            # unclosed sqlite connections would otherwise raise
            # ResourceWarning at GC time (escalated to errors by pytest).
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://inference.test"
            ) as client:
                yield client, db_path
        finally:
            await backend.aclose()
            conn.close()

    return _stack


@pytest.fixture
def tmp_db(tmp_path):
    """Fresh temp SQLite store per test (SPECS §7 pragmas + schema)."""
    conn = store.connect(tmp_path / "test.db")
    store.init_db(conn)
    yield conn
    conn.close()

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
