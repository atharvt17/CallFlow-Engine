"""Service wiring shared by the HTTP app and the CLI (M4-T02; SPECS §2).

Both surfaces must run the *same* pipeline: the CLI's ``dlq replay`` has to go
through the same retry/breaker/deferral stack as ``POST /admin/dlq/replay``, or
a CLI replay would be a compliance hole (SPECS §7.2 "identical code path").
Building that stack twice would guarantee eventual drift, so it is assembled
here once and used by :func:`app.main.create_app` and
:func:`app.cli.build_runtime`.

The runtime is deliberately process-scoped: the circuit breaker and metrics
live in memory. A CLI process therefore has its **own** breaker — it does not
share the serving process's state. That is inherent to Round 1's single-process
design (PLAN D5) and is why the CLI only replays against a database whose
server is not concurrently replaying the same rows (the claims in
:func:`app.store.claim_event_for_replay` still make that safe).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from app import store
from app.config import Settings
from app.metrics import Metrics
from app.pipeline import PipelineBackend, create_backend
from app.pipeline.resilient import ResilientPipelineClient
from app.resilience.circuit_breaker import CircuitBreaker
from app.service import CallService


@dataclass
class ServiceRuntime:
    """One process's store connection + resilience stack + call service."""

    settings: Settings
    conn: sqlite3.Connection
    metrics: Metrics
    breaker: CircuitBreaker
    backend: ResilientPipelineClient
    raw_backend: PipelineBackend
    service: CallService
    owns_connection: bool = True

    @classmethod
    def build(
        cls,
        settings: Settings,
        backend: PipelineBackend | None = None,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> ServiceRuntime:
        """Assemble the stack; ``backend``/``connection`` are injectable for tests.

        An injected ``connection`` is not owned by the runtime — the caller
        closes it (this is how the test suite shares one connection).
        """
        if backend is None:
            backend = create_backend(settings)
        conn = connection if connection is not None else store.connect(settings.db_path)
        store.init_db(conn)
        metrics = Metrics()
        breaker = CircuitBreaker.from_settings(settings, metrics=metrics)
        # Resilience wraps the boundary (M3-T14): retry + timeout + breaker
        # admission live here, so concrete backends stay policy-free.
        resilient = ResilientPipelineClient.from_settings(
            backend, breaker, settings, metrics=metrics
        )
        service = CallService(conn, resilient, settings, metrics)
        return cls(
            settings=settings,
            conn=conn,
            metrics=metrics,
            breaker=breaker,
            backend=resilient,
            raw_backend=backend,
            service=service,
            owns_connection=connection is None,
        )

    async def aclose(self) -> None:
        """Close the backend pool and (if owned) the store connection."""
        await self.backend.aclose()
        if self.owns_connection:
            self.conn.close()
