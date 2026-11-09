"""Resilience engine package (Sprint 3; SPECS §6, PLAN M3).

``retry.py`` holds the retry policy, the retryable classifier, and (from
M3-T03/T04) the backoff calculator and executor. ``circuit_breaker.py``
lands in M3-T07+. Nothing here touches a concrete ``PipelineBackend`` —
the decorator in ``app/pipeline/resilient.py`` (M3-T14) is the only
consumer-facing wiring.
"""
