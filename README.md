# collections-voice-agent — CallFlow Engine

A compliant, resilient collections voice-agent **inference service** wrapped
around a spec-exact mock of the STT→LLM→TTS pipeline
(Predixion AI × TalentX Collections Agent Challenge — Round 1, local track).

> **Status:** implementation in progress per `TASKS.md`. The graded pipeline
> contract (Sprint 1), the resilience engine and persistent deferral
> (Sprints 3–4), DLQ inspect/replay (Sprint 5), and the compressed-day traffic
> generator (Sprint 6) are complete; the cost writeup and release audit
> (Sprint 7) land next.

## Prerequisites

- Docker with Compose v2 (`docker compose version`) — the primary run path
- Python 3.11+ — only needed to run the test suite locally
- GNU Make — for the lifecycle verbs (`make up`, `make test`, …)

## Quickstart

One command starts the whole stack (inference + mock):

```bash
docker compose up --build -d
```

Both services self-report health:

```bash
curl -s http://localhost:8000/healthz   # inference
curl -s http://localhost:8100/healthz   # pipeline mock
```

Stop it:

```bash
docker compose down        # or: make down
```

That is the entire documented setup — no manual steps beyond Docker itself.

## Running the tests locally

```bash
python -m venv .venv
# Windows (Git Bash / PowerShell):
.venv/Scripts/python -m pip install -e ".[dev]"
# Linux/macOS:
#   .venv/bin/python -m pip install -e ".[dev]"
make test    # pytest
make lint    # ruff
```

## Configuration

Every knob is an environment variable with a spec-exact default — see
[`.env.example`](.env.example). Copy it to `.env` to override locally
(`.env` is git-ignored and is **never** baked into the container images).
**No secrets exist in Round 1** — the stack is fully local; there are no
credentials to configure.

`docker compose` configures the containers itself (a local `.env` cannot
fight the compose settings — see the comment in `docker-compose.yml`).

## Make targets

| Target | Effect |
|---|---|
| `make up` | build + start the compose stack (`docker compose up --build -d`) |
| `make down` | stop and remove the stack (named data volume is kept) |
| `make logs` | follow compose logs |
| `make test` | pytest (prefers the project venv) |
| `make lint` | ruff check (prefers the project venv) |
| `make calibration-report` | regenerate `reports/latency_calibration.md` (intentional; tests never write it) |
| `make demo` | compressed Appendix C day replay against the running stack (30×, ~48 real min; writes `reports/run_<ts>.json`) |
| `make dlq-list` | `python -m app.cli dlq list` |
| `make dlq-replay` | `python -m app.cli dlq replay --limit 50` |
| `make stats` | `python -m app.cli stats` |

## Architecture (preview)

Two services plus tooling, per `PLAN.md` §4:

- **inference** (`app/`, port 8000) — public API, validation, SQLite
  persistence, resilient pipeline execution, deferral, DLQ, metrics, admin.
- **pipeline-mock** (`mock/`, port 8100) — Appendix B stand-in for the
  STT→LLM→TTS dependency (spec-exact at defaults).
- **traffic-gen** (`tools/traffic_gen.py`) — Appendix C compressed-day replay
  (Sprint 6).
- **CLI** (`python -m app.cli`) — DLQ inspect/replay + stats (Sprint 5).

The `PipelineBackend` protocol keeps the resilience layer independent of any
specific provider — swapping the mock for a real (or scoring-harness)
dependency is a base-URL change, not a code change.

## Compressed-day demo (`make demo`)

With the stack up (`make up`), one command replays Appendix C's day against
the live service:

```bash
make demo
# equivalent to: python -m tools.traffic_gen
```

It prints a per-window summary and writes `reports/run_<ts>.json` (transient
runs are gitignored).

**Time compression — the honest statement.** The generator replays one
*simulated* day of Appendix C traffic on a compressed clock. At the default
**30×**:

| | Simulated | Real (30×) |
|---|---|---|
| Baseline (00:00–08:00, 10 calls/min) | 10 calls/min | **300 calls/min** (~5/s) |
| Peak hold (08:15–10:00 & 14:15–16:00, 50 calls/min) | 50 calls/min | **1,500 calls/min** (~25/s) |
| One day (23,400 calls) | 24 h | **48 real minutes** |

So a 24-hour traffic pattern is exercised in 48 minutes — compression factors
affect *time*, not volume: the run still submits all 23,400 events. `--speedup`
and `--duration-sim-hours` are overridable (`SPEEDUP=`, `DURATION_SIM_HOURS=`,
`TARGET_URL=`), which is what the bounded integration test
(`tests/integration/test_compressed_day.py`, 8.75 sim-hours at 60×, ~9 real
minutes) uses.

**What the report says, and what it refuses to say.** Every window reports its
target rate beside the rate actually delivered — in both simulated and observed
real terms — with the peak in-flight count and a `saturated` flag. A rate the
run could not reach is never reported as achieved. `silent_loss` is *derived*
(`accepted − terminal − deferred`, with the formula and its inputs printed in
the report), not asserted, and is only meaningful once the deferred queue has
drained; the runner waits for that before snapshotting. The report also carries
retry statistics, circuit-breaker transitions, DLQ counts against the ~0.8%
three-strike expectation, client-observed latency percentiles, and the
compression factor itself.

> **Capacity note — measured, not assumed.** The local two-process stack
> (inference + mock over SQLite, the mock carrying Appendix B's real ~300 ms
> latency) sustains **~32 calls/s**. Measured on a clean volume with a direct
> capacity probe: 17.8 calls/s at concurrency 20, 27.3 at 50, 31.8 at 200 —
> i.e. a flat ceiling, not a queueing artefact.
>
> That is why the committed sample runs at **30×**: its peak hold demands
> 50 calls/min × 30 = 25 calls/s, which fits under the ceiling with headroom.
> At the sanctioned **60×** the peak demands 50 calls/s, the stack saturates,
> the breaker trips, events keep deferring until they cross `DEFER_MAX_AGE_S`,
> and the day does not complete inside a drain budget. Running it anyway is
> *supported and reported honestly* — every window would carry `saturated:
> true` and the verdict would fail — but it would be evidence about this
> machine's CPU, not about the service. `python -m tools.traffic_gen --speedup 60`
> reproduces that path if you want to see the saturation reporting.
>
> The integration test escapes this by running the mock at sub-millisecond
> latency (`FAST_LATENCY_MU = -3.0`): at 60× its peak of 50 calls/s is
> measured comfortably in capacity (peak in-flight ~18 of 200). The demo uses
> the real Appendix B latency, which is slower and therefore capacity-bound
> sooner. These are two different, both-correct profiles; neither is a fudge.

## License

Apache 2.0 — see [`LICENSE`](LICENSE).
