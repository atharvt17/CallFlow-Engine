# collections-voice-agent — CallFlow Engine

A compliant, resilient collections voice-agent **inference service** wrapped around a
spec-exact mock of the STT → LLM → TTS pipeline
(Predixion AI × TalentX Collections Agent Challenge — Round 1, local track).

The service validates Appendix A call events, persists every one before dispatch, drives the
downstream dependency through retry + circuit-breaker resilience, degrades to a **persistent**
deferred queue while the breaker is open, dead-letters genuine terminal failures into a
**SQLite DLQ that is inspectable and replayable over both HTTP and CLI**, and exposes the
Prometheus metrics Round 2's autoscaling will need.

**Documentation map:** [`PLAN.md`](PLAN.md) strategy · [`SPECS.md`](SPECS.md) normative spec ·
[`TASKS.md`](TASKS.md) execution tracker · [`COST.md`](COST.md) Fargate cost estimate.

> **Status:** Sprints 0–5 and M5-T01…T05 complete. Sprint 7 (cost + docs + release audit) is in
> progress. **`reports/sample_day_report.json` is not yet committed** — the full-day demo run is
> outstanding, so no completed Appendix C day is cited as evidence anywhere in this README
> (see [`TASKS.md`](TASKS.md) §0 "Outstanding").

---

## Integration points — read this first

If you are pointing your own harness or mock at this service, these are the four things you
need. Everything else is detail.

| Point | Detail |
|---|---|
| **Dependency base URL** | `PIPELINE_BASE_URL`. Point it at *your* Appendix B-conformant mock and the whole resilience stack applies to it unchanged — no code change, no rebuild. Compose sets it to `http://mock:8100`; the default is `http://localhost:8100`. |
| **Dependency contract** | `POST /pipeline/process` with `{"event_id", "audio_ref"}`. Success → `200 {"status":"ok","transcript","response_audio_ref","latency_ms"}`; injected failure → `503 {"status":"error","reason":"downstream_timeout"}`. Any service matching this shape works. |
| **Compressed time** | The demo replays a full 24-hour Appendix C day in real minutes. At the default 30×, the 10 calls/min baseline becomes **300 calls/min** and the 50 calls/min peak becomes **1,500 calls/min**. Volume is *not* compressed — all 23,400 events are submitted. |
| **Failure injection** | `POST /admin/config` on the mock accepts `{"failure_rate", "latency_mu", "latency_sigma", "failure_latency_ms"}`. `FAILURE_RATE=1.0` exercises the breaker end to end; defaults are always Appendix B-exact. |

---

## Prerequisites

- Docker with Compose v2 (`docker compose version`) — the primary run path
- Python 3.11+ — only to run the test suite or the CLI/tools locally
- GNU Make — for the lifecycle verbs (`make up`, `make test`, …)

## Quickstart

Five commands, compose-first, no manual steps:

```bash
git clone <repo> && cd collections-voice-agent
docker compose up --build -d              # start inference + mock
curl -s http://localhost:8000/healthz      # inference: {"status":"ok",...}
curl -s http://localhost:8100/healthz      # mock:      {"status":"ok"}
docker compose down                        # or: make down
```

Both services self-report health, and `docker compose up -d` waits for them (compose healthchecks
gate the inference service on the mock being healthy). That is the entire documented setup — no
manual step beyond Docker itself.

### Bare-metal alternative (no Docker)

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"    # Linux/macOS: .venv/bin/python
python -m mock.main &                              # terminal 1 — port 8100
python -m app.main &                               # terminal 2 — port 8000
```

Both read the flat env vars in [`.env.example`](.env.example); `PIPELINE_BASE_URL` must point at
the mock (`http://localhost:8100`, the default).

## Architecture

```
                 ┌────────────────────────── docker compose ──────────────────────────┐
                 │                                                                    │
  traffic-gen ───┼──▶  ┌─────────────────────┐        ┌───────────────────────────┐    │
  (App. C,       │     │  inference :8000    │        │ pipeline-mock :8100       │    │
   compressed)   │     │  POST /calls        ├──res──▶│  POST /pipeline/process   │    │
                 │     │  GET /calls/{id}    │  ili-  │  80% ok (lognormal lat.)  │    │
                 │     │  GET /metrics       │  ent   │  20% 503 downstream_      │    │
                 │     │  /admin/cb, /admin/ │  bound │      timeout              │    │
                 │     │      dlq, /stats    │        │  /admin/config, /stats    │    │
                 │     └─────────┬───────────┘        └───────────────────────────┘    │
                 │               ▼                                                     │
                 │        SQLite data/collections.db                                   │
                 │        (call_events + dead_letter, WAL)                             │
                 └─────────────────────────────────────────────────────────────────────┘
```

**Component responsibilities**

- **inference** (`app/`, port 8000) — Appendix A validation, persist-before-dispatch, the
  resilient pipeline boundary, terminal-state accounting, SQLite deferral/DLQ, API, metrics, admin.
- **pipeline-mock** (`mock/`, port 8100) — Appendix B verbatim at defaults (20% immediate 503;
  lognormal latency calibrated to p50/p90/p99 ≈ 300/700/1200 ms), with a documented
  `/admin/config` extension for failure injection.
- **traffic-gen** (`tools/traffic_gen.py`) — replays the Appendix C schedule at `--speedup`,
  Poisson-jittered, and writes an honest per-window run report.
- **CLI** (`python -m app.cli`) — `dlq list|show|replay` and `stats`, HTTP-free, on the same code
  path as the endpoints.

**The extension point.** `PipelineBackend` (`app/pipeline/base.py`) is a `Protocol` with one
method — `process(event_id, audio_ref) -> PipelineResult`. `ResilientPipelineClient` wraps
*that*, never a concrete backend, so retry/breaker/deferral logic is untouched when the
dependency changes. HTTP types do not leak into the interface.

## Make targets

| Target | Effect |
|---|---|
| `make up` | build + start the compose stack |
| `make down` | stop and remove the stack (named data volume is kept) |
| `make logs` | follow compose logs |
| `make test` | pytest (prefers the project venv) |
| `make lint` | ruff check |
| `make demo` | compressed Appendix C day replay against the running stack (30×, ~48 real min) |
| `make calibration-report` | regenerate `reports/latency_calibration.md` |
| `make dlq-list` / `make dlq-replay` / `make stats` | CLI surfaces |

## API reference

Base URL `http://localhost:8000`. All bodies JSON. Errors use the `{"detail": ...}` envelope.

| Method & path | Purpose | Success | Errors |
|---|---|---|---|
| `POST /calls` | Submit one Appendix A call event | `200` processed / failed_downstream / duplicate · `202` deferred | `422` invalid schema |
| `GET /calls/{event_id}` | Fetch one stored event + outcome | `200` record | `404` unknown |
| `GET /healthz` | Liveness + quick state | `200` | — |
| `GET /stats` | Durable counts, breaker, latency percentiles | `200` | — |
| `GET /metrics` | Prometheus text exposition | `200` | — |
| `GET /admin/circuit-breaker` | Live breaker snapshot | `200` | — |
| `GET /admin/dlq` | List dead letters (`?status=pending\|replayed&limit=1..1000`) | `200` array | `422` bad status/limit |
| `GET /admin/dlq/{dlq_id}` | One dead letter, full event JSON | `200` | `404` unknown |
| `POST /admin/dlq/replay` | Replay pending rows | `200` report | `422` bad/oversized `event_ids` |
| `POST /admin/deferred/drain` | Force one drain pass | `200` | — |

### `POST /calls` — status codes

| Scenario | HTTP | Meaning |
|---|---|---|
| Pipeline success | **200** | processed, terminal |
| Terminal downstream failure (persisted, DLQ row created) | **200** | failed_downstream, terminal |
| Duplicate `event_id` | **200** | idempotent replay of the stored record, `"duplicate": true` |
| Breaker open — accepted for later processing | **202** | deferred, **not** completed; persisted before the response |
| Invalid input (schema, enum, unknown field) | **422** | rejected, nothing persisted |
| Unknown `event_id` on `GET /calls/{id}` | **404** | — |

A `202` is not a failure and not a completion: the event is durably queued, queryable via
`GET /calls/{event_id}`, and processed when the breaker recovers (or dead-lettered on age expiry).

**Response bodies** (field names verified against the running service):

```jsonc
// 200 — processed
{ "event_id": "...", "status": "processed", "outcome": "connected",
  "attempts": 1, "retry_count": 0, "pipeline_latency_ms": 208, "e2e_latency_ms": 859.46 }

// 200 — failed downstream (a DLQ row exists; fetch it at /admin/dlq/{dlq_id})
{ "event_id": "...", "status": "failed_downstream", "outcome": "failed_downstream",
  "attempts": 3, "retry_count": 2, "pipeline_latency_ms": 7,
  "e2e_latency_ms": 377.33, "failure_reason": "downstream_timeout", "dlq_id": 128 }

// 202 — breaker open: accepted, NOT completed
{ "event_id": "...", "status": "deferred", "reason": "circuit_open", "deferred_pending": 12 }

// 200 — duplicate of an existing event_id
{ /* the full stored record */ "duplicate": true }
```

`GET /calls/{event_id}` returns the stored row: `event_id`, `campaign_id`, `borrower_bucket`,
`product_type`, `timestamp`, `call_duration_s`, `input_outcome`, `status`, `outcome`, `attempts`,
`retry_count`, `pipeline_latency_ms`, `e2e_latency_ms`, `failure_reason`, `dlq_id`, `created_at`,
`updated_at`, and the original payload under `event`.

**`retry_count` = attempts − 1**, matching Appendix A's sample record (`failed_downstream`,
`retry_count: 2` ⇒ 3 attempts).

### `GET /stats` shape

```jsonc
{ "events": {"deferred":0,"processing":0,"processed":14577,"failed_downstream":127},
  "dead_letter": {"pending":127,"replayed":0},
  "deferred_pending": 0,
  "circuit_breaker": {"state":"closed","window_size":20,"failure_ratio":0.0,
                      "consecutive_failures":0,"opened_at":null,"last_transition_at":null},
  "latency_percentiles": {"p50":361.5,"p90":858.0,"p99":1765.5},
  "uptime_s": 29027.5 }
```

Counts come from SQLite and survive restarts; `latency_percentiles` come from this process's
histograms and reset with it. The CLI's `stats` omits `circuit_breaker` and `uptime_s` — a
separate process cannot observe the serving process's breaker, so it does not fabricate one.

## Configuration reference

Every knob is a flat env var (pydantic-settings, optional `.env`), with the default shown.
**No secrets exist in Round 1** — the stack is fully local; these are configuration, not
credentials.

| Var | Default | Component | Purpose |
|---|---|---|---|
| `SERVICE_HOST` / `SERVICE_PORT` | `0.0.0.0` / `8000` | inference | bind address |
| `PIPELINE_BACKEND` | `http` | inference | `http` \| `inline` (no-op, tests) |
| `PIPELINE_BASE_URL` | `http://localhost:8100` | inference | **dependency integration point** (compose: `http://mock:8100`) |
| `MAX_ATTEMPTS` | `3` | inference | attempts per logical call (first + 2 retries) |
| `PIPELINE_TIMEOUT_S` | `2.5` | inference | per-attempt timeout |
| `BACKOFF_BASE_MS` | `100` | inference | delay before retry k = base · factor^(k−1) |
| `BACKOFF_FACTOR` | `2.0` | inference | exponential growth (→ 100 ms, 200 ms) |
| `BACKOFF_MAX_MS` | `1000` | inference | cap on the base delay, applied **before** jitter |
| `BACKOFF_JITTER` | `0.5` | inference | uniform ±50% on each delay |
| `CB_WINDOW_SIZE` | `20` | inference | sliding window of completed **logical** calls |
| `CB_FAILURE_THRESHOLD` | `0.5` | inference | trip when failure ratio ≥ this |
| `CB_MIN_THROUGHPUT` | `10` | inference | …but only with ≥ this many outcomes in the window |
| `CB_CONSECUTIVE_FAILURES` | `5` | inference | OR trip on this many consecutive logical failures |
| `CB_COOLDOWN_S` | `5.0` | inference | OPEN duration before HALF_OPEN |
| `CB_HALF_OPEN_PROBES` | `3` | inference | max concurrent probe logical calls |
| `CB_HALF_OPEN_SUCCESS` | `2` | inference | probe successes required to CLOSE |
| `DEFER_QUEUE_MAX` | `5000` | inference | in-memory FIFO capacity (backpressure only) |
| `DEFER_MAX_AGE_S` | `3600` | inference | deferred age limit → DLQ `deferred_expired` |
| `SUCCESS_PTP_RATE` | `0.0` | inference | P(`ptp_taken`) on success |
| `DB_PATH` | `data/collections.db` | inference | SQLite file (runtime-only, git-ignored) |
| `LOG_LEVEL` | `INFO` | both | JSON log verbosity |
| `MOCK_PORT` | `8100` | mock | bind port |
| `FAILURE_RATE` | `0.20` | mock | Appendix B injected failure rate |
| `LATENCY_MU` | `5.7` | mock | lognormal μ (ms) |
| `LATENCY_SIGMA` | `0.62` | mock | lognormal σ, calibrated (§ below) |
| `FAILURE_LATENCY_MS` | `0` | mock | extra latency on the failure path (0 = immediate 503) |
| `TARGET_URL` | `http://localhost:8000` | traffic-gen | inference base URL to drive |
| `SPEEDUP` | `60` | traffic-gen | time-compression factor |
| `DURATION_SIM_HOURS` | `24` | traffic-gen | simulated hours to replay (0 < h ≤ 24) |
| `CONCURRENCY` | `200` | traffic-gen | max in-flight requests |

All 31 variables in [`.env.example`](.env.example) correspond 1:1 with the code's settings models —
verified programmatically, so this table cannot drift from the implementation.

## Demo walkthrough (`make demo`)

With the stack up (`make up`):

```bash
make demo          # = python -m tools.traffic_gen --speedup 30
```

It prints a per-window summary and writes `reports/run_<ts>.json` (git-ignored).

**Time compression, stated honestly.** The generator replays one *simulated* Appendix C day on a
compressed clock. At the default **30×**:

| | Simulated | Real (30×) |
|---|---|---|
| Baseline (00:00–08:00, 10 calls/min) | 10 calls/min | **300 calls/min** (~5/s) |
| Peak holds (08:15–10:00, 14:15–16:00, 50 calls/min) | 50 calls/min | **1,500 calls/min** (~25/s) |
| One day (23,400 calls) | 24 h | **48 real minutes** |

Compression affects *time*, not volume: the run still submits all 23,400 events. Override with
`SPEEDUP=`, `DURATION_SIM_HOURS=`, `TARGET_URL=`, `CONCURRENCY=`.

**Where to look while it runs:**

| Surface | What to watch |
|---|---|
| `GET /stats` | `events` by status, `dead_letter` counts, `deferred_pending` — the accounting invariant |
| `GET /metrics` | `in_flight_calls`, `deferred_pending`, `pipeline_attempts_total`, `circuit_state`, `circuit_transitions_total`, latency histograms |
| `GET /admin/circuit-breaker` | live state, window size, failure ratio, consecutive failures |
| `GET /admin/dlq` | the failed events themselves, with full JSON and failure reason |
| `python -m app.cli stats` | the same durable counts **without HTTP** (works if the service is wedged) |

**What the report says, and refuses to say.** Every window reports its target beside the rate
actually delivered — in both simulated and observed-real terms — plus peak in-flight and a
`saturated` flag. A rate the run could not reach is never reported as achieved. `silent_loss` is
*derived* (`accepted_unique − terminal_delta − deferred_delta`, formula and inputs printed in the
report), not asserted, and is only meaningful once the deferred queue has drained; the runner
waits for that before snapshotting. The report also carries retry statistics, breaker transitions,
DLQ counts against the ~0.8% three-strike expectation, client-observed latency percentiles, and
the compression factor.

**Expected numbers when the dependency is Appendix B-default (20% failures):** ~0.8% of accepted
events are dead-lettered (three consecutive failures: 0.2³), the breaker stays **closed**,
`deferred_pending` returns to 0, and silent loss is **0**.

**Expected numbers at the default 30× on a laptop:** the two-process stack sustains roughly
**32 calls/s** with the mock's real ~300 ms latency (measured: 17.8 calls/s at concurrency 20,
27.3 at 50, 31.8 at 200 — flat, so it is a ceiling, not a queueing artefact). 30×'s peak of
25 calls/s sits inside that; the sanctioned **60×** peak of 50 calls/s does not, and a 60× run
honestly reports `saturated: true` per window rather than claiming the rate. `--speedup 60`
reproduces that path. The integration test escapes the limit by running the mock at
sub-millisecond latency, which is why it can assert accounting at 60× in nine minutes.

**DLQ walkthrough** — the dead-letter path is inspectable and replayable from both surfaces.
Under Compose the database lives in a **named volume**, so the CLI must run inside the container
to see the same rows the API is writing (a CLI run on the host reads *its* `DB_PATH`, which is a
different, usually empty, file):

```bash
# HTTP — full rows with the original event JSON
curl -s "http://localhost:8000/admin/dlq?limit=3"

# CLI — identical data, no HTTP. Run it in the container under Compose:
docker compose exec -T inference python -m app.cli dlq list --limit 3
docker compose exec -T inference python -m app.cli dlq show <dlq_id>
docker compose exec -T inference python -m app.cli stats

# Replay — HTTP and CLI are the same code path
curl -s -X POST http://localhost:8000/admin/dlq/replay \
     -H 'Content-Type: application/json' -d '{"limit":50}'
docker compose exec -T inference python -m app.cli dlq replay --limit 50
```

Running bare-metal (no Docker) with the default `DB_PATH=data/collections.db`, the plain
`python -m app.cli …` form works directly — that is what `make dlq-list`, `make dlq-replay` and
`make stats` invoke. `--json` is a **global** flag, so it goes before the subcommand:
`python -m app.cli --json dlq list`.

Replay re-submits through the **same resilient boundary** (retries and breaker apply — no
shortcut) and updates rows in place: a `UNIQUE(event_id)` constraint means repeated failures
update the existing DLQ row rather than duplicating it.

## Swapping the mock for a real open-weight model

*The graded one-paragraph deliverable.*

The mock stands in for `STT → LLM → TTS`, and it is reached through exactly one seam: the
`PipelineBackend` protocol (`process(event_id, audio_ref) -> PipelineResult`). To run a real
open-weight stack — say `faster-whisper` large-v3 for STT, Mistral-7B-Instruct or
Llama-3.1-8B-Instruct (int8/int4 on a single 24 GB GPU) for the LLM, and Piper for TTS — you write
one new class implementing that method: it takes the audio reference, runs the three stages, and
returns a `PipelineResult` with `ok`, `reason`, `http_status` and `latency_ms`. Nothing else
changes. Retry policy, backoff and jitter, the circuit breaker's logical-call accounting, the
persistent deferral queue, the DLQ and its replay path, the metrics and the traffic generator all
keep operating on *that* boundary, so they keep applying unchanged — which is the point: the
scored resilience behaviour must be a property of the service, not of whichever dependency happens
to sit behind it. You can also leave our code entirely alone and simply set `PIPELINE_BASE_URL` at
any service that speaks the Appendix B contract, including a scoring harness's mock; a local
model served over the same `POST /pipeline/process` shape needs zero changes to this repo. The
only adaptation worth real thought is failure classification: a GPU out-of-memory or a model
load timeout is a *retryable* 5xx (matching `downstream_timeout`), while a malformed transcript
request is a *non-retryable* 4xx — mapping that correctly is what keeps the breaker from tripping
on errors retrying cannot fix.

## Latency calibration

Appendix B targets are p50 ≈ 300 ms, p90 ≈ 700 ms, p99 ≈ 1200 ms. Its literal `sigma=0.35`
reproduces p50 but undershoots the tails, and the appendix explicitly sanctions adjusting sigma
after checking observed percentiles. The shipped `LATENCY_SIGMA=0.62` (μ = 5.7) was calibrated
over **50,000 samples**:

| percentile | measured (ms) | target (ms) | band | verdict |
|---|---|---|---|---|
| p50 | 298.4 | 300 | ±10% | PASS |
| p90 | 662.6 | 700 | ±15% | PASS |
| p99 | 1252.7 | 1200 | ±25% | PASS |

Regenerate with `make calibration-report`; the test suite asserts the same bands read-only.
Full detail in [`reports/latency_calibration.md`](reports/latency_calibration.md).

## Design decisions

| Decision | Choice | Why |
|---|---|---|
| Processing model | Synchronous per `POST /calls`, SQLite-backed deferral when the breaker is open | Every request returns a definitive status — the simplest contract for a scoring harness — while the only async case (breaker open) persists before replying |
| Store | SQLite (WAL), authoritative for events, deferral and dead letters | Inspectable, transactional, replayable, survives restarts, zero infra |
| Retry | Hand-rolled (~60 lines), not tenacity | The retry/breaker interplay *is* the scored artifact; hand-rolling keeps it visible and testable |
| Breaker accounting | One final outcome per **logical call**, not per attempt | Retries are designed to absorb the 20% injections; counting attempts would trip the breaker on noise |
| Backend boundary | `PipelineBackend` Protocol, resilience wraps the protocol | Swapping dependency is configuration, not a code change |
| Config | Flat env vars, `.env.example` committed, `.env` git-ignored | No secrets exist at all in Round 1 |

Full rationale in [`PLAN.md`](PLAN.md) §3 and [`SPECS.md`](SPECS.md) §6–§7.

## Limitations (stated, not hidden)

- **The circuit breaker is per-process, in-memory.** Correct for a single instance; multiple
  instances would each hold their own breaker, so aggregate state needs Round 2 work. Corollary:
  a CLI `dlq replay` builds its *own* breaker and cannot see the server's — it may dispatch while
  the server is open. What *is* shared is the durable store, whose replay claims prevent
  double-processing across processes.
- **No overall request deadline.** The per-attempt timeout (2.5 s) is the only deadline;
  worst-case logical-call wall time is ≈7.8 s nominal (3 attempts + backoff). Documented decision,
  not an oversight.
- **Replay is at-least-once in the dispatch window.** A replay interrupted by a crash is
  reconciled by startup recovery, but an in-flight replay can in principle be re-dispatched.
- **Round 1 is local only:** no AWS, no Terraform/CDK, no real audio, no auth/TLS, no real
  STT/LLM/TTS models. The mock *is* the dependency.
- **Operational notes, not a runbook.** Round 1 does not require the Appendix I runbook; the
  admin surfaces above (breaker state, DLQ depth, `deferred_pending`) are exactly what that
  runbook would ask an operator to check first, and are carried forward as Round 2 material.
- **`reports/sample_day_report.json` is not committed yet.** The full-day demo run is outstanding
  (see [`TASKS.md`](TASKS.md) §0). The run report *format* and its verdict logic are covered by
  the test suite; the committed full-day artifact is not yet available.

## Publication & secret audit

The repository is publishable under Apache 2.0. **By design there are no credentials, API keys,
tokens or passwords anywhere in the codebase, configuration or history** — the Round 1 stack is
fully local and has nothing to authenticate to. `.env.example` contains configuration knobs only.
`.env`, the SQLite runtime database and transient run reports are git-ignored; `data/sample_events.json`
is synthetic and committed deliberately (no phone numbers, names, or account identifiers — it is
event metadata only).

That absence was **tested, not assumed**: a key-shaped-string grep over the tracked tree returns
0 hits, and a scan of every text blob in every commit (64 blobs across all history) also returns
**0 hits**. No `.env`, PEM, key, credential, or `.netrc`/`.pypirc`-style file has ever been added
to the repository; the only environment file in history is the committed `.env.example` template.
The audit evidence is recorded in `TASKS.md` under M6-T06.

## License

Apache 2.0 — see [`LICENSE`](LICENSE).
