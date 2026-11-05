# collections-voice-agent — CallFlow Engine

A compliant, resilient collections voice-agent **inference service** wrapped
around a spec-exact mock of the STT→LLM→TTS pipeline
(Predixion AI × TalentX Collections Agent Challenge — Round 1, local track).

> **Status:** implementation in progress per `TASKS.md`. Sprint 0 scaffold is
> done — both services build, start, and self-report healthy. The graded
> pipeline contract (Sprint 1), the resilience engine and persistent deferral
> (Sprints 3–4), DLQ inspect/replay (Sprint 5), the traffic generator (Sprint 6)
> and the cost writeup (Sprint 7) land in the coming sprints.

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
| `make demo` | placeholder — not implemented until Sprint 6 (M5) |
| `make dlq-replay` | placeholder — not implemented until Sprint 5 (M4) |

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

## License

Apache 2.0 — see [`LICENSE`](LICENSE).
