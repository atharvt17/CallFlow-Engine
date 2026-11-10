# CallFlow Engine (collections-voice-agent) — lifecycle verbs (M0-T08).
# POSIX / Git Bash compatible: no bashisms, tab-indented recipes.

# Prefer the project venv's interpreter; fall back to PATH python.
# (venv layout differs between Windows and POSIX.)
ifeq ($(OS),Windows_NT)
  VENV_PY := .venv/Scripts/python.exe
  PY := $(if $(wildcard $(VENV_PY)),$(VENV_PY),python)
else
  VENV_PY := .venv/bin/python
  PY := $(if $(wildcard $(VENV_PY)),$(VENV_PY),python3)
endif

.PHONY: up down logs test lint calibration-report demo dlq-list dlq-replay stats

## up: build and start the compose stack (inference + mock)
up:
	docker compose up --build -d

## down: stop and remove the compose stack (named data volume is kept)
down:
	docker compose down

## logs: follow compose logs (Ctrl+C to stop)
logs:
	docker compose logs -f

## test: run pytest (prefers the project venv; falls back to PATH python)
test:
	$(PY) -m pytest -q

## lint: run ruff (prefers the project venv; falls back to PATH python)
lint:
	$(PY) -m ruff check .

## calibration-report: regenerate reports/latency_calibration.md (intentional)
## The test suite asserts the same bands read-only; only this target writes.
calibration-report:
	$(PY) -m tools.calibration_report

## demo: compressed Appendix C day replay (placeholder until Sprint 6)
demo:
	@echo "make demo: not implemented until Sprint 6 (traffic generator, milestone M5)"; exit 1

## dlq-list: list pending dead letters without HTTP (SPECS §2)
dlq-list:
	$(PY) -m app.cli dlq list

## dlq-replay: replay dead-letter rows (HTTP-free path, SPECS §2/§7.2)
dlq-replay:
	$(PY) -m app.cli dlq replay --limit 50

## stats: durable event/dead-letter counts from the store (HTTP-free)
stats:
	$(PY) -m app.cli stats
