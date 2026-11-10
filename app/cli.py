"""Operator CLI — DLQ inspection/replay + stats without HTTP (M4-T02; SPECS §2).

``python -m app.cli`` is a first-class surface, not a debugging shortcut:
SPECS §2 lists it beside the HTTP API, and §7.2 requires replay to be
available "via CLI ... and HTTP — identical code path". Every command here
therefore calls the same code the endpoints do:

- ``dlq list`` / ``dlq show`` → :mod:`app.reporting` serializers (the ones
  ``GET /admin/dlq`` uses);
- ``dlq replay`` → :meth:`app.service.CallService.replay` (the method
  ``POST /admin/dlq/replay`` calls);
- ``stats`` → :func:`app.reporting.stats_body` (the ``GET /stats`` body).

The CLI talks to the SQLite file directly, which is what makes it work when
the service is down, wedged, or unreachable — the whole point of an
HTTP-free path. Two consequences are stated rather than hidden:

- **No serving-process state.** A CLI process builds its own breaker and
  metrics registry; it cannot read the serving process's. ``stats`` therefore
  omits ``circuit_breaker``/``uptime_s``, which would otherwise be fabricated
  facts about a process this command is not talking to. Use ``GET /stats``
  (or ``GET /admin/circuit-breaker``) for live breaker state.
- **Its own resilience stack, not the server's.** ``dlq replay`` from the CLI
  is still a full resilient replay — retries, per-attempt timeout and breaker
  admission all apply — but it runs against a breaker this process built from
  the same settings, which starts ``closed`` on every invocation. The CLI
  therefore **cannot see** the serving process's breaker: if the server's
  breaker is OPEN, a CLI replay may still dispatch. That is not a shared live
  circuit breaker and must not be described as one. What *is* shared is the
  durable store: replay claims (``failed_downstream → processing``) stop a CLI
  replay and a server replay from processing the same event twice, whichever
  process gets there first.

Use ``GET /stats`` or ``GET /admin/circuit-breaker`` for real breaker state;
the CLI's own breaker exists only to keep replay on the same code path as HTTP
(SPECS §7.2), not to report on the service.

Exit codes (meaningful, scriptable):

- ``0`` — success;
- ``1`` — the operation ran but did not fully succeed (replay: at least one
  event failed again or was skipped; ``dlq show``: unknown id);
- ``2`` — usage error (argparse's own code: bad ``--limit``, more
  ``--event-ids`` than the store can bind);
- ``3`` — the store could not be opened/initialised (mistyped ``DB_PATH``,
  incompatible schema).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from typing import Any

from app import store
from app.config import Settings
from app.metrics import Metrics
from app.reporting import record_payload, stats_body
from app.runtime import ServiceRuntime
from app.service import ReplayReport

EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_USAGE = 2  # argparse's own code; reused for late-detected usage errors
EXIT_STORE_ERROR = 3

DLQ_STATUSES = ("pending", "replayed")


class CliError(Exception):
    """A failure that should end the process with a specific exit code."""

    def __init__(self, message: str, *, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code


# --- argument parsing -------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """The command grammar (SPECS §2: ``dlq list|show|replay`` + ``stats``)."""
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="DLQ inspection/replay and run stats, without HTTP.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable JSON instead of a table",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    dlq = sub.add_parser("dlq", help="inspect or replay dead-letter rows")
    dlq_sub = dlq.add_subparsers(dest="dlq_command", required=True)

    list_cmd = dlq_sub.add_parser("list", help="list dead letters, newest first")
    list_cmd.add_argument(
        "--status",
        choices=DLQ_STATUSES,
        default="pending",
        help="which rows to show (default: pending)",
    )
    list_cmd.add_argument("--limit", type=int, default=50, help="max rows (default: 50)")

    show_cmd = dlq_sub.add_parser("show", help="show one dead letter by id")
    show_cmd.add_argument("dlq_id", type=int, help="dead_letter.id")

    replay_cmd = dlq_sub.add_parser("replay", help="replay pending dead letters")
    replay_cmd.add_argument(
        "--limit", type=int, default=50, help="max rows to replay (default: 50)"
    )
    replay_cmd.add_argument(
        "--event-ids",
        nargs="+",
        metavar="EVENT_ID",
        help="replay exactly these events instead of the oldest N",
    )

    sub.add_parser("stats", help="durable counts and latency percentiles")
    return parser


def _validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Reject values the service layer would otherwise silently coerce.

    ``--limit 0`` is the dangerous one: ``LIMIT 0`` returns nothing, which a
    script would read as "no pending dead letters" — the exact false negative
    the DLQ surfaces exist to prevent.
    """
    if getattr(args, "limit", None) is not None and args.limit < 1:
        parser.error("--limit must be >= 1")
    if getattr(args, "limit", None) is not None and args.limit > 1000:
        parser.error("--limit must be <= 1000")
    event_ids = getattr(args, "event_ids", None)
    if event_ids is not None:
        try:
            store.check_replay_event_ids(event_ids)
        except store.ReplayBoundsError as exc:
            parser.error(str(exc))


# --- output -----------------------------------------------------------------


def _emit(payload: Any, *, as_json: bool, text: str) -> None:
    print(json.dumps(payload, indent=2, default=str) if as_json else text)


def _format_dlq_list(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "no dead letters"
    header = (
        f"{'ID':<6} {'EVENT_ID':<36} {'REASON':<20} {'ATTEMPTS':>8} {'STATUS':<8} "
        f"{'LAST_FAILED_AT'}"
    )
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(
            f"{row['id']!s:<6} {row['event_id']!s:<36} {row['reason']!s:<20} "
            f"{row['attempts']!s:>8} {row['status']!s:<8} {row['last_failed_at']!s}"
        )
    return "\n".join(lines)


def _format_replay(report: ReplayReport) -> str:
    summary = (
        f"attempted={report.attempted} succeeded={report.succeeded} "
        f"failed={report.failed} deferred={report.deferred} "
        f"skipped={report.skipped}"
    )
    lines = [summary]
    for result in report.results:
        suffix = f" ({result.reason})" if result.reason else ""
        lines.append(
            f"  {result.event_id}  dlq_id={result.dlq_id}  {result.status}{suffix}"
        )
    return "\n".join(lines)


# --- commands ---------------------------------------------------------------


def _open_conn(settings: Settings) -> sqlite3.Connection:
    """Open + initialise the store, mapping failures to the store exit code."""
    conn: sqlite3.Connection | None = None
    try:
        conn = store.connect(settings.db_path)
        store.init_db(conn)
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        if conn is not None:
            conn.close()
        raise CliError(
            f"cannot use store at {settings.db_path}: {exc}", exit_code=EXIT_STORE_ERROR
        ) from exc
    return conn


def cmd_dlq_list(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    rows = [
        record_payload(row)
        for row in store.list_dead_letters(conn, status=args.status, limit=args.limit)
    ]
    _emit(rows, as_json=args.json, text=_format_dlq_list(rows))
    return EXIT_OK


def cmd_dlq_show(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    row = store.get_dead_letter(conn, args.dlq_id)
    if row is None:
        raise CliError(f"unknown dlq_id: {args.dlq_id}", exit_code=EXIT_INCOMPLETE)
    payload = record_payload(row)
    _emit(payload, as_json=args.json, text=json.dumps(payload, indent=2, default=str))
    return EXIT_OK


def cmd_dlq_replay(args: argparse.Namespace, settings: Settings) -> int:
    """Replay through the full resilient stack — the same call HTTP makes.

    Unlike the read commands, this needs the whole runtime (backend, breaker,
    service), not just a store handle, because a replay that skipped the
    resilience layer would be the compliance hole SPECS §7.2 forbids.

    Output: ``--json`` prints the exact §5 body, so a script sees what
    ``POST /admin/dlq/replay`` returns and nothing more. The human table also
    reports ``deferred``/``skipped``, which are derived from ``results[].status``
    and exist only here — they are not added to the HTTP contract (see
    :class:`app.service.ReplayReport`).
    """

    async def _run() -> ReplayReport:
        runtime = ServiceRuntime.build(settings)
        try:
            return await runtime.service.replay(limit=args.limit, event_ids=args.event_ids)
        finally:
            await runtime.aclose()

    try:
        report = asyncio.run(_run())
    except store.ReplayBoundsError as exc:
        # Only reachable if the store tightened its cap after argument
        # validation; it is still a usage problem, not a broken store.
        raise CliError(str(exc), exit_code=EXIT_USAGE) from exc
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        raise CliError(
            f"cannot use store at {settings.db_path}: {exc}", exit_code=EXIT_STORE_ERROR
        ) from exc
    _emit(report.as_body(), as_json=args.json, text=_format_replay(report))
    # Anything not replayed is an incomplete operation for a script: the rows
    # the operator asked about are still pending (or were never there).
    unresolved = report.failed + report.skipped + report.deferred
    return EXIT_OK if unresolved == 0 else EXIT_INCOMPLETE


def cmd_stats(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Durable counts from this store; no process-local breaker/uptime.

    ``latency_percentiles`` come from *this* process's empty registry, so they
    are all ``None`` — the honest answer for a one-shot process. They are
    omitted from the text rendering for that reason; ``--json`` keeps them so
    the shape matches ``GET /stats`` exactly.
    """
    body = stats_body(conn, Metrics())
    if args.json:
        _emit(body, as_json=True, text="")
    else:
        events = body["events"]
        dead_letter = body["dead_letter"]
        assert isinstance(events, dict) and isinstance(dead_letter, dict)
        lines = [
            f"deferred_pending  {body['deferred_pending']}",
            f"events            {json.dumps(events, sort_keys=True)}",
            f"dead_letter       {json.dumps(dead_letter, sort_keys=True)}",
        ]
        print("\n".join(lines))
    return EXIT_OK


# --- entry point ------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Run one command; returns the process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(args, parser)

    settings = Settings()
    try:
        if args.command == "dlq" and args.dlq_command == "replay":
            return cmd_dlq_replay(args, settings)

        conn = _open_conn(settings)
        try:
            if args.command == "stats":
                return cmd_stats(conn, args)
            if args.dlq_command == "list":
                return cmd_dlq_list(conn, args)
            return cmd_dlq_show(conn, args)
        finally:
            conn.close()
    except CliError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
