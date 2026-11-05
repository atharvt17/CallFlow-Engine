"""Structured JSON-lines logging to stdout (SPECS §9).

Line shape: ``{"ts": <utc iso>, "level": <levelname>, "event": <name>, ...}``
— every notable event carries ``event_id`` where applicable (SPECS §9 lists
``call_received`` / ``call_processed`` / ``call_failed`` / ``call_deferred`` /
``retry_scheduled`` / ``cb_transition`` / ``dlq_insert`` / ``dlq_replay``).
``setup_logging`` is called by runtime entrypoints only (never inside app
factories, so pytest's log capture stays intact) and honors ``LOG_LEVEL``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_FIELDS_ATTR = "json_fields"


class JsonFormatter(logging.Formatter):
    """Render each record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
        }
        fields = getattr(record, _FIELDS_ATTR, None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Install the JSON handler on the root logger (idempotent)."""
    root = logging.getLogger()
    root.setLevel(level.upper())
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Emit one structured event line: ``{"event": <name>, **fields}``."""
    logger.info(event, extra={_FIELDS_ATTR: fields})
