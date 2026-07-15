"""Structured (JSON-lines) logging.

Every record carries timestamp, level, logger, message and the current
correlation id; any `extra={...}` keys are merged into the JSON object.
Never log payload bodies or secrets — log identifiers instead.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from app.logging.correlation import get_correlation_id

_RESERVED = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_correlation_id(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    # Idempotent: replace our handler rather than stacking duplicates.
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    logging.getLogger("uvicorn.access").setLevel("WARNING")
    logging.getLogger("aio_pika").setLevel("WARNING")
    logging.getLogger("aiormq").setLevel("WARNING")
