"""Structured JSON logging.

Every log line is a single JSON object so the output is machine-parseable in a
real deployment (ship to ELK/Datadog/CloudWatch and filter on ``event``,
``event_id``, ``topic``, etc.). Arbitrary structured fields are passed through
the standard ``extra=`` argument:

    log.info("enriched", extra={"event_id": "evt-1", "category": "billing"})
"""

from __future__ import annotations

import json
import logging
import sys
import time

# Attributes present on every stdlib LogRecord; anything else was passed via
# ``extra=`` and should be promoted to a top-level field in the JSON line.
_RESERVED = set(
    logging.makeLogRecord({}).__dict__.keys()
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str | int = "INFO") -> None:
    """Install the JSON formatter on the root logger (idempotent)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # The kafka-python client is chatty at INFO; keep our own logs in focus.
    logging.getLogger("kafka").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
