"""Structured logging module.

Provides JSON and human-readable log formatters with automatic ``request_id``
injection from the context variable defined in ``app.middleware.request_id``.
"""

import json
import logging
import os
from datetime import UTC, datetime

try:
    from app.middleware.request_id import request_id_var
except ImportError:  # pragma: no cover – VEC-69 may not be merged yet
    request_id_var = None


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, str] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        request_id = _get_request_id()
        if request_id is not None:
            log_entry["request_id"] = request_id

        return json.dumps(log_entry, default=str)


class TextFormatter(logging.Formatter):
    """Formats log records as human-readable text lines."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).isoformat()
        base = f"{timestamp} [{record.levelname}] {record.name} - {record.getMessage()}"

        request_id = _get_request_id()
        if request_id is not None:
            base += f" [request_id={request_id}]"

        return base


def setup_logging() -> None:
    """Configure the root logger based on ``LOG_FORMAT`` and ``LOG_LEVEL`` env vars."""

    log_format = os.environ.get("LOG_FORMAT", "json").lower()
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    formatter: logging.Formatter
    if log_format == "text":
        formatter = TextFormatter()
    else:
        formatter = JsonFormatter()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    # Avoid duplicate handlers when called multiple times (e.g. in tests).
    root_logger.handlers.clear()
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)


def _get_request_id() -> str | None:
    if request_id_var is None:
        return None
    return request_id_var.get()
