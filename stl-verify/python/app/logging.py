"""Structured logging module.

Provides JSON and human-readable log formatters with automatic ``request_id``
injection from the context variable defined in ``app.middleware.request_id``.
"""

import json
import logging
from datetime import UTC, datetime

from app.middleware.request_id import request_id_var

_APP_LOGGER_NAME = "app"


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

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class TextFormatter(logging.Formatter):
    """Formats log records as human-readable text lines."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).isoformat()
        base = f"{timestamp} [{record.levelname}] {record.name} - {record.getMessage()}"

        request_id = _get_request_id()
        if request_id is not None:
            base += f" [request_id={request_id}]"

        if record.exc_info and record.exc_info[0] is not None:
            base += f"\n{self.formatException(record.exc_info)}"

        return base


def setup_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """Configure the application logger tree.

    Only touches the ``app`` logger — leaves the root logger (and any
    Uvicorn/Gunicorn/platform handlers) untouched.
    """
    level = log_level.upper()
    if logging.getLevelNamesMapping().get(level) is None:
        level = "INFO"

    formatter: logging.Formatter
    if log_format.lower() == "text":
        formatter = TextFormatter()
    else:
        formatter = JsonFormatter()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    app_logger = logging.getLogger(_APP_LOGGER_NAME)
    app_logger.setLevel(level)
    app_logger.handlers.clear()
    app_logger.addHandler(handler)
    app_logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Return a named logger under the ``app`` tree."""
    return logging.getLogger(name)


def _get_request_id() -> str | None:
    return request_id_var.get(None)
