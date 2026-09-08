"""Structured logging module.

Provides JSON and human-readable log formatters that include ``request_id``
when the context variable defined in ``app.middleware.request_id`` has been
populated by application middleware or other request-scoped code.
"""

import json
import logging
from datetime import UTC, datetime

from app.middleware.request_id import request_id_var

_APP_LOGGER_NAME = "app"

# Anything NOT here reached the record through a caller's ``extra=`` and is
# emitted as its own JSON field — what makes a decision event (ADR-015 gate 3)
# queryable in Loki. Derived from a real record so it tracks the interpreter.
_STANDARD_RECORD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


def _record_extras(record: logging.LogRecord) -> dict[str, object]:
    """Caller-supplied ``extra=`` fields on ``record``, in insertion order."""
    return {key: value for key, value in record.__dict__.items() if key not in _STANDARD_RECORD_ATTRS}


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        request_id = _get_request_id()
        if request_id is not None:
            log_entry["request_id"] = request_id

        # setdefault, not update: an `extra` key must never overwrite the
        # envelope fields every line is parsed by.
        for key, value in _record_extras(record).items():
            log_entry.setdefault(key, value)

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

        extras = _record_extras(record)
        if extras:
            base += " " + " ".join(f"{key}={value}" for key, value in extras.items())

        if record.exc_info and record.exc_info[0] is not None:
            base += f"\n{self.formatException(record.exc_info)}"

        return base


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    logger_names: tuple[str, ...] = (_APP_LOGGER_NAME,),
) -> None:
    """Configure the given logger trees with one shared formatter.

    Never touches the root logger (Uvicorn/Gunicorn/platform handlers stay
    theirs). The API configures the default ``app`` tree; a worker entry point
    passes its own tree too (``("app", "cli")``) so every line in the pod
    shares one format — two formats in one stdout stream is a Loki parsing
    problem.
    """
    level = log_level.upper()
    level_names = logging.getLevelNamesMapping()
    if level_names.get(level) is None:
        valid_levels = ", ".join(sorted(name for name in level_names if name.isupper()))
        raise ValueError(f"invalid log level {log_level!r}; expected one of: {valid_levels}")

    formatter: logging.Formatter
    if log_format.lower() == "text":
        formatter = TextFormatter()
    else:
        formatter = JsonFormatter()

    for name in logger_names:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        tree_logger = logging.getLogger(name)
        tree_logger.setLevel(level)
        tree_logger.handlers.clear()
        tree_logger.addHandler(handler)
        tree_logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Return a named logger under the ``app`` tree."""
    return logging.getLogger(name)


def _get_request_id() -> str | None:
    return request_id_var.get(None)
