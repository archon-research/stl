"""Tests for app.logging structured logging module."""

import json
import logging

import pytest

from app.logging import JsonFormatter, TextFormatter, get_logger, setup_logging
from app.middleware.request_id import request_id_var


def _make_record(
    name: str = "test.logger",
    level: int = logging.INFO,
    msg: str = "hello world",
) -> logging.LogRecord:
    """Create a minimal LogRecord for formatter tests."""
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="test.py",
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )


class TestJsonFormatter:
    def test_json_formatter_output(self) -> None:
        formatter = JsonFormatter()
        record = _make_record()
        output = formatter.format(record)
        parsed = json.loads(output)

        assert "timestamp" in parsed
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.logger"
        assert parsed["message"] == "hello world"

    def test_json_formatter_includes_request_id_when_set(self) -> None:
        token = request_id_var.set("abc-123")
        try:
            formatter = JsonFormatter()
            record = _make_record()
            output = formatter.format(record)
            parsed = json.loads(output)

            assert parsed["request_id"] == "abc-123"
        finally:
            request_id_var.reset(token)

    def test_json_formatter_omits_request_id_when_not_set(self) -> None:
        # Ensure context var is at default (None)
        formatter = JsonFormatter()
        record = _make_record()
        output = formatter.format(record)
        parsed = json.loads(output)

        assert "request_id" not in parsed


class TestTextFormatter:
    def test_text_formatter_output(self) -> None:
        formatter = TextFormatter()
        record = _make_record()
        output = formatter.format(record)

        assert "[INFO]" in output
        assert "test.logger" in output
        assert "hello world" in output
        # Should not contain request_id when not set
        assert "request_id=" not in output

    def test_text_formatter_includes_request_id_when_set(self) -> None:
        token = request_id_var.set("req-456")
        try:
            formatter = TextFormatter()
            record = _make_record()
            output = formatter.format(record)

            assert "[request_id=req-456]" in output
        finally:
            request_id_var.reset(token)


class TestSetupLogging:
    def test_setup_logging_json_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOG_FORMAT", "json")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        setup_logging()

        root = logging.getLogger()
        assert root.level == logging.DEBUG
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)

    def test_setup_logging_text_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOG_FORMAT", "text")
        monkeypatch.setenv("LOG_LEVEL", "WARNING")

        setup_logging()

        root = logging.getLogger()
        assert root.level == logging.WARNING
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, TextFormatter)

    def test_setup_logging_defaults_to_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LOG_FORMAT", raising=False)
        monkeypatch.delenv("LOG_LEVEL", raising=False)

        setup_logging()

        root = logging.getLogger()
        assert root.level == logging.INFO
        assert isinstance(root.handlers[0].formatter, JsonFormatter)


class TestGetLogger:
    def test_get_logger_returns_named_logger(self) -> None:
        logger = get_logger("test")
        assert logger.name == "test"
        assert isinstance(logger, logging.Logger)
