"""Shared doubles for the PostgreSQL adapter unit tests."""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def stub_engine() -> Callable:
    """Build an engine whose connection answers each execute in turn, or raises.

    ``results`` are the kwargs of one ``MagicMock`` per statement, so a caller
    running two statements describes both (``{"fetchone.return_value": row}``,
    ``{"fetchall.return_value": rows}``). ``error`` makes every execute raise
    instead, which is how the failure paths are driven.
    """

    def build(*results: dict, error: Exception | None = None) -> tuple[MagicMock, AsyncMock]:
        conn = AsyncMock()
        conn.__aenter__ = AsyncMock(return_value=conn)
        conn.__aexit__ = AsyncMock(return_value=False)
        conn.execute = AsyncMock(side_effect=error or [MagicMock(**result) for result in results])
        engine = MagicMock()
        engine.connect.return_value = conn
        return engine, conn

    return build
