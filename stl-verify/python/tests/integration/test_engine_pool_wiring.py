"""The running app's connection pool must be the configured one.

``tests/unit/test_engine.py`` covers the factory in isolation, so it stays green
even when the lifespan builds its own engine inline and serves requests on
SQLAlchemy's defaults. This asserts against the engine the app actually uses.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.pool import QueuePool

from app.config import Settings
from app.main import create_app

_POOL_SIZE = 7
_MAX_OVERFLOW = 13
_POOL_TIMEOUT = 3


@pytest.fixture()
def started_app(async_db_url: str, tmp_path: Path) -> Iterator[FastAPI]:
    """Yield the app with its lifespan running, so ``state.engine`` is the live one."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate(
            {
                "database_url": SecretStr(async_db_url),
                "suraf_mappings_file": empty_mapping,
                "db_pool_size": _POOL_SIZE,
                "db_max_overflow": _MAX_OVERFLOW,
                "db_pool_timeout": _POOL_TIMEOUT,
            }
        )
    )
    with TestClient(test_app):
        yield test_app


def test_app_engine_pool_reflects_the_configured_settings(started_app: FastAPI) -> None:
    pool = started_app.state.engine.pool

    assert isinstance(pool, QueuePool)
    assert pool.size() == _POOL_SIZE
    # No public accessors for the overflow ceiling or the queue wait; asserted
    # directly so a regression to SQLAlchemy's defaults fails rather than
    # degrading silently under load.
    assert pool._max_overflow == _MAX_OVERFLOW
    assert pool._timeout == _POOL_TIMEOUT


def test_app_engine_pre_pings_connections(started_app: FastAPI) -> None:
    """Stale pooled connections (pooler restarts, idle timeouts) must be recycled.

    Without it the first request after a drop fails instead of transparently
    reconnecting.
    """
    assert started_app.state.engine.pool._pre_ping is True
