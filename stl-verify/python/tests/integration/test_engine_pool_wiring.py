"""The running app's connection pool must be the configured one.

``tests/unit/test_engine.py`` covers the factory in isolation, so it stays green
even when the lifespan builds its own engine inline and serves requests on
SQLAlchemy's defaults. This asserts against the engine the app actually uses,
plus live-connection behavior of the factory the lifespan builds it with.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import QueuePool

from app.adapters.postgres.engine import (
    create_db_engine,
    mark_stale_transaction_state_as_disconnect,
)
from app.config import Settings
from app.main import create_app

_POOL_SIZE = 7
_MAX_OVERFLOW = 13
_POOL_TIMEOUT = 3
_POOL_RECYCLE_SECONDS = 60


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
                "core_model_mappings_file": empty_mapping,
                "db_pool_size": _POOL_SIZE,
                "db_max_overflow": _MAX_OVERFLOW,
                "db_pool_timeout": _POOL_TIMEOUT,
                "db_pool_recycle_seconds": _POOL_RECYCLE_SECONDS,
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
    assert pool._recycle == _POOL_RECYCLE_SECONDS


def test_app_engine_pre_pings_connections(started_app: FastAPI) -> None:
    """Stale pooled connections (pooler restarts, idle timeouts) must be recycled.

    Without it the first request after a drop fails instead of transparently
    reconnecting.
    """
    assert started_app.state.engine.pool._pre_ping is True


def test_app_engine_invalidates_connections_with_stale_transaction_state(started_app: FastAPI) -> None:
    """The listener has to sit on the engine the app serves from; see
    ``mark_stale_transaction_state_as_disconnect`` for what it protects against.
    """
    assert event.contains(
        started_app.state.engine.sync_engine,
        "handle_error",
        mark_stale_transaction_state_as_disconnect,
    )


async def test_engine_connections_carry_no_statement_cache(async_db_url: str) -> None:
    """Both prepared-statement caches must be off on the wire, not just in config
    (why: ``Settings.db_statement_cache_size``). Asserted against a live
    connection because the two knobs are plain connect kwargs — a typo in
    either name would configure nothing and fail only in production.
    """
    settings = Settings.model_validate({"database_url": SecretStr(async_db_url)})
    engine = create_db_engine(settings.async_database_url, statement_cache_size=settings.db_statement_cache_size)
    try:
        async with engine.connect() as connection:
            raw = await connection.get_raw_connection()
            asyncpg_connection = raw.driver_connection
            dialect_adapter = cast(Any, raw.dbapi_connection)
            assert asyncpg_connection is not None
            assert dialect_adapter is not None
            # asyncpg's implicit cache (statement_cache_size=0).
            assert asyncpg_connection._stmt_cache_enabled is False
            # The dialect's own LRU cache (prepared_statement_cache_size=0).
            assert dialect_adapter._prepared_statement_cache is None
    finally:
        await engine.dispose()


async def test_a_stale_transaction_error_invalidates_the_connection(async_db_url: str) -> None:
    """A real 25P01 through the real dialect must retire the connection.

    The unit tests prove the classification given a hand-built exception chain;
    this pins the chain shape the dialect actually produces and SQLAlchemy's
    resulting invalidation, so a dependency upgrade that changes either fails
    loudly instead of silently disarming the listener.
    """
    settings = Settings.model_validate({"database_url": SecretStr(async_db_url)})
    engine = create_db_engine(settings.async_database_url)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))  # autobegin: client holds an open transaction
            raw = await connection.get_raw_connection()
            assert raw.driver_connection is not None
            await raw.driver_connection.execute("ROLLBACK")  # server idle; client state now stale
            with pytest.raises(DBAPIError) as raised:
                await connection.begin_nested()  # SAVEPOINT -> SQLSTATE 25P01
            assert raised.value.connection_invalidated
    finally:
        await engine.dispose()
