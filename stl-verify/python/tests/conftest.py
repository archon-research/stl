"""Root test conftest.

Session DB + per-test transaction rollback fixtures. Layered:

    pg_container    (session) — one TimescaleDB testcontainer per pytest session
        │
        └─ session_engine (session) — one AsyncEngine to the migrated session DB;
        │                              metadata reflected once
        │
        └─ wrap_conn  (per-test) — pre-opened AsyncConnection + outer txn
        │                          + nested savepoint, rolled back on teardown
        │
        └─ wrap_engine (per-test) — AsyncEngine wrapper whose .connect() re-yields
                                    wrap_conn so repos hit the savepoint
"""

import pathlib
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from tests.db import set_session_metadata

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parents[2] / "db" / "migrations"
TIMESCALEDB_IMAGE = "timescale/timescaledb:2.25.1-pg17"
SESSION_DB_NAME = "stl_verify_test"


async def _create_database(admin_dsn: str, db_name: str) -> None:
    conn = await asyncpg.connect(admin_dsn)
    try:
        try:
            await conn.execute(f'CREATE DATABASE "{db_name}"')
        except Exception as exc:
            raise RuntimeError(f"create database {db_name!r}: {exc}") from exc
    finally:
        await conn.close()


async def _run_migrations(dsn: str) -> None:
    sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not sql_files:
        # Guards against silent typos in ``MIGRATIONS_DIR``: an empty glob would
        # leave the session DB schema-less, which then fails far away as
        # "relation X does not exist" inside individual tests.
        raise RuntimeError(f"no migration files found under {MIGRATIONS_DIR}")
    conn = await asyncpg.connect(dsn)
    try:
        for sql_file in sql_files:
            # CONCURRENTLY cannot run inside a transaction block.
            # Tests have no concurrent traffic, so the plain variant is safe.
            sql = sql_file.read_text().replace(" CONCURRENTLY", "")
            try:
                await conn.execute(sql)
            except Exception as exc:
                raise RuntimeError(f"Migration failed: {sql_file.name}: {exc}") from exc
    finally:
        await conn.close()


def _dsn(container: PostgresContainer, db_name: str) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return f"postgresql://postgres:postgres@{host}:{port}/{db_name}"


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    """Single TimescaleDB testcontainer shared across the pytest session."""
    with PostgresContainer(
        image=TIMESCALEDB_IMAGE,
        username="postgres",
        password="postgres",
        dbname="postgres",
    ) as container:
        yield container


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session_engine(pg_container: PostgresContainer) -> AsyncIterator[AsyncEngine]:
    """One migrated database + AsyncEngine per pytest session.

    Reflects ``MetaData`` once and hands it to ``tests.seeds`` so insert helpers
    can resolve tables by name.
    """
    admin_dsn = _dsn(pg_container, "postgres")
    await _create_database(admin_dsn, SESSION_DB_NAME)

    plain_dsn = _dsn(pg_container, SESSION_DB_NAME)
    await _run_migrations(plain_dsn)

    async_dsn = plain_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(async_dsn)
    try:
        metadata = MetaData()
        async with engine.connect() as conn:
            await conn.run_sync(metadata.reflect)
        set_session_metadata(metadata)

        yield engine
    finally:
        # Dispose runs even if reflection raised, so a failed setup does not
        # leak the connection pool.
        await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def wrap_conn(session_engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """Per-test AsyncConnection with an outer txn + nested savepoint.

    Both are rolled back at teardown so the session DB returns to its post-migration
    state for the next test. Seed fixtures consume this directly so writes land
    inside the same savepoint that repos exercise via ``wrap_engine``.
    """
    async with session_engine.connect() as conn:
        outer = await conn.begin()
        savepoint = await conn.begin_nested()
        try:
            yield conn
        finally:
            # Roll back each layer independently so a failure on one does not
            # mask the other and leave the connection in an unknown state.
            savepoint_err: BaseException | None = None
            outer_err: BaseException | None = None
            if savepoint.is_active:
                try:
                    await savepoint.rollback()
                except Exception as exc:
                    savepoint_err = exc
            if outer.is_active:
                try:
                    await outer.rollback()
                except Exception as exc:
                    outer_err = exc
            if savepoint_err is not None and outer_err is not None:
                raise ExceptionGroup("wrap_conn rollback failed", [savepoint_err, outer_err])
            if savepoint_err is not None:
                raise savepoint_err
            if outer_err is not None:
                raise outer_err


@pytest_asyncio.fixture(loop_scope="session")
async def wrap_engine(session_engine: AsyncEngine, wrap_conn: AsyncConnection) -> AsyncIterator[AsyncEngine]:
    """AsyncEngine wrapper whose ``.connect()`` re-yields ``wrap_conn``.

    Repos call ``async with engine.connect() as conn:`` internally; with this wrap
    every such call hits the savepoint owned by ``wrap_conn``. Repos that fire
    multiple ``engine.connect()`` per call (e.g. ``allocation_position_repository``)
    all share the same savepoint, which keeps cross-statement state consistent.
    """

    class _WrapEngine:
        sync_engine = session_engine.sync_engine
        dialect = session_engine.dialect
        url = session_engine.url

        @asynccontextmanager
        async def connect(self) -> AsyncIterator[AsyncConnection]:
            yield wrap_conn

        async def dispose(self) -> None:
            # Intentional no-op: pool ownership belongs to ``session_engine``.
            # An SUT that disposes "its" engine must not free the shared pool.
            pass

    yield cast(AsyncEngine, _WrapEngine())
