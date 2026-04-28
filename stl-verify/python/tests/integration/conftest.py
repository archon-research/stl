"""Shared fixtures for integration tests.

A single TimescaleDB container is started once per test session.  Each test
module that needs database access requests its own isolated database within
that container via the ``module_db`` fixture.  This gives every module a
clean schema (migrations are applied independently) while avoiding the cost
of spinning up multiple Docker containers.
"""

import asyncio
import pathlib
from typing import cast

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parents[3] / "db" / "migrations"
TIMESCALEDB_IMAGE = "timescale/timescaledb:2.25.1-pg17"


# ---------------------------------------------------------------------------
# Session-scoped container (shared by all test modules)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container():
    """Start a single TimescaleDB container for the entire test session."""
    with PostgresContainer(
        image=TIMESCALEDB_IMAGE,
        username="postgres",
        password="postgres",
        dbname="postgres",
    ) as container:
        yield container


@pytest.fixture(scope="session")
def pg_base_url(pg_container) -> str:
    """Return a plain ``postgresql://`` URL pointing at the session container's
    default ``postgres`` database. Used only for administrative operations
    (CREATE DATABASE, etc.).
    """
    host = pg_container.get_container_host_ip()
    port = pg_container.get_exposed_port(5432)
    return f"postgresql://postgres:postgres@{host}:{port}/postgres"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_database(admin_url: str, db_name: str) -> None:
    """Create a fresh database inside the running container.

    Connects to the ``postgres`` database to issue ``CREATE DATABASE``.
    """
    conn = await asyncpg.connect(admin_url)
    try:
        await conn.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        await conn.close()


async def _run_migrations(dsn: str) -> None:
    """Execute every migration file in filename order using asyncpg."""
    conn = await asyncpg.connect(dsn)
    try:
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            sql = sql_file.read_text()
            # CONCURRENTLY cannot run inside a transaction block.
            # In tests there is no concurrent traffic, so it is safe
            # to run the plain (non-concurrent) variant instead.
            sql = sql.replace(" CONCURRENTLY", "")
            try:
                await conn.execute(sql)
            except Exception as exc:
                raise RuntimeError(f"Migration failed: {sql_file.name}") from exc
    finally:
        await conn.close()


def _db_url_for(pg_container, db_name: str) -> str:
    """Build a plain ``postgresql://`` URL for *db_name*."""
    host = pg_container.get_container_host_ip()
    port = pg_container.get_exposed_port(5432)
    return f"postgresql://postgres:postgres@{host}:{port}/{db_name}"


# ---------------------------------------------------------------------------
# Per-module database fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def module_db(request, pg_container, pg_base_url):
    """Create an isolated database for the calling test module.

    The database name is derived from the module's file name so that every
    module gets its own namespace.  Migrations are applied automatically.

    Yields a dict with two keys:

    * ``db_url``    -- plain ``postgresql://`` URL  (for asyncpg)
    * ``async_url`` -- ``postgresql+asyncpg://`` URL  (for SQLAlchemy async)
    """
    module_name = pathlib.Path(request.fspath).stem  # e.g. "test_allocation_api"
    db_name = module_name.replace(".", "_")

    db_url = _db_url_for(pg_container, db_name)
    async_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    asyncio.run(_create_database(pg_base_url, db_name))
    asyncio.run(_run_migrations(db_url))

    yield {"db_url": db_url, "async_url": async_url}


# ---------------------------------------------------------------------------
# Shared per-module DB URL fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_url(module_db) -> str:
    """Plain ``postgresql://`` URL for the module's isolated database."""
    return module_db["db_url"]


@pytest.fixture(scope="module")
def async_db_url(module_db) -> str:
    """SQLAlchemy async URL for the module's isolated database."""
    return module_db["async_url"]


# ---------------------------------------------------------------------------
# Shared seed helpers
# ---------------------------------------------------------------------------


async def insert_token(conn: asyncpg.Connection, symbol: str, decimals: int, address: bytes) -> int:
    """Insert a token or return the existing ID."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO token (chain_id, address, symbol, decimals)
        VALUES (1, $1, $2, $3)
        ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
        RETURNING id
        """,
            address,
            symbol,
            decimals,
        ),
    )


async def insert_user(conn: asyncpg.Connection, address: bytes) -> int:
    """Insert a user or return the existing ID (upsert on chain_id + address)."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO "user" (chain_id, address)
        VALUES (1, $1)
        ON CONFLICT (chain_id, address) DO UPDATE SET updated_at = NOW()
        RETURNING id
        """,
            address,
        ),
    )


async def insert_receipt_token(
    db_url: str,
    chain_id: int,
    address: bytes,
    symbol: str = "aUSDC",
) -> int:
    """Insert a receipt_token row (upserting on chain_id + address) and return its ID.

    Picks an arbitrary protocol and token from the given ``chain_id`` to
    satisfy the foreign-key constraints.
    """
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = cast(
            int,
            await conn.fetchval(
                "SELECT id FROM protocol WHERE chain_id = $1 LIMIT 1",
                chain_id,
            ),
        )
        token_id = cast(
            int,
            await conn.fetchval(
                "SELECT id FROM token WHERE chain_id = $1 LIMIT 1",
                chain_id,
            ),
        )
        return cast(
            int,
            await conn.fetchval(
                """
                INSERT INTO receipt_token
                    (protocol_id, underlying_token_id, receipt_token_address, symbol,
                     created_at_block, chain_id)
                VALUES ($1, $2, $3, $4, 1, $5)
                ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                    DO UPDATE SET symbol = EXCLUDED.symbol
                RETURNING id
                """,
                protocol_id,
                token_id,
                address,
                symbol,
                chain_id,
            ),
        )
    finally:
        await conn.close()


def composite_mapping_key(chain_id: int, address: bytes) -> str:
    """Build a ``chain_id:0xAddress`` composite key for the asset mapping JSON."""
    return f"{chain_id}:0x{address.hex()}"


async def store_test_ids(conn: asyncpg.Connection, ids: dict[str, int]) -> None:
    """Persist seed IDs into a helper table so test fixtures can retrieve them."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _test_ids (
            key TEXT PRIMARY KEY,
            val BIGINT NOT NULL
        )
        """
    )
    for key, val in ids.items():
        await conn.execute("INSERT INTO _test_ids (key, val) VALUES ($1, $2)", key, val)
