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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
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
        protocol_id = await conn.fetchval(
            "SELECT id FROM protocol WHERE chain_id = $1 LIMIT 1",
            chain_id,
        )
        if protocol_id is None:
            raise RuntimeError(f"no protocol seed found for chain_id={chain_id}")
        token_id = await conn.fetchval(
            "SELECT id FROM token WHERE chain_id = $1 LIMIT 1",
            chain_id,
        )
        if token_id is None:
            raise RuntimeError(f"no token seed found for chain_id={chain_id}")
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


# ---------------------------------------------------------------------------
# SQLAlchemy seed primitives
#
# The allocation tests seed through a SQLAlchemy ``AsyncConnection`` (the same
# engine type the production repositories use) rather than the raw-asyncpg
# helpers above.  Unifying the suite on one seed style is tracked separately.
# ---------------------------------------------------------------------------


async def insert_token_sa(conn: AsyncConnection, addr_hex: str, symbol: str, decimals: int) -> int:
    """Insert a chain-1 token and return its id."""
    return (
        await conn.execute(
            text(
                "INSERT INTO token (chain_id, address, symbol, decimals) "
                "VALUES (1, decode(:addr, 'hex'), :symbol, :decimals) RETURNING id"
            ),
            {"addr": addr_hex, "symbol": symbol, "decimals": decimals},
        )
    ).scalar_one()


async def insert_allocation_position(
    conn: AsyncConnection,
    *,
    token_id: int,
    prime_id: int,
    proxy_hex: str,
    balance: int,
    block: int,
    tx: str,
    direction: str,
    log_index: int = 0,
    block_version: int = 0,
) -> None:
    """Insert one allocation_position row (chain_id=1, tx_amount=balance)."""
    await conn.execute(
        text(
            "INSERT INTO allocation_position "
            "(chain_id, token_id, prime_id, proxy_address, balance, "
            "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
            "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), :bal, :bn, :bv, "
            "decode(:tx, 'hex'), :li, :bal, :dir)"
        ),
        {
            "tid": token_id,
            "pid": prime_id,
            "proxy": proxy_hex,
            "bal": balance,
            "bn": block,
            "bv": block_version,
            "tx": tx,
            "dir": direction,
            "li": log_index,
        },
    )


# ---------------------------------------------------------------------------
# Ghost-balance regression seed
#
# Shared by the allocation API tests (``test_allocation_api``) and the
# allocation repository tests (``test_allocation_repository``).  The original
# bug applied ``balance > 0`` inside the DISTINCT ON / LIMIT 1 latest-row
# selection, so sweep-to-zero rows were skipped and the last non-zero balance
# resurfaced as an open position.  These rows belong to their own
# ``ghost_balance`` prime so they never masquerade as the migration-seeded
# primes (spark/grove/obex) in ``/v1/primes``.
# ---------------------------------------------------------------------------

# The proxy_kind classifier marks an address as ALM unless it matches a known
# sub-proxy.  Any address that is NOT the Spark sub-proxy will do.
GHOST_CLOSED_PROXY_HEX = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # all positions closed to zero
GHOST_SWEEP_PROXY_HEX = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"  # zero sweep rows newer than the non-zero row
GHOST_OPEN_PROXY_HEX = "cccccccccccccccccccccccccccccccccccccccc"  # open position with older zero rows
GHOST_MIXED_PROXY_HEX = "dddddddddddddddddddddddddddddddddddddddd"  # one swept + one open token
GHOST_TIEBREAK_PROXY_HEX = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"  # same-block rows, log_index decides

# Token addresses, not used by migrations.
_GHOST_SYRUP_USDT_HEX = "1111111111111111111111111111111111111111"
_GHOST_USDS_HEX = "2222222222222222222222222222222222222222"
GHOST_RECEIPT_SYRUP_HEX = "3333333333333333333333333333333333333333"  # receipt token wrapping syrupUSDT

# Vault address for the ghost_balance prime (20 bytes, unique in the prime table).
_GHOST_VAULT_HEX = "99" * 20

# Transaction hashes (32 bytes).
_GHOST_TXA = "aa" * 32
_GHOST_TXB = "bb" * 32
_GHOST_TXC = "cc" * 32
_GHOST_TXD = "dd" * 32
_GHOST_TXE = "ee" * 32
_GHOST_TXF = "ff" * 32


async def _ghost_seed_reference_rows(conn: AsyncConnection) -> tuple[int, int, int]:
    """Create the ghost_balance prime, tokens, receipt-token registration, and a syrupUSDT price of 2 USD.

    Returns (prime_id, asyrup_token_id, usds_token_id).
    """
    prime_id = (
        await conn.execute(
            text(
                "INSERT INTO prime (name, vault_address) VALUES ('ghost_balance', decode(:vault, 'hex')) RETURNING id"
            ),
            {"vault": _GHOST_VAULT_HEX},
        )
    ).scalar_one()
    # protocol.name is not unique across chains; chain_id pins mainnet.
    protocol_id = (
        await conn.execute(text("SELECT id FROM protocol WHERE name = 'Aave V3' AND chain_id = 1"))
    ).scalar_one()
    oracle_id = (await conn.execute(text("SELECT id FROM oracle WHERE name = 'aave_v3'"))).scalar_one()

    syrup_id = await insert_token_sa(conn, _GHOST_SYRUP_USDT_HEX, "syrupUSDT", 6)
    usds_id = await insert_token_sa(conn, _GHOST_USDS_HEX, "USDS", 18)
    asyrup_id = await insert_token_sa(conn, GHOST_RECEIPT_SYRUP_HEX, "aSyrupUSDT", 6)

    await conn.execute(
        text(
            "INSERT INTO receipt_token "
            "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
            "VALUES (1, :pid, :uid, decode(:addr, 'hex'), 'aSyrupUSDT')"
        ),
        {"pid": protocol_id, "uid": syrup_id, "addr": GHOST_RECEIPT_SYRUP_HEX},
    )
    await conn.execute(
        text(
            "INSERT INTO onchain_token_price "
            "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
            "VALUES (:tid, :oid, 2000, 0, NOW(), 2)"
        ),
        {"tid": syrup_id, "oid": oracle_id},
    )
    return prime_id, asyrup_id, usds_id


async def _ghost_seed_closed_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """Both holdings closed to zero: aSyrupUSDT 100 then 0, USDS 75894 then 0.

    The latest balance is 0 for both, so neither token may appear in any query.
    """
    for token_id, rows in [
        (asyrup_id, [(1000, 100, _GHOST_TXA, "in"), (2000, 0, _GHOST_TXB, "out")]),
        (usds_id, [(1000, 75894, _GHOST_TXC, "in"), (2000, 0, _GHOST_TXD, "out")]),
    ]:
        for block, bal, tx, direction in rows:
            await insert_allocation_position(
                conn,
                token_id=token_id,
                prime_id=prime_id,
                proxy_hex=GHOST_CLOSED_PROXY_HEX,
                balance=bal,
                block=block,
                tx=tx,
                direction=direction,
            )


async def _ghost_seed_sweep_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int) -> None:
    """The production sweep shape: five zero-balance sweep rows newer than the non-zero row."""
    await insert_allocation_position(
        conn,
        token_id=asyrup_id,
        prime_id=prime_id,
        proxy_hex=GHOST_SWEEP_PROXY_HEX,
        balance=68231707,
        block=2000,
        tx=_GHOST_TXE,
        direction="in",
    )
    for sweep_block in range(2001, 2006):
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_SWEEP_PROXY_HEX,
            balance=0,
            block=sweep_block,
            tx=_GHOST_TXF,
            direction="sweep",
        )


async def _ghost_seed_open_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int) -> None:
    """Open position with an older zero row; must still appear with balance=500."""
    for block, bal, tx, direction in [(999, 0, _GHOST_TXA, "out"), (1000, 500, _GHOST_TXB, "in")]:
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_OPEN_PROXY_HEX,
            balance=bal,
            block=block,
            tx=tx,
            direction=direction,
        )


async def _ghost_seed_mixed_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """One swept receipt token (250 then 0) and one open USDS holding (1000); only USDS may appear."""
    for block, bal, tx, direction in [(3000, 250, _GHOST_TXA, "in"), (3001, 0, _GHOST_TXB, "out")]:
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_MIXED_PROXY_HEX,
            balance=bal,
            block=block,
            tx=tx,
            direction=direction,
        )
    await insert_allocation_position(
        conn,
        token_id=usds_id,
        prime_id=prime_id,
        proxy_hex=GHOST_MIXED_PROXY_HEX,
        balance=1000,
        block=3000,
        tx=_GHOST_TXC,
        direction="in",
    )


async def _ghost_seed_tiebreak_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """All rows in block 4000, so log_index alone orders them.

    aSyrupUSDT is deposited then fully withdrawn within the block and must not
    appear; USDS is the mirror shape and ends the block at 400.
    """
    for log_index, bal, direction in [(0, 300, "in"), (1, 0, "out")]:
        await insert_allocation_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=GHOST_TIEBREAK_PROXY_HEX,
            balance=bal,
            block=4000,
            tx=_GHOST_TXD,
            direction=direction,
            log_index=log_index,
        )
    for log_index, bal, direction in [(0, 0, "out"), (1, 400, "in")]:
        await insert_allocation_position(
            conn,
            token_id=usds_id,
            prime_id=prime_id,
            proxy_hex=GHOST_TIEBREAK_PROXY_HEX,
            balance=bal,
            block=4000,
            tx=_GHOST_TXE,
            direction=direction,
            log_index=log_index,
        )


async def seed_ghost_balance(async_url: str) -> None:
    """Seed all ghost-balance proxy scenarios into the given database."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            prime_id, asyrup_id, usds_id = await _ghost_seed_reference_rows(conn)
            await _ghost_seed_closed_proxy(conn, prime_id, asyrup_id, usds_id)
            await _ghost_seed_sweep_proxy(conn, prime_id, asyrup_id)
            await _ghost_seed_open_proxy(conn, prime_id, asyrup_id)
            await _ghost_seed_mixed_proxy(conn, prime_id, asyrup_id, usds_id)
            await _ghost_seed_tiebreak_proxy(conn, prime_id, asyrup_id, usds_id)
    finally:
        await engine.dispose()
