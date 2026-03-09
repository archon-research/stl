from collections.abc import AsyncIterator
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.backed_breakdown_repository import BackedBreakdownRepository

# ---------------------------------------------------------------------------
# Database URL fixtures (derived from the shared module_db)
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
# Seed helpers
# ---------------------------------------------------------------------------


async def _insert_token(conn: asyncpg.Connection, symbol: str, decimals: int, address: bytes) -> int:
    """Insert a token or return the existing ID."""
    return await conn.fetchval(
        """
        INSERT INTO token (chain_id, address, symbol, decimals)
        VALUES (1, $1, $2, $3)
        ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
        RETURNING id
        """,
        address,
        symbol,
        decimals,
    )


async def _insert_oracle_asset(conn: asyncpg.Connection, oracle_id: int, token_id: int) -> None:
    """Register a token with an oracle (idempotent)."""
    await conn.execute(
        """
        INSERT INTO oracle_asset (oracle_id, token_id, enabled)
        VALUES ($1, $2, true)
        ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING
        """,
        oracle_id,
        token_id,
    )


async def _insert_price(
    conn: asyncpg.Connection,
    token_id: int,
    oracle_id: int,
    price: str,
    block_number: int,
    *,
    time_offset: str = "0 seconds",
) -> None:
    """Insert an onchain token price."""
    await conn.execute(
        f"""
        INSERT INTO onchain_token_price
            (token_id, oracle_id, block_number, block_version, timestamp, price_usd)
        VALUES ($1, $2, $3, 0, NOW() + interval '{time_offset}', $4::numeric(30,18))
        """,
        token_id,
        oracle_id,
        block_number,
        price,
    )


async def _insert_reserve(
    conn: asyncpg.Connection, protocol_id: int, token_id: int, block_number: int, *, collateral_enabled: bool
) -> None:
    """Insert a SparkLend reserve data row."""
    await conn.execute(
        """
        INSERT INTO sparklend_reserve_data
            (protocol_id, token_id, block_number, block_version,
             usage_as_collateral_enabled, ltv)
        VALUES ($1, $2, $3, 0, $4, $5)
        """,
        protocol_id,
        token_id,
        block_number,
        collateral_enabled,
        Decimal("8000") if collateral_enabled else Decimal("0"),
    )


async def _insert_user(conn: asyncpg.Connection, address: bytes) -> int:
    """Insert a user and return the ID."""
    return await conn.fetchval(
        """
        INSERT INTO "user" (chain_id, address)
        VALUES (1, $1)
        RETURNING id
        """,
        address,
    )


async def _insert_collateral(
    conn: asyncpg.Connection, user_id: int, protocol_id: int, token_id: int, amount: str, block_number: int
) -> None:
    """Insert a borrower collateral snapshot."""
    await conn.execute(
        """
        INSERT INTO borrower_collateral
            (user_id, protocol_id, token_id, block_number, block_version,
             amount, change, event_type, tx_hash, collateral_enabled)
        VALUES ($1, $2, $3, $4, 0, $5, $5, 'deposit', $6, true)
        """,
        user_id,
        protocol_id,
        token_id,
        block_number,
        amount,
        b"\x00" * 32,
    )


async def _insert_debt(
    conn: asyncpg.Connection, user_id: int, protocol_id: int, token_id: int, amount: int, block_number: int
) -> None:
    """Insert a borrower debt delta."""
    await conn.execute(
        """
        INSERT INTO borrower
            (user_id, protocol_id, token_id, block_number, block_version,
             amount, change, event_type, tx_hash)
        VALUES ($1, $2, $3, $4, 0, $5, $5, 'borrow', $6)
        """,
        user_id,
        protocol_id,
        token_id,
        block_number,
        amount,
        b"\x00" * 32,
    )


async def _store_test_ids(conn: asyncpg.Connection, ids: dict[str, int]) -> None:
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


async def _seed_tokens_and_prices(
    conn: asyncpg.Connection, oracle_id: int, block_number: int
) -> tuple[int, int, int, int]:
    """Create tokens and their oracle prices. Returns (weth_id, cbbtc_id, sp_usds_id, sp_usdc_id)."""
    weth_id = await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1")
    cbbtc_id = await conn.fetchval("SELECT id FROM token WHERE symbol = 'cbBTC' AND chain_id = 1")
    sp_usds_id = await _insert_token(
        conn, "spUSDS", 18, b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14"
    )
    sp_usdc_id = await _insert_token(
        conn, "spUSDC", 6, b"\x21\x22\x23\x24\x25\x26\x27\x28\x29\x2a\x2b\x2c\x2d\x2e\x2f\x30\x31\x32\x33\x34"
    )

    for tid in [weth_id, cbbtc_id, sp_usds_id, sp_usdc_id]:
        await _insert_oracle_asset(conn, oracle_id, tid)

    for token_id, price in [
        (weth_id, "2000.000000000000000000"),
        (cbbtc_id, "50000.000000000000000000"),
        (sp_usds_id, "1.000000000000000000"),
    ]:
        await _insert_price(conn, token_id, oracle_id, price, block_number)
    await _insert_price(conn, sp_usdc_id, oracle_id, "1.000000000000000000", block_number, time_offset="1 second")

    return weth_id, cbbtc_id, sp_usds_id, sp_usdc_id


async def _seed_reserves(
    conn: asyncpg.Connection,
    protocol_id: int,
    block_number: int,
    weth_id: int,
    cbbtc_id: int,
    sp_usds_id: int,
    sp_usdc_id: int,
) -> None:
    """Configure which tokens are usable as collateral."""
    for token_id, enabled in [
        (weth_id, True),
        (cbbtc_id, True),
        (sp_usds_id, False),
        (sp_usdc_id, False),
    ]:
        await _insert_reserve(conn, protocol_id, token_id, block_number, collateral_enabled=enabled)


async def _seed_user1(
    conn: asyncpg.Connection, protocol_id: int, block_number: int, weth_id: int, cbbtc_id: int, sp_usds_id: int
) -> None:
    """User 1: 10 WETH + 0.5 cbBTC collateral, 30 000 spUSDS debt (single-debt borrower)."""
    user_id = await _insert_user(conn, b"\xaa" * 20)
    await _insert_collateral(conn, user_id, protocol_id, weth_id, "10", block_number)
    await _insert_collateral(conn, user_id, protocol_id, cbbtc_id, "0.5", block_number)
    await _insert_debt(conn, user_id, protocol_id, sp_usds_id, 30000, block_number)


async def _seed_user2(
    conn: asyncpg.Connection, protocol_id: int, block_number: int, weth_id: int, sp_usds_id: int, sp_usdc_id: int
) -> None:
    """User 2: 5 WETH collateral, 6 000 spUSDS + 4 000 spUSDC debt (mixed-debt borrower)."""
    user_id = await _insert_user(conn, b"\xbb" * 20)
    await _insert_collateral(conn, user_id, protocol_id, weth_id, "5", block_number)
    await _insert_debt(conn, user_id, protocol_id, sp_usds_id, 6000, block_number)
    await _insert_debt(conn, user_id, protocol_id, sp_usdc_id, 4000, block_number)


_SEED_BLOCK_NUMBER = 20_000_000


# ---------------------------------------------------------------------------
# Seed and repository fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    """Seed test data for backed breakdown tests."""
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE name = 'SparkLend'")
        oracle_id = await conn.fetchval("SELECT id FROM oracle WHERE name = 'sparklend'")
        block = _SEED_BLOCK_NUMBER

        weth_id, cbbtc_id, sp_usds_id, sp_usdc_id = await _seed_tokens_and_prices(conn, oracle_id, block)
        await _seed_reserves(conn, protocol_id, block, weth_id, cbbtc_id, sp_usds_id, sp_usdc_id)
        await _seed_user1(conn, protocol_id, block, weth_id, cbbtc_id, sp_usds_id)
        await _seed_user2(conn, protocol_id, block, weth_id, sp_usds_id, sp_usdc_id)

        await _store_test_ids(
            conn,
            {
                "protocol_id": protocol_id,
                "weth_id": weth_id,
                "cbbtc_id": cbbtc_id,
                "sp_usds_id": sp_usds_id,
                "sp_usdc_id": sp_usdc_id,
            },
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def test_ids(db_url: str, _seed_data: None) -> dict[str, int]:
    """Retrieve seeded IDs for use in test assertions."""
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT key, val FROM _test_ids")
        return {row["key"]: row["val"] for row in rows}
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, _seed_data: None) -> AsyncIterator[BackedBreakdownRepository]:
    """Create a repository backed by the test database."""
    engine = create_async_engine(async_db_url)
    try:
        yield BackedBreakdownRepository(engine)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_single_borrower_full_attribution(
    repository: BackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """User 1 has only spUSDS debt, so 100% of their collateral is attributed.

    User 1: 10 WETH ($20,000) + 0.5 cbBTC ($25,000) collateral, 30,000 spUSDS debt.
    User 2: 5 WETH ($10,000) collateral, 6,000 spUSDS + 4,000 spUSDC debt (60%/40%).

    Attributed to spUSDS:
      User 1 WETH:  $20,000 * 1.0 = $20,000
      User 1 cbBTC: $25,000 * 1.0 = $25,000
      User 2 WETH:  $10,000 * 0.6 = $6,000

    Totals: WETH = $26,000, cbBTC = $25,000, grand total = $51,000
    WETH pct  = 26000/51000 * 100 = 50.9804%
    cbBTC pct = 25000/51000 * 100 = 49.0196%
    """
    result = await repository.get_backed_breakdown(
        protocol_id=test_ids["protocol_id"],
        debt_token_id=test_ids["sp_usds_id"],
    )

    assert result.debt_token_id == test_ids["sp_usds_id"]
    assert result.protocol_id == test_ids["protocol_id"]
    assert len(result.items) == 2

    by_symbol = {item.symbol: item for item in result.items}

    assert "WETH" in by_symbol
    assert "cbBTC" in by_symbol

    assert by_symbol["WETH"].total_backing_usd == Decimal("26000.00")
    assert by_symbol["cbBTC"].total_backing_usd == Decimal("25000.00")

    assert by_symbol["WETH"].backing_pct == Decimal("50.9804")
    assert by_symbol["cbBTC"].backing_pct == Decimal("49.0196")


@pytest.mark.asyncio(loop_scope="module")
async def test_no_borrowers_returns_empty(repository: BackedBreakdownRepository, test_ids: dict[str, int]) -> None:
    """A token nobody has borrowed returns an empty breakdown."""
    result = await repository.get_backed_breakdown(
        protocol_id=test_ids["protocol_id"],
        debt_token_id=test_ids["weth_id"],  # nobody borrows WETH in test data
    )

    assert result.items == ()


@pytest.mark.asyncio(loop_scope="module")
async def test_nonexistent_protocol_returns_empty(
    repository: BackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """A non-existent protocol ID returns an empty breakdown."""
    result = await repository.get_backed_breakdown(
        protocol_id=99999,
        debt_token_id=test_ids["sp_usds_id"],
    )

    assert result.items == ()
