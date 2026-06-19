"""Integration tests for the Maple Syrup backed-breakdown repository.

Each test module gets its own isolated database (see ``module_db`` in
``tests/integration/conftest.py``) with every migration pre-applied, so the
``maple`` protocol, the underlying ``USDC`` token and the ``syrupUSDC``
``receipt_token`` are already seeded by the migrations. We only insert the
pool / loan / collateral / state snapshot rows the scenario needs.
"""

import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.backed_breakdown_repository_maple import MapleBackedBreakdownRepository
from tests.integration.seed import insert_user

SYRUP_USDC = bytes.fromhex("80ac24aa929eaf5013f6436cda2a7ba190f5cc0b")
# maple_loan_collateral PK is (maple_loan_id, synced_at, processing_version): one
# collateral asset per loan per snapshot. Multi-asset backing => multiple loans.
_EXTERNAL_LOAN_BTC = bytes.fromhex("1111111111111111111111111111111111111111")
_EXTERNAL_LOAN_CBBTC = bytes.fromhex("1212121212121212121212121212121212121212")
_INTERNAL_LOAN = bytes.fromhex("2222222222222222222222222222222222222222")
_INACTIVE_LOAN = bytes.fromhex("3333333333333333333333333333333333333333")
_BORROWER = bytes.fromhex("4444444444444444444444444444444444444444")
_SYNCED = dt.datetime(2026, 6, 18, 12, 0, tzinfo=dt.timezone.utc)


async def _insert_loan(
    conn: asyncpg.Connection, protocol_id: int, pool_id: int, user_id: int, address: bytes, meta: str | None
) -> int:
    return await conn.fetchval(
        """
        INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id, loan_meta_type)
        VALUES (1, $1, $2, $3, $4, $5)
        RETURNING id
        """,
        protocol_id,
        address,
        pool_id,
        user_id,
        meta,
    )


async def _insert_loan_state(
    conn: asyncpg.Connection, loan_id: int, state: str, principal_owed: int, acm_ratio: int | None
) -> None:
    await conn.execute(
        """
        INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio)
        VALUES ($1, $2, $3, $4, $5)
        """,
        loan_id,
        _SYNCED,
        state,
        Decimal(principal_owed),
        Decimal(acm_ratio) if acm_ratio is not None else None,
    )


async def _insert_collateral(
    conn: asyncpg.Connection, loan_id: int, symbol: str, amount: int, decimals: int, value_usd: int
) -> None:
    await conn.execute(
        """
        INSERT INTO maple_loan_collateral
            (maple_loan_id, synced_at, asset_symbol, asset_amount, asset_decimals, asset_value_usd, state)
        VALUES ($1, $2, $3, $4, $5, $6, 'Deposited')
        """,
        loan_id,
        _SYNCED,
        symbol,
        Decimal(amount),
        decimals,
        Decimal(value_usd),
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE chain_id = 1 AND name = 'maple'")
        usdc_id = await conn.fetchval("SELECT id FROM token WHERE chain_id = 1 AND symbol = 'USDC'")
        assert protocol_id is not None and usdc_id is not None

        pool_id = await conn.fetchval(
            """
            INSERT INTO maple_pool (chain_id, protocol_id, address, name, asset_token_id, is_syrup)
            VALUES (1, $1, $2, 'Syrup USDC', $3, true)
            RETURNING id
            """,
            protocol_id,
            SYRUP_USDC,
            usdc_id,
        )
        # Pool liquidity: 1,000,000 USDC (6 decimals).
        await conn.execute(
            """
            INSERT INTO maple_pool_state
                (maple_pool_id, synced_at, tvl, liquid_assets, collateral_value_usd,
                 principal_out, utilization, monthly_apy, spot_apy)
            VALUES ($1, $2, NULL, 1000000000000, NULL, 0, 0, 0, 0)
            """,
            pool_id,
            _SYNCED,
        )

        borrower_id = await insert_user(conn, _BORROWER)

        # External active loans (one collateral asset each):
        #   BTC 2 @ $65,000 -> $130,000; cbBTC 1 @ $64,000 -> $64,000.
        ext_btc = await _insert_loan(conn, protocol_id, pool_id, borrower_id, _EXTERNAL_LOAN_BTC, None)
        await _insert_loan_state(conn, ext_btc, "Active", 120000000000, 1656007)
        await _insert_collateral(conn, ext_btc, "BTC", 200000000, 8, 6500000000000)

        ext_cbbtc = await _insert_loan(conn, protocol_id, pool_id, borrower_id, _EXTERNAL_LOAN_CBBTC, None)
        await _insert_loan_state(conn, ext_cbbtc, "Active", 60000000000, 1656007)
        await _insert_collateral(conn, ext_cbbtc, "cbBTC", 100000000, 8, 6400000000000)

        # Internal amm loan: placeholder USDC collateral — must be excluded (is_internal).
        internal = await _insert_loan(conn, protocol_id, pool_id, borrower_id, _INTERNAL_LOAN, "amm")
        await _insert_loan_state(conn, internal, "Active", 100000000000, 1000000)
        await _insert_collateral(conn, internal, "USDC", 100000000000, 6, 100000000)

        # Inactive (Closed) external loan — must be excluded (state != 'Active').
        inactive = await _insert_loan(conn, protocol_id, pool_id, borrower_id, _INACTIVE_LOAN, None)
        await _insert_loan_state(conn, inactive, "Closed", 0, None)
        await _insert_collateral(conn, inactive, "ETH", 1000000000000000000, 18, 99900000000)
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, _seed_data: None) -> AsyncIterator[MapleBackedBreakdownRepository]:
    engine = create_async_engine(async_db_url)
    try:
        yield MapleBackedBreakdownRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_external_collateral_plus_liquidity(repository: MapleBackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)

    by_symbol = {i.symbol: i for i in result.items}
    assert set(by_symbol) == {"BTC", "cbBTC", "USDC"}  # USDC = available liquidity
    assert by_symbol["BTC"].backing_value == Decimal("130000.00")
    assert by_symbol["cbBTC"].backing_value == Decimal("64000.00")
    assert by_symbol["USDC"].backing_value == Decimal("1000000.00")
    assert by_symbol["BTC"].price_usd == Decimal("65000.00000000")
    assert by_symbol["USDC"].price_usd == Decimal("1.00000000")


@pytest.mark.asyncio(loop_scope="module")
async def test_all_token_ids_are_none(repository: MapleBackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    assert all(i.token_id is None for i in result.items)


@pytest.mark.asyncio(loop_scope="module")
async def test_percentages_sum_to_100(repository: MapleBackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    total = Decimal("130000") + Decimal("64000") + Decimal("1000000")
    by_symbol = {i.symbol: i for i in result.items}
    assert abs(sum(i.backing_pct for i in result.items) - Decimal("100")) <= Decimal("0.05")
    assert abs(by_symbol["BTC"].backing_pct - (Decimal("130000") / total * 100)) <= Decimal("0.05")


@pytest.mark.asyncio(loop_scope="module")
async def test_items_ordered_by_value_desc(repository: MapleBackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    values = [i.backing_value for i in result.items]
    assert values == sorted(values, reverse=True)


@pytest.mark.asyncio(loop_scope="module")
async def test_unknown_pool_raises(repository: MapleBackedBreakdownRepository) -> None:
    with pytest.raises(ValueError, match="maple syrup pool not found"):
        await repository.get_backed_breakdown(bytes.fromhex("dead" * 10), chain_id=1)
