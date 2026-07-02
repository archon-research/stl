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
from tests.integration.seed import (
    insert_maple_loan,
    insert_maple_loan_collateral,
    insert_maple_loan_state,
    insert_maple_pool,
    insert_maple_pool_state,
    insert_user,
    maple_seed_ids,
)

SYRUP_USDC = bytes.fromhex("80ac24aa929eaf5013f6436cda2a7ba190f5cc0b")
# maple_loan_collateral PK is (maple_loan_id, synced_at, processing_version): one
# collateral asset per loan per snapshot. Multi-asset backing => multiple loans.
_EXTERNAL_LOAN_BTC = bytes.fromhex("1111111111111111111111111111111111111111")
_EXTERNAL_LOAN_CBBTC = bytes.fromhex("1212121212121212121212121212121212121212")
_INTERNAL_LOAN = bytes.fromhex("2222222222222222222222222222222222222222")
_INACTIVE_LOAN = bytes.fromhex("3333333333333333333333333333333333333333")
_BORROWER = bytes.fromhex("4444444444444444444444444444444444444444")
_SYNCED = dt.datetime(2026, 6, 18, 12, 0, tzinfo=dt.timezone.utc)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id, usdc_id = await maple_seed_ids(conn)

        pool_id = await insert_maple_pool(
            conn, protocol_id=protocol_id, address=SYRUP_USDC, asset_token_id=usdc_id, synced_at=_SYNCED
        )
        # Pool liquidity: 1,000,000 USDC (6 decimals).
        await insert_maple_pool_state(conn, pool_id=pool_id, synced_at=_SYNCED, liquid_assets=1000000000000)

        borrower_id = await insert_user(conn, _BORROWER)

        async def seed_loan(address: bytes, meta: str | None, state: str, principal: int, acm: int | None) -> int:
            loan_id = await insert_maple_loan(
                conn,
                protocol_id=protocol_id,
                pool_id=pool_id,
                borrower_user_id=borrower_id,
                address=address,
                synced_at=_SYNCED,
                loan_meta_type=meta,
            )
            await insert_maple_loan_state(
                conn,
                loan_id=loan_id,
                synced_at=_SYNCED,
                state=state,
                principal_owed=principal,
                acm_ratio=acm,
            )
            return loan_id

        # External active loans (one collateral asset each):
        #   BTC 2 @ $65,000 -> $130,000; cbBTC 1 @ $64,000 -> $64,000.
        ext_btc = await seed_loan(_EXTERNAL_LOAN_BTC, None, "Active", 120000000000, 1656007)
        await insert_maple_loan_collateral(
            conn,
            loan_id=ext_btc,
            synced_at=_SYNCED,
            symbol="BTC",
            amount=200000000,
            decimals=8,
            value_usd=6500000000000,
        )

        ext_cbbtc = await seed_loan(_EXTERNAL_LOAN_CBBTC, None, "Active", 60000000000, 1656007)
        await insert_maple_loan_collateral(
            conn,
            loan_id=ext_cbbtc,
            synced_at=_SYNCED,
            symbol="cbBTC",
            amount=100000000,
            decimals=8,
            value_usd=6400000000000,
        )

        # Internal amm loan: placeholder USDC collateral — must be excluded (is_internal).
        internal = await seed_loan(_INTERNAL_LOAN, "amm", "Active", 100000000000, 1000000)
        await insert_maple_loan_collateral(
            conn,
            loan_id=internal,
            synced_at=_SYNCED,
            symbol="USDC",
            amount=100000000000,
            decimals=6,
            value_usd=100000000,
        )

        # Inactive (Closed) external loan — must be excluded (state != 'Active').
        inactive = await seed_loan(_INACTIVE_LOAN, None, "Closed", 0, None)
        await insert_maple_loan_collateral(
            conn,
            loan_id=inactive,
            synced_at=_SYNCED,
            symbol="ETH",
            amount=1000000000000000000,
            decimals=18,
            value_usd=99900000000,
        )
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
async def test_unknown_pool_returns_empty(repository: MapleBackedBreakdownRepository) -> None:
    # A registered-but-not-yet-indexed (or absent) syrup pool degrades to an
    # empty breakdown rather than raising — the asset is known, data is not.
    result = await repository.get_backed_breakdown(bytes.fromhex("dead" * 10), chain_id=1)
    assert result.items == ()
    assert result.backed_asset_id == 0
