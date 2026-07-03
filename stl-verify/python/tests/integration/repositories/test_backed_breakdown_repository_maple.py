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
    insert_token,
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
_PENDING_LOAN = bytes.fromhex("5555555555555555555555555555555555555555")
_STALE_LOAN = bytes.fromhex("6666666666666666666666666666666666666666")
_REPAID_LOAN = bytes.fromhex("7777777777777777777777777777777777777777")
_SYNCED = dt.datetime(2026, 6, 18, 12, 0, tzinfo=dt.timezone.utc)
_SYNCED_OLD = dt.datetime(2026, 6, 17, 12, 0, tzinfo=dt.timezone.utc)

# Isolated pools for scenarios that would otherwise pollute the SYRUP_USDC assertions.
_PV_POOL = bytes.fromhex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
_PV_LOAN = bytes.fromhex("abababababababababababababababababababab")
_NONSTABLE_POOL = bytes.fromhex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
_NONSTABLE_LOAN = bytes.fromhex("bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc")
_WBTC = bytes.fromhex("2260fac5e5542a773aa44fbcfedf7c193bc2c599")


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

        # DepositPending collateral with NULL amount/value — must be excluded
        # (query filters asset_amount / asset_value_usd IS NOT NULL).
        pending = await seed_loan(_PENDING_LOAN, None, "Active", 50000000000, 1656007)
        await insert_maple_loan_collateral(
            conn,
            loan_id=pending,
            synced_at=_SYNCED,
            symbol="PENDINGBTC",
            amount=None,
            decimals=8,
            value_usd=None,
            state="DepositPending",
        )

        # Collateral attached to an older snapshot than the loan's latest
        # loan_state — must be excluded (collateral joins the latest state on
        # synced_at + processing_version, never mixing stale collateral with
        # fresh principal). Loan is Active + external, so only the snapshot
        # mismatch keeps it out.
        stale = await seed_loan(_STALE_LOAN, None, "Active", 70000000000, 1656007)
        await insert_maple_loan_collateral(
            conn,
            loan_id=stale,
            synced_at=_SYNCED_OLD,
            symbol="STALEBTC",
            amount=100000000,
            decimals=8,
            value_usd=7000000000000,
        )

        # Repaid loan whose WHOLE latest snapshot (state + collateral) is
        # self-consistent but from an older cycle than the pool's current cycle
        # (_SYNCED). The indexer only queries Active loans and never emits
        # tombstones, so a repaid loan's last Active rows linger in
        # maple_loan_current forever. The query is bounded to the pool's current
        # cycle, so this $9,000,000 must NOT count as live backing (B1) — unlike
        # _STALE_LOAN (fresh state, stale collateral), here even the latest state
        # is from the old cycle.
        repaid = await insert_maple_loan(
            conn,
            protocol_id=protocol_id,
            pool_id=pool_id,
            borrower_user_id=borrower_id,
            address=_REPAID_LOAN,
            synced_at=_SYNCED_OLD,
            loan_meta_type=None,
        )
        await insert_maple_loan_state(
            conn,
            loan_id=repaid,
            synced_at=_SYNCED_OLD,
            state="Active",
            principal_owed=90000000000,
            acm_ratio=1656007,
        )
        await insert_maple_loan_collateral(
            conn,
            loan_id=repaid,
            synced_at=_SYNCED_OLD,
            symbol="REPAIDBTC",
            amount=100000000,
            decimals=8,
            value_usd=9000000000000,
        )

        # Reprocess pool: one loan snapshot re-processed within the same cycle
        # (_SYNCED). build_id 0 -> processing_version 0 (OLDVERBTC), build_id 1 ->
        # processing_version 1 (NEWVERBTC). The query must select the latest
        # processing_version, so NEWVERBTC counts and OLDVERBTC is dropped.
        pv_pool = await insert_maple_pool(
            conn, protocol_id=protocol_id, address=_PV_POOL, asset_token_id=usdc_id, synced_at=_SYNCED
        )
        await insert_maple_pool_state(conn, pool_id=pv_pool, synced_at=_SYNCED, liquid_assets=0)
        pv_loan = await insert_maple_loan(
            conn,
            protocol_id=protocol_id,
            pool_id=pv_pool,
            borrower_user_id=borrower_id,
            address=_PV_LOAN,
            synced_at=_SYNCED,
        )
        for build_id, (sym, value) in enumerate((("OLDVERBTC", 5000000000000), ("NEWVERBTC", 6000000000000))):
            await insert_maple_loan_state(
                conn, loan_id=pv_loan, synced_at=_SYNCED, state="Active", principal_owed=40000000000, build_id=build_id
            )
            await insert_maple_loan_collateral(
                conn,
                loan_id=pv_loan,
                synced_at=_SYNCED,
                symbol=sym,
                amount=100000000,
                decimals=8,
                value_usd=value,
                build_id=build_id,
            )

        # Non-stable-underlying pool: WBTC is outside the stablecoin allowlist, so
        # its liquid_assets must NOT be valued at $1 — the pool contributes only its
        # loan collateral, no liquidity row.
        wbtc_id = await insert_token(conn, "WBTC", 8, _WBTC)
        ns_pool = await insert_maple_pool(
            conn,
            protocol_id=protocol_id,
            address=_NONSTABLE_POOL,
            asset_token_id=wbtc_id,
            synced_at=_SYNCED,
            name="Syrup WBTC",
        )
        await insert_maple_pool_state(conn, pool_id=ns_pool, synced_at=_SYNCED, liquid_assets=500000000)
        ns_loan = await insert_maple_loan(
            conn,
            protocol_id=protocol_id,
            pool_id=ns_pool,
            borrower_user_id=borrower_id,
            address=_NONSTABLE_LOAN,
            synced_at=_SYNCED,
        )
        await insert_maple_loan_state(
            conn, loan_id=ns_loan, synced_at=_SYNCED, state="Active", principal_owed=10000000000
        )
        await insert_maple_loan_collateral(
            conn,
            loan_id=ns_loan,
            synced_at=_SYNCED,
            symbol="ETH",
            amount=1000000000000000000,
            decimals=18,
            value_usd=300000000000,
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
async def test_null_valued_collateral_is_excluded(repository: MapleBackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    assert "PENDINGBTC" not in {i.symbol for i in result.items}


@pytest.mark.asyncio(loop_scope="module")
async def test_stale_collateral_snapshot_is_excluded(repository: MapleBackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    assert "STALEBTC" not in {i.symbol for i in result.items}


@pytest.mark.asyncio(loop_scope="module")
async def test_internal_loan_collateral_is_excluded(repository: MapleBackedBreakdownRepository) -> None:
    # The internal amm loan holds $100,000 of placeholder USDC collateral. It must
    # be excluded (NOT l.is_internal), so the USDC row equals pool liquidity alone.
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    by_symbol = {i.symbol: i for i in result.items}
    assert by_symbol["USDC"].backing_value == Decimal("1000000.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_non_active_loan_is_excluded(repository: MapleBackedBreakdownRepository) -> None:
    # The Closed external loan holds ETH collateral; only Active loans back the
    # vault, so ETH must not appear.
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    assert "ETH" not in {i.symbol for i in result.items}


@pytest.mark.asyncio(loop_scope="module")
async def test_latest_processing_version_is_selected(repository: MapleBackedBreakdownRepository) -> None:
    # A reprocess of the same cycle appends processing_version 1; the query must
    # read it and drop the superseded processing_version 0 collateral.
    result = await repository.get_backed_breakdown(_PV_POOL, chain_id=1)
    symbols = {i.symbol for i in result.items}
    assert "NEWVERBTC" in symbols
    assert "OLDVERBTC" not in symbols


@pytest.mark.asyncio(loop_scope="module")
async def test_non_stable_underlying_omits_liquidity(repository: MapleBackedBreakdownRepository) -> None:
    # WBTC underlying is outside the stablecoin allowlist, so pool liquidity is not
    # valued at $1: the breakdown carries the loan collateral but no WBTC liquidity row.
    result = await repository.get_backed_breakdown(_NONSTABLE_POOL, chain_id=1)
    symbols = {i.symbol for i in result.items}
    assert "ETH" in symbols  # the pool resolves and its collateral is present
    assert "WBTC" not in symbols  # liquidity omitted rather than valued at $1


@pytest.mark.asyncio(loop_scope="module")
async def test_loan_absent_from_current_cycle_is_excluded(repository: MapleBackedBreakdownRepository) -> None:
    # A repaid loan lingers in maple_loan_current (no tombstones) with a stale
    # but self-consistent Active snapshot; the current-cycle bound must drop it so
    # it never counts as live backing forever (B1).
    result = await repository.get_backed_breakdown(SYRUP_USDC, chain_id=1)
    assert "REPAIDBTC" not in {i.symbol for i in result.items}


@pytest.mark.asyncio(loop_scope="module")
async def test_unknown_pool_returns_empty(repository: MapleBackedBreakdownRepository) -> None:
    # A registered-but-not-yet-indexed (or absent) syrup pool degrades to an
    # empty breakdown rather than raising — the asset is known, data is not.
    result = await repository.get_backed_breakdown(bytes.fromhex("dead" * 10), chain_id=1)
    assert result.items == ()
    assert result.backed_asset_id == 0
