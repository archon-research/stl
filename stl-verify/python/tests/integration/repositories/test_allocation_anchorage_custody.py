"""Integration tests for the Anchorage BTC custody repository read.

Encodes the $521M trap: an unbounded ``DISTINCT ON`` over every package would
leak three closed packages (active=true, frozen at an earlier snapshot_time)
into the totals. The correct read scopes to the prime's latest snapshot_time
cohort first, resolves ``processing_version`` corrections, then collapses
packages into one ``(asset_type, custody_type)`` row.

Uses a shared TimescaleDB container (session-scoped) with an isolated database
per test module; ``module_db`` applies migrations and ``seed_anchorage_custody``
loads the cohort + trap rows.
"""

import asyncio
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    ANCHORAGE_CUSTODY_PROXY_HEX,
    ANCHORAGE_EMPTY_PROXY_HEX,
    ANCHORAGE_LATEST_SNAPSHOT,
    ANCHORAGE_MULTI_PROXY_HEX,
    ANCHORAGE_OTHER_PROXY_HEX,
    seed_anchorage_custody,
)


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed Anchorage custody test data into the module DB and yield the async URL."""
    asyncio.run(seed_anchorage_custody(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_collapses_current_cohort_into_one_btc_custody_row(repo) -> None:
    """Every package in the latest cohort collapses into a single BTC/AnchorageCustody
    row stamped with the cohort's snapshot_time.
    """
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_CUSTODY_PROXY_HEX}"))

    assert len(holdings) == 1
    row = holdings[0]
    assert row.symbol == "BTC"
    assert row.custody_type == "AnchorageCustody"
    assert row.as_of == ANCHORAGE_LATEST_SNAPSHOT


@pytest.mark.asyncio
async def test_cohort_filter_excludes_stale_closed_packages(repo) -> None:
    """Closed packages (active=true, frozen at the earlier 2026-06-13 poll) must
    not leak collateral/BTC: collateral is 309,672,229 (not the unscoped
    521,000,000) and BTC is 4722.61 (not 7222.61).
    """
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_CUSTODY_PROXY_HEX}"))

    row = holdings[0]
    assert row.collateral_usd == Decimal("309672229")
    assert row.balance == Decimal("4722.61")


@pytest.mark.asyncio
async def test_processing_version_correction_supersedes_original(repo) -> None:
    """The pv-1 correction on PKG-C wins over the pv-0 original, so exposure sums
    to 250,000,000 rather than the original's 1,199,000,000.
    """
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_CUSTODY_PROXY_HEX}"))

    assert holdings[0].amount_usd == Decimal("250000000")


@pytest.mark.asyncio
async def test_active_predicate_excludes_inactive_package_in_cohort(repo) -> None:
    """A package in the latest cohort but flagged active=false is excluded by the
    ``AND aps.active`` predicate — not the cohort filter, which it shares (same
    max snapshot_time). Its non-zero values (exposure 88M, BTC 800) would inflate
    the sums if surfaced, so exposure staying 250,000,000 / BTC 4722.61 proves
    the predicate is load-bearing.
    """
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_CUSTODY_PROXY_HEX}"))

    assert len(holdings) == 1
    assert holdings[0].amount_usd == Decimal("250000000")
    assert holdings[0].balance == Decimal("4722.61")


@pytest.mark.asyncio
async def test_returns_empty_for_prime_with_no_custody_snapshots(repo) -> None:
    """A known prime with no Anchorage rows yields an empty list, not an error."""
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_EMPTY_PROXY_HEX}"))

    assert holdings == []


@pytest.mark.asyncio
async def test_multiple_asset_type_groups_ordered_by_amount_desc(repo) -> None:
    """A cohort spanning two asset-type groups yields two rows ordered by
    ``amount_usd`` DESC. Balances are per asset type (not affected by the
    package-level double-count): BTC 13, ETH 7.
    """
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_MULTI_PROXY_HEX}"))

    assert [h.symbol for h in holdings] == ["BTC", "ETH"]  # DESC by amount_usd (160 > 40)
    by_symbol = {h.symbol: h for h in holdings}
    assert by_symbol["BTC"].balance == Decimal("13")
    assert by_symbol["ETH"].balance == Decimal("7")


@pytest.mark.asyncio
async def test_multi_asset_package_loan_not_double_counted_across_groups(repo) -> None:
    """PKG-MX's single package-level loan/collateral (30 / 45) is attributed once —
    to BTC, its dominant collateral — not added to the ETH group as well. So the
    ETH group shows amount 40 / collateral 50 (its own single-asset package only),
    and the cross-group totals equal the true per-package sums (amount 200,
    collateral 245). An unguarded ``GROUP BY`` double-counts PKG-MX into ETH
    (amount 70 / collateral 95, totals 230 / 290).
    """
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_MULTI_PROXY_HEX}"))
    by_symbol = {h.symbol: h for h in holdings}

    assert by_symbol["ETH"].amount_usd == Decimal("40")
    assert by_symbol["ETH"].collateral_usd == Decimal("50")
    assert by_symbol["BTC"].amount_usd == Decimal("160")
    assert sum(h.amount_usd for h in holdings) == Decimal("200")
    assert sum(h.collateral_usd for h in holdings) == Decimal("245")


@pytest.mark.asyncio
async def test_cohort_is_scoped_per_prime(repo) -> None:
    """A second prime with its own later-polled cohort must not perturb the main
    prime, and returns only its own row. Proves the per-prime ``prime_id`` scope
    on both ``MAX(snapshot_time)`` and the cohort filter is load-bearing: without
    it the second prime's later poll would starve the main prime.
    """
    main = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_CUSTODY_PROXY_HEX}"))
    other = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_OTHER_PROXY_HEX}"))

    assert len(main) == 1
    assert main[0].amount_usd == Decimal("250000000")  # unchanged by the other prime
    assert len(other) == 1
    assert other[0].amount_usd == Decimal("99")
    assert other[0].balance == Decimal("9")
