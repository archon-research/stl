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
async def test_returns_empty_for_prime_with_no_custody_snapshots(repo) -> None:
    """A known prime with no Anchorage rows yields an empty list, not an error."""
    holdings = await repo.list_anchorage_custody_holdings(EthAddress(f"0x{ANCHORAGE_EMPTY_PROXY_HEX}"))

    assert holdings == []
