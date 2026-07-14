"""Integration tests for deterministic oracle-price tie resolution.

A protocol bound to several oracles can carry price rows for one underlying at
IDENTICAL (block_number, block_version, processing_version) — the frozen
sparklend sources re-emit fixed rows on republished blocks next to live
chainlink rows (warehouse-verified at block 25524078/1/0). Every receipt-path
valuation read must break that tie on ``oracle_id DESC`` so identical API
calls return identical USD figures; the higher id is the later-registered
oracle, and a retired source only ever loses the tie until the live source's
next row lands.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_price_tiebreak_positions``, which inserts the lower-oracle_id (stale)
row first to bias an un-tiebroken top-1 sort toward the wrong answer.
"""

import asyncio
import datetime as dt

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    TIE_FRESH_PRICE,
    TIE_PROXY_HEX,
    TIE_UNDERLYING_VALUE,
    seed_price_tiebreak_positions,
)

_PRIME = EthAddress(f"0x{TIE_PROXY_HEX}")

_EXPECTED_AMOUNT_USD = TIE_UNDERLYING_VALUE * TIE_FRESH_PRICE


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the price-tie scenario and yield the async URL."""
    asyncio.run(seed_price_tiebreak_positions(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


async def _receipt_token_id(db_url: str) -> int:
    conn = await asyncpg.connect(db_url)
    try:
        return await conn.fetchval("SELECT id FROM receipt_token WHERE symbol = 'tieReceipt'")
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def tie_receipt_token_id(async_db_url: str, db_url: str) -> int:
    """The seeded tieReceipt receipt_token id (async_db_url only for seeding)."""
    return asyncio.run(_receipt_token_id(db_url))


@pytest.mark.asyncio
async def test_receipt_positions_price_tie_resolves_to_highest_oracle_id(repo) -> None:
    """list_receipt_token_positions values the position at the higher-oracle_id price."""
    positions = await repo.list_receipt_token_positions(_PRIME)
    position = {p.symbol: p for p in positions}.get("tieReceipt")
    assert position is not None
    assert position.amount_usd == _EXPECTED_AMOUNT_USD


@pytest.mark.asyncio
async def test_usd_exposure_price_tie_resolves_to_highest_oracle_id(repo, tie_receipt_token_id) -> None:
    """get_usd_exposure resolves the tie identically to the positions list."""
    exposure = await repo.get_usd_exposure(tie_receipt_token_id, _PRIME)
    assert exposure == _EXPECTED_AMOUNT_USD


@pytest.mark.asyncio
async def test_total_usd_exposure_price_tie_resolves_to_highest_oracle_id(repo) -> None:
    """get_total_usd_exposure sums using the higher-oracle_id price."""
    total = await repo.get_total_usd_exposure(_PRIME)
    assert total == _EXPECTED_AMOUNT_USD


@pytest.mark.asyncio
async def test_activity_buckets_net_flow_price_tie_resolves_to_highest_oracle_id(repo) -> None:
    """list_activity_buckets values the seeded inflow at the higher-oracle_id price.

    Flows are valued at the row's share ratio (``underlying_value / balance``;
    see ``_ALLOCATION_ACTIVITY_BUCKETS_SQL``). The seeded inflow spends its full
    balance (``tx_amount == balance``), so ``tx_amount x ratio == underlying_value``
    and the flow equals the position figure; the ratio is folded into the
    expectation so this pins ONLY the oracle tie (fresh 1.25 beats stale 1.00).
    """
    now = dt.datetime.now(dt.UTC)
    buckets = await repo.list_activity_buckets(
        prime_id=_PRIME,
        from_timestamp=now - dt.timedelta(hours=1),
        to_timestamp=now + dt.timedelta(hours=1),
        bucket_seconds=7200.0,
        limit=10,
    )
    assert len(buckets) == 1
    assert buckets[0].net_flow_usd == TIE_UNDERLYING_VALUE * TIE_FRESH_PRICE
