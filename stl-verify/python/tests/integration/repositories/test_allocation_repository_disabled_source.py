"""Integration tests: a retired price source is excluded from latest-price reads.

Disabling an ``oracle_asset`` mapping must retire that source from every
current/latest price read immediately at read time. The seed reproduces the
production hazard: a reorg republishes a frozen (retired) source's row at a
FRESH block next to a live source whose change-suppressed feed has not written
a newer row, so the retired source holds the max-block row (and the later
timestamp, and the higher oracle_id). Recency and the oracle_id tiebreak both
favour the retired source, so only excluding its disabled mapping yields the
live source's LOWER-block price.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_disabled_source_positions``.
"""

import asyncio

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    DIS_DIRECT_BALANCE,
    DIS_DIRECT_PROXY_HEX,
    DIS_ENABLED_PRICE,
    DIS_RECEIPT_PROXY_HEX,
    DIS_RECEIPT_UNDERLYING_VALUE,
    seed_disabled_source_positions,
)

_RECEIPT_PRIME = EthAddress(f"0x{DIS_RECEIPT_PROXY_HEX}")
_DIRECT_PRIME = EthAddress(f"0x{DIS_DIRECT_PROXY_HEX}")

_EXPECTED_RECEIPT_USD = DIS_RECEIPT_UNDERLYING_VALUE * DIS_ENABLED_PRICE
_EXPECTED_DIRECT_USD = DIS_DIRECT_BALANCE * DIS_ENABLED_PRICE


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the retired-source scenario and yield the async URL."""
    asyncio.run(seed_disabled_source_positions(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_receipt_positions_exclude_disabled_higher_block_source(repo) -> None:
    """list_receipt_token_positions values the position at the enabled source's lower-block price."""
    positions = await repo.list_receipt_token_positions(_RECEIPT_PRIME)
    position = {p.symbol: p for p in positions}.get("disReceipt")
    assert position is not None
    assert position.amount_usd == _EXPECTED_RECEIPT_USD


@pytest.mark.asyncio
async def test_direct_holdings_exclude_disabled_higher_block_source(repo) -> None:
    """list_direct_asset_holdings values the bare holding at the enabled source's lower-block price."""
    holding = {h.symbol: h for h in await repo.list_direct_asset_holdings(_DIRECT_PRIME)}.get("disDirect")
    assert holding is not None
    assert holding.amount_usd == _EXPECTED_DIRECT_USD


async def _receipt_token_id(db_url: str) -> int:
    conn = await asyncpg.connect(db_url)
    try:
        return await conn.fetchval("SELECT id FROM receipt_token WHERE symbol = 'disReceipt'")
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def dis_receipt_token_id(async_db_url: str, db_url: str) -> int:
    """The seeded disReceipt receipt_token id (async_db_url only for seeding)."""
    return asyncio.run(_receipt_token_id(db_url))


@pytest.mark.asyncio
async def test_usd_exposure_excludes_disabled_higher_block_source(repo, dis_receipt_token_id) -> None:
    """get_usd_exposure values the position at the enabled source's lower-block price."""
    exposure = await repo.get_usd_exposure(dis_receipt_token_id, _RECEIPT_PRIME)
    assert exposure == _EXPECTED_RECEIPT_USD


@pytest.mark.asyncio
async def test_total_usd_exposure_excludes_disabled_higher_block_source(repo) -> None:
    """get_total_usd_exposure sums using the enabled source's lower-block price."""
    total = await repo.get_total_usd_exposure(_RECEIPT_PRIME)
    assert total == _EXPECTED_RECEIPT_USD
