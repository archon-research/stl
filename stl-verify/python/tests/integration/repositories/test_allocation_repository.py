"""Integration tests for the allocation repository USD-exposure methods.

Regression coverage for the ghost-balance bug: positions whose latest balance
is zero must not resurface their last non-zero balance.  The original query
applied ``balance > 0`` inside the DISTINCT ON / LIMIT 1 latest-row selection,
so sweep-to-zero rows were skipped.

Uses a shared TimescaleDB container (session-scoped) with an isolated database
per test module; the ``module_db`` fixture from ``conftest.py`` applies
migrations and ``seed_ghost_balance`` loads the test rows.
"""

import asyncio
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    GHOST_CLOSED_PROXY_HEX,
    GHOST_OPEN_PROXY_HEX,
    GHOST_RECEIPT_SYRUP_HEX,
    seed_ghost_balance,
)


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed ghost-balance test data into the module's isolated database and yield the async URL."""
    asyncio.run(seed_ghost_balance(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture()
async def receipt_token_id(async_db_url: str) -> int:
    """Resolve the aSyrupUSDT receipt-token id seeded by ``seed_ghost_balance``."""
    conn = await asyncpg.connect(async_db_url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        return await conn.fetchval(
            "SELECT id FROM receipt_token WHERE chain_id = 1 AND receipt_token_address = $1",
            bytes.fromhex(GHOST_RECEIPT_SYRUP_HEX),
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_get_total_usd_exposure_excludes_swept_positions(repo) -> None:
    """A fully swept prime has zero total exposure, not its stale balance × price."""
    prime_id = EthAddress(f"0x{GHOST_CLOSED_PROXY_HEX}")
    result = await repo.get_total_usd_exposure(prime_id)
    assert result == Decimal("0"), f"Expected 0 total USD exposure for fully-swept prime, got {result}"


@pytest.mark.asyncio
async def test_get_total_usd_exposure_for_open_position_is_priced(repo) -> None:
    """An open position contributes balance × price (500 × 2 = 1000) to the total."""
    prime_id = EthAddress(f"0x{GHOST_OPEN_PROXY_HEX}")
    result = await repo.get_total_usd_exposure(prime_id)
    assert result == Decimal("1000"), f"Expected priced total of 1000 for open position, got {result}"


@pytest.mark.asyncio
async def test_get_usd_exposure_raises_for_swept_position(repo, receipt_token_id: int) -> None:
    """A position swept to 0 raises instead of resurfacing a stale balance."""
    prime_id = EthAddress(f"0x{GHOST_CLOSED_PROXY_HEX}")
    with pytest.raises(ValueError, match="no position or price found"):
        await repo.get_usd_exposure(receipt_token_id, prime_id)


@pytest.mark.asyncio
async def test_get_usd_exposure_returns_priced_balance_for_open_position(repo, receipt_token_id: int) -> None:
    """An open position returns balance × price (500 × 2 = 1000)."""
    prime_id = EthAddress(f"0x{GHOST_OPEN_PROXY_HEX}")
    result = await repo.get_usd_exposure(receipt_token_id, prime_id)
    assert result == Decimal("1000"), f"Expected priced exposure of 1000, got {result}"
