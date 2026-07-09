"""Integration tests for redeemable-value pricing of receipt-token positions.

Every position-valuation read (``list_receipt_token_positions``,
``get_usd_exposure``, ``get_total_usd_exposure``, ``list_exposure_buckets``)
values a receipt position at ``COALESCE(underlying_value, balance) x underlying
price``: ``underlying_value`` is the on-chain redeemable value (convertToAssets),
so non-1:1 vault shares (syrupUSDC-like) stop being priced as if one share were
one underlying unit. NULL ``underlying_value`` (rows written before the column
existed) falls back to the balance basis; 1:1 aTokens are unchanged because
their ``underlying_value`` equals ``balance`` by construction.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_receipt_underlying_value_positions``.
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
    RUV_ATOKEN_BALANCE,
    RUV_DIVERGENT_BALANCE,
    RUV_DIVERGENT_UNDERLYING_VALUE,
    RUV_LEGACY_BALANCE,
    RUV_PROXY_HEX,
    RUV_SYRUP_LIKE_BALANCE,
    RUV_SYRUP_LIKE_UNDERLYING_VALUE,
    RUV_UNDERLYING_PRICE,
    seed_receipt_underlying_value_positions,
)

_PRIME = EthAddress(f"0x{RUV_PROXY_HEX}")

_EXPECTED_TOTAL = (
    RUV_SYRUP_LIKE_UNDERLYING_VALUE + RUV_LEGACY_BALANCE + RUV_ATOKEN_BALANCE + RUV_DIVERGENT_UNDERLYING_VALUE
) * RUV_UNDERLYING_PRICE

# One row per valuation basis: (symbol, expected share balance, expected USD).
_VALUATION_CASES = [
    # Non-1:1 share ratio: redeemable value drives the USD figure, balance stays shares.
    ("syrupLike", RUV_SYRUP_LIKE_BALANCE, RUV_SYRUP_LIKE_UNDERLYING_VALUE * RUV_UNDERLYING_PRICE),
    # NULL underlying_value (legacy row): balance-based fallback.
    ("legacyReceipt", RUV_LEGACY_BALANCE, RUV_LEGACY_BALANCE * RUV_UNDERLYING_PRICE),
    # 1:1 aToken (underlying_value == balance): value unchanged.
    ("aOneToOne", RUV_ATOKEN_BALANCE, RUV_ATOKEN_BALANCE * RUV_UNDERLYING_PRICE),
]


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the receipt redeemable-value scenarios and yield the async URL."""
    asyncio.run(seed_receipt_underlying_value_positions(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


async def _fetch_receipt_token_ids(db_url: str) -> dict[str, int]:
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT id, symbol FROM receipt_token WHERE chain_id = 1")
        return {row["symbol"]: row["id"] for row in rows}
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def receipt_token_ids(async_db_url: str) -> dict[str, int]:
    """Map seeded receipt-token symbols to their receipt_token ids."""
    return asyncio.run(_fetch_receipt_token_ids(async_db_url.replace("postgresql+asyncpg://", "postgresql://")))


async def _position(repo: AllocationRepository, symbol: str):
    positions = await repo.list_receipt_token_positions(_PRIME)
    return {p.symbol: p for p in positions}.get(symbol)


@pytest.mark.parametrize(("symbol", "expected_balance", "expected_amount_usd"), _VALUATION_CASES)
@pytest.mark.asyncio
async def test_receipt_position_amount_usd_uses_redeemable_value(
    repo, symbol, expected_balance, expected_amount_usd
) -> None:
    """list_receipt_token_positions values each basis correctly and keeps balance as shares."""
    position = await _position(repo, symbol)
    assert position is not None
    assert position.balance == expected_balance
    assert position.amount_usd == expected_amount_usd


@pytest.mark.parametrize(("symbol", "_expected_balance", "expected_amount_usd"), _VALUATION_CASES)
@pytest.mark.asyncio
async def test_usd_exposure_uses_redeemable_value(
    repo, receipt_token_ids, symbol, _expected_balance, expected_amount_usd
) -> None:
    """get_usd_exposure applies the same redeemable-value basis per receipt token."""
    exposure = await repo.get_usd_exposure(receipt_token_ids[symbol], _PRIME)
    assert exposure == expected_amount_usd


@pytest.mark.asyncio
async def test_total_usd_exposure_sums_redeemable_values(repo) -> None:
    """get_total_usd_exposure sums the redeemable-value USD figure across positions."""
    total = await repo.get_total_usd_exposure(_PRIME)
    assert total == _EXPECTED_TOTAL


@pytest.mark.asyncio
async def test_exposure_buckets_value_positions_at_redeemable_value(repo) -> None:
    """list_exposure_buckets carries the redeemable value forward, not the share balance."""
    now = dt.datetime.now(dt.UTC)
    buckets = await repo.list_exposure_buckets(
        _PRIME,
        from_timestamp=now - dt.timedelta(hours=1),
        to_timestamp=now + dt.timedelta(hours=1),
        bucket_seconds=3600.0,
        limit=10,
    )
    non_null = [b.exposure_usd for b in buckets if b.exposure_usd is not None]
    assert non_null, "expected at least one priced bucket in the seeded window"
    assert all(value == _EXPECTED_TOTAL for value in non_null)


@pytest.mark.asyncio
async def test_receipt_pricing_follows_registry_underlying(repo) -> None:
    """A receipt position is priced by the registry's underlying, not its own underlying_token_id.

    Guard for the redeemable-value change: pricing keeps joining
    ``receipt_token.underlying_token_id`` (the curated registry) rather than
    ``allocation_position.underlying_token_id``. Verified against the production
    warehouse on 2026-07-09: of 5484 receipt-position rows carrying an
    ``underlying_token_id``, zero diverged from the registry's, so a divergence
    (as seeded here) indicates an ingest bug and the registry stays authoritative.
    """
    position = await _position(repo, "divergentReceipt")
    assert position is not None
    # Registry underlying is priced at RUV_UNDERLYING_PRICE (1.00); the position's
    # own (divergent) underlying is priced at 5.00 and must not be used.
    assert position.amount_usd == RUV_DIVERGENT_UNDERLYING_VALUE * RUV_UNDERLYING_PRICE
    assert position.balance == RUV_DIVERGENT_BALANCE
