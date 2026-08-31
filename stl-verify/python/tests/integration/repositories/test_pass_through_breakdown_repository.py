"""Integration tests for the pass-through breakdown repository.

Covers the single-token aggregate over ``allocation_position_current`` that
backs the risk-breakdown fallback for directly-held allocated assets:
aggregation across proxies, the underlying-differs collapse, price resolution,
SubProxy exclusion, and the proxy-address filter.
"""

from collections.abc import AsyncIterator
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.pass_through_breakdown_repository import PassThroughBreakdownRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    PT_POSITIONLESS_TOKEN_HEX,
    PT_PROXY_A_HEX,
    PT_PROXY_B_HEX,
    PT_SELF_TOKEN_HEX,
    PT_UNPRICED_TOKEN_HEX,
    PT_WRAPPER_TOKEN_HEX,
    seed_pass_through_positions,
)

# The module-scoped engine fixture lives on the module loop; every test must
# run there too or asyncpg sees a cross-loop Future.
pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def test_ids(db_url: str) -> dict[str, int]:
    await seed_pass_through_positions(db_url)
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT key, val FROM _test_ids")
        return {row["key"]: row["val"] for row in rows}
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, test_ids: dict[str, int]) -> AsyncIterator[PassThroughBreakdownRepository]:
    engine = create_async_engine(async_db_url)
    try:
        yield PassThroughBreakdownRepository(engine)
    finally:
        await engine.dispose()


async def test_aggregates_balances_across_all_prime_proxies(
    repository: PassThroughBreakdownRepository, test_ids: dict[str, int]
) -> None:
    holding = await repository.get_holding(1, EthAddress("0x" + PT_SELF_TOKEN_HEX))

    assert holding is not None
    assert holding.token_id == test_ids["pt_self_id"]
    assert holding.symbol == "PTSELF"
    assert holding.amount == Decimal("157.5")  # 100.5 + 50 + 7, SubProxy treasury row excluded
    assert holding.price_usd == Decimal("1.0002")


async def test_narrows_to_the_given_proxy_addresses(
    repository: PassThroughBreakdownRepository, test_ids: dict[str, int]
) -> None:
    holding = await repository.get_holding(
        1,
        EthAddress("0x" + PT_SELF_TOKEN_HEX),
        proxy_addresses=[EthAddress("0x" + PT_PROXY_A_HEX), EthAddress("0x" + PT_PROXY_B_HEX)],
    )

    assert holding is not None
    assert holding.amount == Decimal("150.5")


async def test_collapses_to_the_underlying_when_it_differs(
    repository: PassThroughBreakdownRepository, test_ids: dict[str, int]
) -> None:
    holding = await repository.get_holding(1, EthAddress("0x" + PT_WRAPPER_TOKEN_HEX))

    assert holding is not None
    assert holding.token_id == test_ids["pt_underlying_id"]
    assert holding.symbol == "PTUSDC"
    assert holding.amount == Decimal("20320203.5")
    assert holding.price_usd == Decimal("0.9999")


async def test_unpriced_token_yields_null_price(
    repository: PassThroughBreakdownRepository, test_ids: dict[str, int]
) -> None:
    holding = await repository.get_holding(1, EthAddress("0x" + PT_UNPRICED_TOKEN_HEX))

    assert holding is not None
    assert holding.amount == Decimal("42")
    assert holding.price_usd is None


async def test_token_without_positions_yields_none(
    repository: PassThroughBreakdownRepository, test_ids: dict[str, int]
) -> None:
    assert await repository.get_holding(1, EthAddress("0x" + PT_POSITIONLESS_TOKEN_HEX)) is None


async def test_unknown_token_yields_none(repository: PassThroughBreakdownRepository, test_ids: dict[str, int]) -> None:
    assert await repository.get_holding(1, EthAddress("0x" + "ee" * 20)) is None
