"""Integration tests for scoped ``underlying_value`` pricing of direct holdings.

VEC-450 (B4): ``list_direct_asset_holdings`` values an allowlisted vault
(``sparkPrimeUSDC1``) from ``allocation_position.underlying_value`` x the
underlying's oracle price. Allowlisted tokens price ONLY by that basis (NULL,
and surfaced, if the underlying price is missing) and never fall through to the
legacy ``balance x own-oracle price``; every non-allowlisted holding keeps its
legacy pricing unchanged.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_underlying_value_direct_holdings``.
"""

import asyncio
import logging

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    UV_PROXY_NON_ALLOWLISTED,
    UV_PROXY_NULL_VALUE,
    UV_PROXY_PLAIN,
    UV_PROXY_PRICED,
    UV_PROXY_SPARK_USDC_BC,
    UV_PROXY_UNDERLYING_UNPRICED,
    UV_SPARK_OWN_PRICE,
    UV_SPARK_USDC_BC_UNDERLYING_VALUE,
    UV_SPARKPRIME_BALANCE,
    UV_SPARKPRIME_UNDERLYING_VALUE,
    UV_USDC_BALANCE,
    UV_USDC_PRICE,
    seed_underlying_value_direct_holdings,
)

_REPO_LOGGER = "app.adapters.postgres.allocation_position_repository"


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the underlying-value direct-holdings scenarios and yield the async URL."""
    asyncio.run(seed_underlying_value_direct_holdings(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


async def _holding(repo: AllocationRepository, proxy_hex: str, symbol: str):
    """Return the named direct holding for a proxy, or None if absent."""
    holdings = await repo.list_direct_asset_holdings(EthAddress(f"0x{proxy_hex}"))
    return {h.symbol: h for h in holdings}.get(symbol)


@pytest.mark.parametrize(
    ("proxy_hex", "symbol", "expected_amount_usd"),
    [
        # Allowlisted vault, underlying_value + priced underlying -> underlying-value basis.
        (UV_PROXY_PRICED, "sparkPrimeUSDC1", UV_SPARKPRIME_UNDERLYING_VALUE * UV_USDC_PRICE),
        # Allowlisted vault, underlying oracle missing -> NULL, not the legacy basis.
        (UV_PROXY_UNDERLYING_UNPRICED, "sparkPrimeUSDC1", None),
        # Allowlisted vault, underlying_value NULL -> NULL, not the legacy basis.
        (UV_PROXY_NULL_VALUE, "sparkPrimeUSDC1", None),
        # Same shape as the priced case but not allowlisted -> legacy path (no own oracle -> NULL).
        (UV_PROXY_NON_ALLOWLISTED, "unlistedVault", None),
        # Plain token held directly -> legacy balance x own price.
        (UV_PROXY_PLAIN, "USDC", UV_USDC_BALANCE * UV_USDC_PRICE),
        # Allowlisted sparkUSDCbc (Morpho vault share held bare, no own oracle)
        # -> underlying_value x USDC price.
        (UV_PROXY_SPARK_USDC_BC, "sparkUSDCbc", UV_SPARK_USDC_BC_UNDERLYING_VALUE * UV_USDC_PRICE),
    ],
)
@pytest.mark.asyncio
async def test_direct_holding_amount_usd_by_pricing_branch(repo, proxy_hex, symbol, expected_amount_usd) -> None:
    """Each pricing branch selects the intended valuation source."""
    holding = await _holding(repo, proxy_hex, symbol)
    assert holding is not None
    assert holding.amount_usd == expected_amount_usd


@pytest.mark.asyncio
async def test_allowlisted_vault_ignores_own_share_oracle(repo) -> None:
    """An allowlisted vault with its own oracle is still valued by underlying_value, never balance x own price."""
    holding = await _holding(repo, UV_PROXY_PRICED, "sparkPrimeUSDC1")
    assert holding.amount_usd == UV_SPARKPRIME_UNDERLYING_VALUE * UV_USDC_PRICE
    assert holding.amount_usd != UV_SPARKPRIME_BALANCE * UV_SPARK_OWN_PRICE


class _CapturingHandler(logging.Handler):
    """Collects emitted records; attached directly to the target logger so
    capture does not depend on propagation (the app disables it), which would
    otherwise make this test order-dependent."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.mark.asyncio
async def test_allowlisted_vault_unpriced_underlying_is_surfaced(repo) -> None:
    """An allowlisted vault that resolves to no USD value is warned about, not silently unpriced."""
    handler = _CapturingHandler()
    target = logging.getLogger(_REPO_LOGGER)
    previous_level = target.level
    target.addHandler(handler)
    target.setLevel(logging.WARNING)
    try:
        await repo.list_direct_asset_holdings(EthAddress(f"0x{UV_PROXY_UNDERLYING_UNPRICED}"))
    finally:
        target.removeHandler(handler)
        target.setLevel(previous_level)
    flagged = [r for r in handler.records if "sparkPrimeUSDC1" in getattr(r, "allowlisted_unpriced_symbols", [])]
    assert flagged, "expected a warning flagging the allowlisted vault that resolved to no USD value"
