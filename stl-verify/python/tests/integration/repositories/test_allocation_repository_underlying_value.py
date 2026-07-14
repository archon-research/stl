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
    UV_PROXY_SYMBOLLESS_UNDERLYING,
    UV_PROXY_UNDERLYING_UNPRICED,
    UV_PROXY_UNIV3_POOL,
    UV_SPARK_OWN_PRICE,
    UV_SPARKPRIME_BALANCE,
    UV_SPARKPRIME_UNDERLYING_VALUE,
    UV_UNIV3_UNDERLYING_VALUE,
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
        # Allowlisted Uni V3 pool position (not an ERC20, never has its own
        # oracle) -> tracker-computed underlying_value x USDC price.
        (UV_PROXY_UNIV3_POOL, "AUSDUSDC-UNIV3", UV_UNIV3_UNDERLYING_VALUE * UV_USDC_PRICE),
    ],
)
@pytest.mark.asyncio
async def test_direct_holding_amount_usd_by_pricing_branch(repo, proxy_hex, symbol, expected_amount_usd) -> None:
    """Each pricing branch selects the intended valuation source."""
    holding = await _holding(repo, proxy_hex, symbol)
    assert holding is not None
    assert holding.amount_usd == expected_amount_usd


@pytest.mark.asyncio
async def test_allowlisted_holding_carries_underlying_identity(repo) -> None:
    """An allowlisted holding priced from its underlying reports that underlying's
    id/address/symbol, so consumers keyed on underlying_* match the price basis."""
    usdc = await _holding(repo, UV_PROXY_PLAIN, "USDC")
    univ3 = await _holding(repo, UV_PROXY_UNIV3_POOL, "AUSDUSDC-UNIV3")
    assert univ3.underlying_token_id == usdc.token_id
    assert univ3.underlying_token_address == usdc.token_address
    assert univ3.underlying_symbol == "USDC"


@pytest.mark.asyncio
async def test_allowlisted_unpriced_underlying_still_carries_identity(repo) -> None:
    """The identity travels with the price basis, not with pricing success: an
    allowlisted holding whose underlying has no oracle price reports that
    underlying's identity alongside a NULL amount_usd (a surfaced coverage gap)."""
    holding = await _holding(repo, UV_PROXY_UNDERLYING_UNPRICED, "sparkPrimeUSDC1")
    assert holding.amount_usd is None
    assert holding.underlying_symbol == "NOPRICEUND"
    assert holding.underlying_token_id is not None
    assert holding.underlying_token_address is not None


@pytest.mark.parametrize(
    ("proxy_hex", "symbol"),
    [
        # The row carries an underlying_token_id, but the token is not allowlisted.
        (UV_PROXY_NON_ALLOWLISTED, "unlistedVault"),
        # Plain holding with no underlying on the row at all.
        (UV_PROXY_PLAIN, "USDC"),
        # Allowlisted, but the row carries no underlying (e.g. written before
        # its type's valuation deployed).
        (UV_PROXY_NULL_VALUE, "sparkPrimeUSDC1"),
        # Allowlisted with an underlying whose token row has a NULL symbol:
        # the identity must emit atomically (all three or none), never a
        # hybrid of underlying id/address with the held token's symbol.
        (UV_PROXY_SYMBOLLESS_UNDERLYING, "sparkPrimeUSDC1"),
    ],
)
@pytest.mark.asyncio
async def test_holding_without_projectable_underlying_has_no_identity(repo, proxy_hex, symbol) -> None:
    """Rows outside the allowlisted-with-resolvable-underlying projection emit no
    underlying identity, so the endpoint falls back to the held token as a unit."""
    holding = await _holding(repo, proxy_hex, symbol)
    assert holding.underlying_token_id is None
    assert holding.underlying_token_address is None
    assert holding.underlying_symbol is None


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
