"""Integration tests for share-ratio valuation of receipt-token flows.

``list_activity_buckets`` values a receipt-token flow at ``tx_amount x
(underlying_value / balance) x underlying price``. Every allocation_position
row is per-tx and carries ``balance`` and ``underlying_value`` pinned to its
own block, so the per-row quotient is the vault share ratio at that tx's
block and converts the share-denominated ``tx_amount`` into underlying units
before pricing. Guard rows fall back explicitly: NULL ``underlying_value``
(legacy rows) and ``balance = 0`` (full exits, ratio undefined) value the flow
at the raw ``tx_amount``; ``underlying_value = 0`` with ``balance > 0``
(drained vault) values it at 0, matching the position basis. A row whose own
``underlying_token_id`` diverges from the registry's is refused (contributes
nothing), matching every other valuation read.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_flow_share_ratio_activity``.
"""

import asyncio
import datetime as dt
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    FR_BUCKET_TS,
    FR_FULL_EXIT_TX_AMOUNT,
    FR_LEGACY_TX_AMOUNT,
    FR_MIXED_IN_BALANCE,
    FR_MIXED_IN_TX_AMOUNT,
    FR_MIXED_IN_UNDERLYING_VALUE,
    FR_MIXED_LEGACY_TX_AMOUNT,
    FR_MIXED_OUT_BALANCE,
    FR_MIXED_OUT_TX_AMOUNT,
    FR_MIXED_OUT_UNDERLYING_VALUE,
    FR_PROXY_DIVERGENT,
    FR_PROXY_DRAINED,
    FR_PROXY_FULL_EXIT,
    FR_PROXY_LEGACY,
    FR_PROXY_MIXED,
    FR_PROXY_RATIO,
    FR_RATIO_BALANCE,
    FR_RATIO_TX_AMOUNT,
    FR_RATIO_UNDERLYING_VALUE,
    FR_UNDERLYING_PRICE,
    seed_flow_share_ratio_activity,
)

# The mixed bucket sums per-row-valued flows: both ratio rows convert at their
# own row's share ratio, the legacy row falls back to its raw tx_amount, and
# the sweep contributes nothing.
_EXPECTED_MIXED_NET_FLOW = (
    FR_MIXED_IN_TX_AMOUNT * FR_MIXED_IN_UNDERLYING_VALUE / FR_MIXED_IN_BALANCE
    - FR_MIXED_OUT_TX_AMOUNT * FR_MIXED_OUT_UNDERLYING_VALUE / FR_MIXED_OUT_BALANCE
    + FR_MIXED_LEGACY_TX_AMOUNT
) * FR_UNDERLYING_PRICE


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the flow share-ratio scenarios and yield the async URL."""
    asyncio.run(seed_flow_share_ratio_activity(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


async def _single_bucket(repo: AllocationRepository, proxy_hex: str):
    """Return the one hourly bucket every seeded scenario lands its rows in."""
    buckets = await repo.list_activity_buckets(
        prime_id=EthAddress(f"0x{proxy_hex}"),
        from_timestamp=FR_BUCKET_TS - dt.timedelta(minutes=30),
        to_timestamp=FR_BUCKET_TS + dt.timedelta(minutes=30),
        bucket_seconds=3600.0,
        limit=10,
    )
    assert len(buckets) == 1
    return buckets[0]


@pytest.mark.asyncio
async def test_deposit_flow_is_valued_at_the_per_row_share_ratio(repo) -> None:
    """A deposit of vault shares is valued at tx_amount x (underlying_value / balance), not 1:1."""
    bucket = await _single_bucket(repo, FR_PROXY_RATIO)
    expected = FR_RATIO_TX_AMOUNT * FR_RATIO_UNDERLYING_VALUE / FR_RATIO_BALANCE * FR_UNDERLYING_PRICE
    assert bucket.net_flow_usd == expected


@pytest.mark.asyncio
async def test_legacy_null_underlying_value_flow_falls_back_to_tx_amount(repo) -> None:
    """A row written before underlying_value existed is valued at the raw tx_amount (position-read parity)."""
    bucket = await _single_bucket(repo, FR_PROXY_LEGACY)
    assert bucket.net_flow_usd == FR_LEGACY_TX_AMOUNT * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_full_exit_zero_balance_flow_falls_back_to_tx_amount(repo) -> None:
    """A full exit leaves balance = 0 on its own row; the undefined ratio falls back to the raw tx_amount."""
    bucket = await _single_bucket(repo, FR_PROXY_FULL_EXIT)
    assert bucket.net_flow_usd == -FR_FULL_EXIT_TX_AMOUNT * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_drained_vault_flow_is_valued_at_zero(repo) -> None:
    """underlying_value = 0 with balance > 0 is a real ratio of 0: the flow is worth nothing."""
    bucket = await _single_bucket(repo, FR_PROXY_DRAINED)
    assert bucket.net_flow_usd == Decimal(0)
    assert bucket.event_count == 1, "the worthless flow must still be counted"


@pytest.mark.asyncio
async def test_divergent_underlying_flow_is_refused_a_valuation(repo) -> None:
    """A row whose own underlying_token_id diverges from the registry's contributes nothing.

    Its underlying_value is denominated in a different asset than the registry
    price multiplies, so pricing its ratio would produce a plausible wrong
    number; the flow read refuses it like every other valuation read (rationale
    on ``_RECEIPT_TOKEN_POSITIONS_SQL``).
    """
    bucket = await _single_bucket(repo, FR_PROXY_DIVERGENT)
    assert bucket.net_flow_usd == Decimal(0)
    assert bucket.event_count == 1, "the refused flow must still be counted"


@pytest.mark.asyncio
async def test_mixed_bucket_sums_ratio_and_fallback_flows(repo) -> None:
    """One bucket mixing ratio in/out, a legacy fallback, and a sweep sums each row at its own valuation."""
    bucket = await _single_bucket(repo, FR_PROXY_MIXED)
    assert bucket.event_count == 4
    assert bucket.net_flow_usd == _EXPECTED_MIXED_NET_FLOW
