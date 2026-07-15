"""Integration tests for share-ratio valuation of receipt-token flows.

``list_activity_buckets`` values a receipt-token flow at ``tx_amount x share
ratio x underlying price``, resolving the ratio in three tiers: the row's own
``underlying_value / balance`` when usable (``underlying_value`` present,
``balance > 0``); otherwise the nearest same-token row's ratio (the vault
share ratio is token-global, so any proxy's row pins it; nearest by block
distance, the at-or-before row winning ties, with the usual block_version /
processing_version / log_index tiebreaks); otherwise the raw ``tx_amount``,
which then genuinely means the token has never been valued. A row whose own
``underlying_token_id`` diverges from the registry's is refused (contributes
nothing), and divergent rows are never nearest-ratio candidates, matching
every other valuation read.

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
    FR_ATOKEN_TX_AMOUNT,
    FR_BUCKET_TS,
    FR_DISTANCE_NEAR_DONOR_BALANCE,
    FR_DISTANCE_NEAR_DONOR_UNDERLYING_VALUE,
    FR_DISTANCE_TX_AMOUNT,
    FR_DONOR_DIVERGENT_TX_AMOUNT,
    FR_FULL_EXIT_DONOR_BALANCE,
    FR_FULL_EXIT_DONOR_UNDERLYING_VALUE,
    FR_FULL_EXIT_TX_AMOUNT,
    FR_LEGACY_DONOR_BALANCE,
    FR_LEGACY_DONOR_UNDERLYING_VALUE,
    FR_LEGACY_TX_AMOUNT,
    FR_MIXED_IN_BALANCE,
    FR_MIXED_IN_TX_AMOUNT,
    FR_MIXED_IN_UNDERLYING_VALUE,
    FR_MIXED_LEGACY_TX_AMOUNT,
    FR_MIXED_OUT_BALANCE,
    FR_MIXED_OUT_TX_AMOUNT,
    FR_MIXED_OUT_UNDERLYING_VALUE,
    FR_NEVER_VALUED_TX_AMOUNT,
    FR_PROXY_ATOKEN,
    FR_PROXY_DISTANCE,
    FR_PROXY_DIVERGENT,
    FR_PROXY_DONOR_DIVERGENT,
    FR_PROXY_DRAINED,
    FR_PROXY_FULL_EXIT,
    FR_PROXY_LEGACY,
    FR_PROXY_MIXED,
    FR_PROXY_NEVER_VALUED,
    FR_PROXY_RATIO,
    FR_PROXY_TIE,
    FR_RATIO_BALANCE,
    FR_RATIO_TX_AMOUNT,
    FR_RATIO_UNDERLYING_VALUE,
    FR_TIE_BEFORE_DONOR_BALANCE,
    FR_TIE_BEFORE_DONOR_UNDERLYING_VALUE,
    FR_TIE_TX_AMOUNT,
    FR_UNDERLYING_PRICE,
    seed_flow_share_ratio_activity,
)

# The mixed bucket sums per-row-valued flows: both own-ratio rows convert at
# their own row's share ratio, the legacy row borrows the out row's ratio
# (its nearest at-or-before same-token row), and the sweep contributes
# nothing.
_EXPECTED_MIXED_NET_FLOW = (
    FR_MIXED_IN_TX_AMOUNT * FR_MIXED_IN_UNDERLYING_VALUE / FR_MIXED_IN_BALANCE
    - FR_MIXED_OUT_TX_AMOUNT * FR_MIXED_OUT_UNDERLYING_VALUE / FR_MIXED_OUT_BALANCE
    + FR_MIXED_LEGACY_TX_AMOUNT * FR_MIXED_OUT_UNDERLYING_VALUE / FR_MIXED_OUT_BALANCE
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
async def test_legacy_null_underlying_value_flow_uses_the_nearest_row_ratio(repo) -> None:
    """A row written before underlying_value existed borrows the nearest same-token row's ratio.

    The donor row belongs to a different proxy: the share ratio is a property
    of the token, so the lookup must cross positions.
    """
    bucket = await _single_bucket(repo, FR_PROXY_LEGACY)
    ratio = FR_LEGACY_DONOR_UNDERLYING_VALUE / FR_LEGACY_DONOR_BALANCE
    assert bucket.net_flow_usd == FR_LEGACY_TX_AMOUNT * ratio * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_full_exit_zero_balance_flow_uses_the_nearest_row_ratio(repo) -> None:
    """A full exit (balance = 0 on its own row) is valued at the nearest valued row's ratio.

    Mirrors the warehouse syrupUSDC exit at block 24869871: the token's only
    valued row sits AFTER the exit at ratio 1.172241, so the exit must not
    collapse to the raw 1:1 fallback (a ~17% discontinuity in the series).
    """
    bucket = await _single_bucket(repo, FR_PROXY_FULL_EXIT)
    ratio = FR_FULL_EXIT_DONOR_UNDERLYING_VALUE / FR_FULL_EXIT_DONOR_BALANCE
    assert ratio == Decimal("1.172241")
    assert bucket.net_flow_usd == -FR_FULL_EXIT_TX_AMOUNT * ratio * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_never_valued_token_falls_back_to_the_raw_tx_amount(repo) -> None:
    """With no usable same-token row anywhere, the flow is valued at the raw tx_amount.

    The token's only other row is 0/0 (syrupUSDT's shape): a present
    underlying_value with balance = 0 is not a usable ratio and must not be
    borrowed.
    """
    bucket = await _single_bucket(repo, FR_PROXY_NEVER_VALUED)
    assert bucket.event_count == 2, "the 0/0 sweep is counted, not valued"
    assert bucket.net_flow_usd == FR_NEVER_VALUED_TX_AMOUNT * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_one_to_one_atoken_legacy_flow_is_unchanged_by_the_nearest_row_lookup(repo) -> None:
    """An aToken's usable rows carry ratio exactly 1, so the borrowed ratio equals the raw fallback."""
    bucket = await _single_bucket(repo, FR_PROXY_ATOKEN)
    assert bucket.net_flow_usd == FR_ATOKEN_TX_AMOUNT * FR_UNDERLYING_PRICE


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
async def test_divergent_rows_are_not_nearest_ratio_candidates(repo) -> None:
    """A legacy flow whose only valued same-token row is divergent falls back raw.

    The divergent row's underlying_value is denominated in a different asset,
    so borrowing its ratio would mix units; it is excluded from the candidate
    set just as it is refused as an own-row ratio.
    """
    bucket = await _single_bucket(repo, FR_PROXY_DONOR_DIVERGENT)
    assert bucket.net_flow_usd == FR_DONOR_DIVERGENT_TX_AMOUNT * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_nearest_ratio_is_chosen_by_block_distance_across_sides(repo) -> None:
    """A valued row one block after beats one two blocks before: distance decides, not side."""
    bucket = await _single_bucket(repo, FR_PROXY_DISTANCE)
    ratio = FR_DISTANCE_NEAR_DONOR_UNDERLYING_VALUE / FR_DISTANCE_NEAR_DONOR_BALANCE
    assert bucket.net_flow_usd == FR_DISTANCE_TX_AMOUNT * ratio * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_equidistant_ratio_candidates_prefer_the_at_or_before_row(repo) -> None:
    """With valued rows one block before and one block after, the at-or-before ratio wins."""
    bucket = await _single_bucket(repo, FR_PROXY_TIE)
    ratio = FR_TIE_BEFORE_DONOR_UNDERLYING_VALUE / FR_TIE_BEFORE_DONOR_BALANCE
    assert bucket.net_flow_usd == FR_TIE_TX_AMOUNT * ratio * FR_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_mixed_bucket_sums_ratio_and_nearest_row_flows(repo) -> None:
    """One bucket mixing own-ratio in/out, a nearest-ratio legacy row, and a sweep sums per-row valuations."""
    bucket = await _single_bucket(repo, FR_PROXY_MIXED)
    assert bucket.event_count == 4
    assert bucket.net_flow_usd == _EXPECTED_MIXED_NET_FLOW
