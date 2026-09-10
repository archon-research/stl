"""Integration tests for ``series="balance"`` on ``list_activity_buckets`` (VEC-760).

``series="balance"`` reads each bucket's recorded position value instead of
summing the flows into it. The behaviours worth pinning are the ones that make
it a *different* read rather than a cheaper one:

* it values a receipt position as ``COALESCE(underlying_value, balance)`` times
  the registry underlying's price, and a direct holding by the token's own;
* it carries a value forward into buckets with no observation, including from a
  row that sits before the window entirely;
* it collapses to the newest ``processing_version`` first, because a correction
  copies its original's ``created_at`` and ``last()`` therefore cannot break the
  tie (VEC-758);
* it refuses a row whose own underlying disagrees with the registry's, matching
  every other valuation read;
* it leaves ``net_flow_usd`` alone, and ``series="flow"`` leaves
  ``balance_usd`` unset -- one query runs per call, not both.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_balance_series_positions``.
"""

import datetime as dt
from decimal import Decimal
from typing import Literal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import utc_now
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    BS_CARRY_UNDERLYING_VALUE,
    BS_CORRECTED_FIXED_UNDERLYING_VALUE,
    BS_CORRECTED_ORIGINAL_UNDERLYING_VALUE,
    BS_DIRECT_BALANCE,
    BS_DIRECT_PRICE,
    BS_PROXY_CARRY,
    BS_PROXY_CORRECTED,
    BS_PROXY_DIRECT,
    BS_PROXY_DIVERGENT,
    BS_PROXY_SEEDED,
    BS_SEEDED_UNDERLYING_VALUE,
    BS_UNDERLYING_PRICE,
    seed_balance_series_positions,
)

pytestmark = pytest.mark.asyncio(loop_scope="module")

_DAY = 86400.0


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repo(async_db_url: str, db_url: str):
    await seed_balance_series_positions(db_url)
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine, utc_now)
    finally:
        await engine.dispose()


async def _buckets(
    repo: AllocationRepository,
    proxy_hex: str,
    *,
    series: Literal["flow", "balance"] = "balance",
    days: int = 10,
):
    now = dt.datetime.now(dt.UTC)
    return await repo.list_activity_buckets(
        proxy_addresses=[EthAddress("0x" + proxy_hex)],
        from_timestamp=now - dt.timedelta(days=days),
        to_timestamp=now,
        bucket_seconds=_DAY,
        limit=500,
        series=series,
    )


async def test_receipt_position_valued_at_underlying_times_price(repo: AllocationRepository) -> None:
    buckets = await _buckets(repo, BS_PROXY_CARRY)
    assert buckets, "expected buckets for the carry-forward proxy"
    expected = BS_CARRY_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    # Every bucket from the observation onward carries the same value: one
    # observation, then LOCF. The newest bucket is first.
    assert buckets[0].balance_usd == expected


async def test_value_carries_forward_into_buckets_with_no_observation(repo: AllocationRepository) -> None:
    buckets = await _buckets(repo, BS_PROXY_CARRY)
    expected = BS_CARRY_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    # The single row is 3 days old, so the two newer daily buckets hold no
    # observation of their own and must still report the value.
    carried = [b for b in buckets if b.balance_usd == expected]
    assert len(carried) >= 3, f"expected the value carried into later buckets, got {[b.balance_usd for b in buckets]}"


async def test_a_row_before_the_window_seeds_the_first_bucket(repo: AllocationRepository) -> None:
    # This proxy's only row predates the window, so every in-window bucket
    # depends on the carry-in seed rather than on an observation.
    buckets = await _buckets(repo, BS_PROXY_SEEDED)
    expected = BS_SEEDED_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    assert buckets, "expected buckets seeded from the pre-window row"
    assert all(b.balance_usd == expected for b in buckets), [b.balance_usd for b in buckets]


async def test_correction_supersedes_its_original_rather_than_adding_to_it(repo: AllocationRepository) -> None:
    buckets = await _buckets(repo, BS_PROXY_CORRECTED)
    corrected = BS_CORRECTED_FIXED_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    original = BS_CORRECTED_ORIGINAL_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    assert buckets[0].balance_usd == corrected
    # The two failure modes this guards: summing both versions, and last()
    # breaking the created_at tie the wrong way.
    assert buckets[0].balance_usd != original + corrected
    assert buckets[0].balance_usd != original


async def test_divergent_underlying_is_refused(repo: AllocationRepository) -> None:
    buckets = await _buckets(repo, BS_PROXY_DIVERGENT)
    # Refused rows contribute nothing, so there is no value to report at all.
    assert all(b.balance_usd in (None, Decimal(0)) for b in buckets), [b.balance_usd for b in buckets]


async def test_direct_holding_priced_by_its_own_token_price(repo: AllocationRepository) -> None:
    buckets = await _buckets(repo, BS_PROXY_DIRECT)
    assert buckets[0].balance_usd == BS_DIRECT_BALANCE * BS_DIRECT_PRICE


async def test_flow_series_leaves_balance_unset_and_balance_series_leaves_flow_alone(
    repo: AllocationRepository,
) -> None:
    flow = await _buckets(repo, BS_PROXY_CARRY, series="flow")
    assert all(b.balance_usd is None for b in flow), "flow series must not claim a balance"

    balance = await _buckets(repo, BS_PROXY_CARRY, series="balance")
    assert any(b.balance_usd is not None for b in balance)
    # Only one query runs, so the flow columns are left at their zero value
    # rather than being computed alongside.
    assert all(b.net_flow_usd == 0 and b.event_count == 0 for b in balance)


async def test_buckets_before_the_first_observation_are_null_not_zero(repo: AllocationRepository) -> None:
    # The seeded row is 3 days old inside a 10-day window, so the leading
    # buckets have no known value. They must come back NULL: $0 is
    # indistinguishable from a position that really emptied, which is the
    # silent-zero failure VEC-537 is about, and the chart drops a null bucket
    # rather than drawing a false floor.
    buckets = await _buckets(repo, BS_PROXY_CARRY)
    oldest_first = sorted(buckets, key=lambda b: b.bucket_start)
    assert oldest_first[0].balance_usd is None, "a bucket before any observation must not report a figure"
    assert oldest_first[-1].balance_usd == BS_CARRY_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
