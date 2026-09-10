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
  every other valuation read, and that refusal poisons the bucket rather than
  silently omitting the entity from the total (VEC-537's failure class);
* ``protocol_name`` filters it, the same as every other filter on this read;
* it leaves ``net_flow_usd``/``event_count``/``total_tx_amount`` at ``None``,
  and ``series="flow"`` leaves ``balance_usd`` unset -- one query runs per
  call, not both.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_balance_series_positions``.
"""

import datetime as dt
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
    BS_PROXY_MIXED,
    BS_PROXY_SEEDED,
    BS_PROXY_SUMMED,
    BS_SEEDED_UNDERLYING_VALUE,
    BS_SUMMED_DIRECT_BALANCE,
    BS_SUMMED_UNDERLYING_VALUE,
    BS_UNDERLYING_PRICE,
    BS_VAULT_HEX,
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
    protocol_name: str | None = None,
    allowed_vaults: list[EthAddress] | None = None,
):
    now = dt.datetime.now(dt.UTC)
    return await repo.list_activity_buckets(
        proxy_addresses=[EthAddress("0x" + proxy_hex)],
        from_timestamp=now - dt.timedelta(days=days),
        to_timestamp=now,
        bucket_seconds=_DAY,
        limit=500,
        series=series,
        protocol_name=protocol_name,
        allowed_vaults=allowed_vaults,
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
    # A refused row is PRESENT, not absent: it still produces a full range of
    # buckets (VEC-760 B5), every one None, rather than vanishing outright and
    # leaving an empty result set that would pass this assertion vacuously.
    assert buckets, "a refused row must still produce buckets, not vanish from the result entirely"
    assert all(b.balance_usd is None for b in buckets), [b.balance_usd for b in buckets]


async def test_unpriceable_entity_poisons_the_total_instead_of_being_dropped(repo: AllocationRepository) -> None:
    # BS_PROXY_MIXED holds a priced direct holding AND a divergent (unpriceable)
    # receipt row. A plain SUM would silently skip the unpriceable entity and
    # report just the direct holding's value -- a confident but wrong total,
    # the exact VEC-537 failure class B5 closes. The whole bucket must go None.
    buckets = await _buckets(repo, BS_PROXY_MIXED)
    assert buckets, "expected buckets for the mixed proxy"
    assert all(b.balance_usd is None for b in buckets), [b.balance_usd for b in buckets]


async def test_multiple_entities_under_one_proxy_are_summed(repo: AllocationRepository) -> None:
    # BS_PROXY_SUMMED holds two different cleanly-priced entities; the total
    # must be their sum, not just whichever one a wrong query happened to pick.
    buckets = await _buckets(repo, BS_PROXY_SUMMED)
    expected = BS_SUMMED_UNDERLYING_VALUE * BS_UNDERLYING_PRICE + BS_SUMMED_DIRECT_BALANCE * BS_DIRECT_PRICE
    assert buckets[0].balance_usd == expected


async def test_direct_holding_priced_by_its_own_token_price(repo: AllocationRepository) -> None:
    buckets = await _buckets(repo, BS_PROXY_DIRECT)
    assert buckets[0].balance_usd == BS_DIRECT_BALANCE * BS_DIRECT_PRICE


async def test_protocol_name_filters_the_balance_series(repo: AllocationRepository) -> None:
    # VEC-760 I2: protocol_name was silently ignored on this query -- present
    # in _ALLOCATION_ACTIVITY_BUCKETS_SQL but absent from the balance one.
    expected = BS_CARRY_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    matched = await _buckets(repo, BS_PROXY_CARRY, protocol_name="bsLike")
    assert matched[0].balance_usd == expected

    unmatched = await _buckets(repo, BS_PROXY_CARRY, protocol_name="not-a-real-protocol")
    assert all(b.balance_usd is None for b in unmatched), [b.balance_usd for b in unmatched]


async def test_allowed_vaults_cross_tenant_filter_applies_to_the_balance_series(
    repo: AllocationRepository,
) -> None:
    """The balance query's ``allowed_vaults`` predicate, exercised non-NULL.

    Asserting on ``event_count`` (as the general authz suite does) would pass
    vacuously here -- balance mode leaves it ``None`` -- so this checks
    ``balance_usd`` instead.
    """
    expected = BS_CARRY_UNDERLYING_VALUE * BS_UNDERLYING_PRICE
    own_vault = await _buckets(repo, BS_PROXY_CARRY, allowed_vaults=[EthAddress("0x" + BS_VAULT_HEX)])
    assert own_vault[0].balance_usd == expected

    other_vault = await _buckets(repo, BS_PROXY_CARRY, allowed_vaults=[EthAddress("0x" + "ab" * 20)])
    assert all(b.balance_usd is None for b in other_vault), [b.balance_usd for b in other_vault]


async def test_flow_series_leaves_balance_unset_and_balance_series_leaves_flow_alone(
    repo: AllocationRepository,
) -> None:
    flow = await _buckets(repo, BS_PROXY_CARRY, series="flow")
    assert all(b.balance_usd is None for b in flow), "flow series must not claim a balance"

    balance = await _buckets(repo, BS_PROXY_CARRY, series="balance")
    assert any(b.balance_usd is not None for b in balance)
    # Only one query runs, so the flow columns are left None rather than being
    # computed alongside -- not a misleading zero (VEC-760).
    assert all(b.net_flow_usd is None and b.event_count is None and b.total_tx_amount is None for b in balance)


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
