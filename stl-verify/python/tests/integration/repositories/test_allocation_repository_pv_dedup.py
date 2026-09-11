"""Integration tests for processing_version dedup on allocation_position reads (VEC-758).

allocation_position is append-only: a correction is a new row differing from the
one it corrects in processing_version alone, and (for the LOCF reads) sharing its
original's created_at exactly. ``list_allocation_activity``, ``list_activity_buckets``,
``list_exposure_buckets`` and ``list_total_capital_buckets`` all collapse to the
newest processing_version per identity before reading; each test here fails if that
dedup is removed (the correction shows up as an extra row / summed twice) or sorted
the wrong way (the original wins instead of the correction).

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_processing_version_dedup_scenarios``.
"""

import asyncio
import datetime as dt

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import utc_now
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    PVD_ALM_PROXY_HEX,
    PVD_CORRECTED_AMOUNT,
    PVD_CREATED_AT,
    PVD_ORIGINAL_AMOUNT,
    PVD_TOTAL_CAPITAL_CORRECTED,
    PVD_TOTAL_CAPITAL_CREATED_AT,
    PVD_TOTAL_CAPITAL_ORIGINAL,
    PVD_UNDERLYING_PRICE,
    seed_processing_version_dedup_scenarios,
)


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the processing_version dedup scenarios and yield the async URL."""
    asyncio.run(seed_processing_version_dedup_scenarios(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine, utc_now)
    finally:
        await engine.dispose()


async def test_activity_feed_shows_the_correction_only(repo: AllocationRepository) -> None:
    """A reprocessed row must surface once in the feed, at its corrected values --
    not as a duplicate of the identity it corrects."""
    rows = await repo.list_allocation_activity(proxy_addresses=[EthAddress(f"0x{PVD_ALM_PROXY_HEX}")], limit=10)

    assert len(rows) == 1, f"expected the correction to replace its original, got {len(rows)} rows"
    assert rows[0].balance == PVD_CORRECTED_AMOUNT
    assert rows[0].tx_amount == PVD_CORRECTED_AMOUNT


async def test_activity_bucket_sums_the_correction_only(repo: AllocationRepository) -> None:
    """A reprocessed row must count once and sum at its corrected amount -- an
    un-deduped correction would double the event count and the tx-amount sum."""
    buckets = await repo.list_activity_buckets(
        proxy_addresses=[EthAddress(f"0x{PVD_ALM_PROXY_HEX}")],
        from_timestamp=PVD_CREATED_AT - dt.timedelta(hours=1),
        to_timestamp=PVD_CREATED_AT + dt.timedelta(hours=1),
        bucket_seconds=3600.0,
        limit=10,
    )

    assert len(buckets) == 1
    assert buckets[0].event_count == 1, "an un-deduped correction would double-count the event"
    assert buckets[0].total_tx_amount == PVD_CORRECTED_AMOUNT
    assert buckets[0].total_tx_amount != PVD_ORIGINAL_AMOUNT + PVD_CORRECTED_AMOUNT
    # own-row share ratio is 1 (underlying_value == balance), so net_flow_usd is
    # the corrected amount at the underlying's price -- a live check that the
    # dedup happens before valuation, not after.
    assert buckets[0].net_flow_usd == PVD_CORRECTED_AMOUNT * PVD_UNDERLYING_PRICE


async def test_exposure_bucket_locf_resolves_the_created_at_tie_to_the_correction(repo: AllocationRepository) -> None:
    """A correction copies its original's created_at exactly, so last() has no
    tie to break on its own; the dedup must happen before last() sees the rows."""
    buckets = await repo.list_exposure_buckets(
        [EthAddress(f"0x{PVD_ALM_PROXY_HEX}")],
        from_timestamp=PVD_CREATED_AT - dt.timedelta(hours=1),
        to_timestamp=PVD_CREATED_AT + dt.timedelta(hours=1),
        bucket_seconds=3600.0,
        limit=10,
    )

    assert buckets, "expected a bucket for the corrected position"
    corrected_exposure = PVD_CORRECTED_AMOUNT * PVD_UNDERLYING_PRICE
    original_exposure = PVD_ORIGINAL_AMOUNT * PVD_UNDERLYING_PRICE
    assert buckets[0].exposure_usd == corrected_exposure
    assert buckets[0].exposure_usd != original_exposure


async def test_total_capital_bucket_locf_resolves_the_created_at_tie_to_the_correction(
    repo: AllocationRepository,
) -> None:
    """Same created_at-tie hazard as the exposure buckets, on the SubProxy
    treasury USDS read."""
    buckets = await repo.list_total_capital_buckets(
        EthAddress(f"0x{PVD_ALM_PROXY_HEX}"),
        from_timestamp=PVD_TOTAL_CAPITAL_CREATED_AT - dt.timedelta(hours=1),
        to_timestamp=PVD_TOTAL_CAPITAL_CREATED_AT + dt.timedelta(hours=1),
        bucket_seconds=3600.0,
        limit=10,
    )

    assert buckets, "expected a bucket for the corrected treasury balance"
    assert buckets[0].total_capital_usd == PVD_TOTAL_CAPITAL_CORRECTED
    assert buckets[0].total_capital_usd != PVD_TOTAL_CAPITAL_ORIGINAL
