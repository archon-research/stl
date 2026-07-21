from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress
from app.domain.entities.time_series_bucket import TotalCapitalBucket
from tests.factories import make_anchorage_custody_holding, make_direct_asset_holding

_PRIME = EthAddress("0x" + "ab" * 20)


def _bucket(value: Decimal | None) -> TotalCapitalBucket:
    return TotalCapitalBucket(bucket_start=datetime(2026, 1, 1, tzinfo=UTC), total_capital_usd=value)


def test_record_unpriced_holdings_sets_span_attribute_for_unpriced():
    priced = make_direct_asset_holding(symbol="USDT", amount_usd=Decimal("100"))
    unpriced_a = make_direct_asset_holding(symbol="syrupUSDC", token_id=1, amount_usd=None)
    unpriced_b = make_direct_asset_holding(symbol="PYUSDUSDS", token_id=2, amount_usd=None)

    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_unpriced_holdings(_PRIME, [priced, unpriced_a, unpriced_b])

    span.set_attribute.assert_called_once_with("allocations.direct_holdings.unpriced", 2)


def test_record_unpriced_holdings_noop_when_all_priced():
    priced = make_direct_asset_holding(symbol="USDT", amount_usd=Decimal("100"))

    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_unpriced_holdings(_PRIME, [priced])

    span.set_attribute.assert_not_called()


def test_record_empty_total_capital_sets_span_attribute_when_all_null():
    buckets = [_bucket(None), _bucket(None)]

    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_empty_total_capital(_PRIME, buckets)

    span.set_attribute.assert_called_once_with("allocations.total_capital.all_null", True)


def test_record_empty_total_capital_noop_when_any_observed():
    buckets = [_bucket(None), _bucket(Decimal("1000"))]

    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_empty_total_capital(_PRIME, buckets)

    span.set_attribute.assert_not_called()


def test_record_empty_total_capital_noop_when_no_buckets():
    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_empty_total_capital(_PRIME, [])

    span.set_attribute.assert_not_called()


def test_record_stale_custody_sets_span_attribute_when_snapshot_is_stale():
    # The factory default as_of is the frozen 2026-06-16 snapshot, far past the 1h threshold.
    stale = make_anchorage_custody_holding()

    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_stale_custody(_PRIME, [stale])

    span.set_attribute.assert_called_once()
    name, value = span.set_attribute.call_args.args
    assert name == "allocations.anchorage_custody.stale_seconds"
    assert value > 0


def test_record_stale_custody_noop_when_snapshot_is_fresh():
    fresh = make_anchorage_custody_holding(as_of=datetime.now(UTC) - timedelta(minutes=5))

    span = MagicMock()
    with patch(
        "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
        return_value=span,
    ):
        AllocationRepository._record_stale_custody(_PRIME, [fresh])

    span.set_attribute.assert_not_called()
