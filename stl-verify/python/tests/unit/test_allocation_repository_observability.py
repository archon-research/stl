import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import utc_now
from app.domain.entities.allocation import EthAddress
from app.domain.entities.time_series_bucket import TotalCapitalBucket
from tests.factories import ANCHORAGE_FROZEN_AS_OF, make_anchorage_custody_holding, make_direct_asset_holding

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


def test_record_stale_custody_logs_warning_with_alert_fields():
    """The alertable signal is the warning payload (prime_id / stale_count /
    oldest_snapshot_time), not just the span attribute — assert it directly so
    dropping the warning cannot ship green. The module logger is patched because
    the app installs a non-propagating JSON handler that caplog cannot see.
    """
    stale = make_anchorage_custody_holding()  # frozen 2026-06-16 default

    span = MagicMock()
    with (
        patch(
            "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
            return_value=span,
        ),
        patch("app.adapters.postgres.allocation_position_repository.logger") as mock_logger,
    ):
        AllocationRepository._record_stale_custody(_PRIME, [stale])

    mock_logger.warning.assert_called_once()
    message, kwargs = mock_logger.warning.call_args.args[0], mock_logger.warning.call_args.kwargs
    assert "stale" in message.lower()
    extra = kwargs["extra"]
    assert extra["prime_id"] == str(_PRIME)
    assert extra["stale_count"] == 1
    assert extra["oldest_snapshot_time"] == ANCHORAGE_FROZEN_AS_OF.isoformat()


def test_record_stale_custody_fires_just_over_one_hour():
    """Boundary: a snapshot 61 minutes old is stale (past the 1h threshold)."""
    holding = make_anchorage_custody_holding(as_of=datetime.now(UTC) - timedelta(minutes=61))

    span = MagicMock()
    with (
        patch(
            "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
            return_value=span,
        ),
        patch("app.adapters.postgres.allocation_position_repository.logger") as mock_logger,
    ):
        AllocationRepository._record_stale_custody(_PRIME, [holding])

    span.set_attribute.assert_called_once()
    mock_logger.warning.assert_called_once()


def test_record_stale_custody_silent_just_under_one_hour():
    """Boundary: a snapshot 59 minutes old is fresh (within the 1h threshold)."""
    holding = make_anchorage_custody_holding(as_of=datetime.now(UTC) - timedelta(minutes=59))

    span = MagicMock()
    with (
        patch(
            "app.adapters.postgres.allocation_position_repository.trace.get_current_span",
            return_value=span,
        ),
        patch("app.adapters.postgres.allocation_position_repository.logger") as mock_logger,
    ):
        AllocationRepository._record_stale_custody(_PRIME, [holding])

    span.set_attribute.assert_not_called()
    mock_logger.warning.assert_not_called()


class _FailingEngine:
    """An engine whose every connection attempt fails, as an outage does."""

    @asynccontextmanager
    async def connect(self):
        raise RuntimeError("connection refused")
        yield  # pragma: no cover - unreachable, but required to make this a generator


async def test_a_failed_activity_query_logs_the_allow_list_LENGTH_never_its_contents():
    """``allowed_vaults`` is the caller's entire authorization set and runs to
    the OpenFGA ListObjects ceiling. Logging the bind params verbatim puts all
    of it on one ~45KB line, which deps.py explicitly refuses to do.
    """
    vaults = [EthAddress("0x" + f"{value:02x}" * 20) for value in range(1, 51)]
    repo = AllocationRepository(cast(AsyncEngine, _FailingEngine()), utc_now)

    with (
        patch("app.adapters.postgres.allocation_position_repository.logger") as mock_logger,
        pytest.raises(ValueError),
    ):
        await repo.list_allocation_activity(allowed_vaults=vaults, limit=10)

    extra = mock_logger.error.call_args.kwargs["extra"]
    assert extra["params"]["allowed_vaults"] == "[50 values]"
    emitted = json.dumps(extra, default=str)
    assert all(vault.hex not in emitted for vault in vaults)


async def test_an_unfiltered_activity_query_still_logs_a_readable_none():
    """Auth off is None, not an empty list, and the log has to keep saying so."""
    repo = AllocationRepository(cast(AsyncEngine, _FailingEngine()), utc_now)

    with (
        patch("app.adapters.postgres.allocation_position_repository.logger") as mock_logger,
        pytest.raises(ValueError),
    ):
        await repo.list_allocation_activity(allowed_vaults=None, limit=10)

    assert mock_logger.error.call_args.kwargs["extra"]["params"]["allowed_vaults"] is None
