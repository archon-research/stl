from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.services.capital_metrics_history_service import CapitalMetricsHistoryService

_VALID_ADDR = EthAddress("0x" + "ab" * 20)


@pytest.mark.asyncio
async def test_prime_exists_delegates_to_repository() -> None:
    repo = AsyncMock()
    repo.prime_exists.return_value = True
    service = CapitalMetricsHistoryService(repo)

    result = await service.prime_exists(_VALID_ADDR)

    assert result is True
    repo.prime_exists.assert_awaited_once_with(_VALID_ADDR)


@pytest.mark.asyncio
async def test_list_snapshots_delegates_with_kwargs() -> None:
    repo = AsyncMock()
    repo.list_snapshots.return_value = []
    service = CapitalMetricsHistoryService(repo)

    from_ts = datetime(2026, 1, 1, tzinfo=UTC)
    to_ts = datetime(2026, 1, 2, tzinfo=UTC)
    await service.list_snapshots(_VALID_ADDR, from_timestamp=from_ts, to_timestamp=to_ts, limit=25)

    repo.list_snapshots.assert_awaited_once_with(
        _VALID_ADDR, from_timestamp=from_ts, to_timestamp=to_ts, limit=25
    )


@pytest.mark.asyncio
async def test_list_buckets_delegates_with_kwargs() -> None:
    repo = AsyncMock()
    repo.list_buckets.return_value = []
    service = CapitalMetricsHistoryService(repo)

    from_ts = datetime(2026, 1, 1, tzinfo=UTC)
    to_ts = datetime(2026, 1, 2, tzinfo=UTC)
    await service.list_buckets(
        _VALID_ADDR, from_timestamp=from_ts, to_timestamp=to_ts, bucket_seconds=300.0, limit=25
    )

    repo.list_buckets.assert_awaited_once_with(
        _VALID_ADDR, from_timestamp=from_ts, to_timestamp=to_ts, bucket_seconds=300.0, limit=25
    )
