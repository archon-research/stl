from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.services.prime_debt_service import PrimeDebtService

_VALID_ADDR = EthAddress("0x" + "ab" * 20)


def _snapshot() -> PrimeDebtSnapshot:
    from datetime import UTC, datetime
    from decimal import Decimal

    return PrimeDebtSnapshot(
        prime_address=str(_VALID_ADDR),
        prime_name="spark",
        ilk_name="ETH-A",
        debt_wad=Decimal("12.5"),
        block_number=123,
        block_version=0,
        synced_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_prime_exists_delegates_to_repository() -> None:
    repo = AsyncMock()
    repo.prime_exists.return_value = True
    service = PrimeDebtService(repo)

    result = await service.prime_exists(_VALID_ADDR)

    assert result is True
    repo.prime_exists.assert_awaited_once_with(_VALID_ADDR)


@pytest.mark.asyncio
async def test_list_debt_snapshots_delegates_with_limit() -> None:
    repo = AsyncMock()
    snap = _snapshot()
    repo.list_debt_snapshots.return_value = [snap]
    service = PrimeDebtService(repo)

    result = await service.list_debt_snapshots(_VALID_ADDR, limit=25)

    assert result == [snap]
    repo.list_debt_snapshots.assert_awaited_once_with(
        _VALID_ADDR,
        from_timestamp=None,
        to_timestamp=None,
        limit=25,
    )


@pytest.mark.asyncio
async def test_list_debt_snapshots_returns_empty_list() -> None:
    repo = AsyncMock()
    repo.list_debt_snapshots.return_value = []
    service = PrimeDebtService(repo)

    result = await service.list_debt_snapshots(_VALID_ADDR)

    assert result == []


@pytest.mark.asyncio
async def test_prime_exists_propagates_repository_error() -> None:
    repo = AsyncMock()
    repo.prime_exists.side_effect = ValueError("db failure")
    service = PrimeDebtService(repo)

    with pytest.raises(ValueError, match="db failure"):
        await service.prime_exists(_VALID_ADDR)
