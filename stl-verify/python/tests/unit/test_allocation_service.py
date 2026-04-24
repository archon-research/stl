from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress, Prime
from app.services.allocation_service import AllocationService
from tests.conftest import make_receipt_token_position

_VALID_ADDR = EthAddress("0x" + "ab" * 20)


@pytest.mark.asyncio
async def test_list_primes_returns_all_primes():
    repo = AsyncMock()
    repo.list_primes.return_value = [
        Prime(id="0xaaa", name="grove", address="0xaaa"),
        Prime(id="0xbbb", name="spark", address="0xbbb"),
    ]
    service = AllocationService(repo)

    result = await service.list_primes()

    assert result == [
        Prime(id="0xaaa", name="grove", address="0xaaa"),
        Prime(id="0xbbb", name="spark", address="0xbbb"),
    ]
    repo.list_primes.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_receipt_token_positions_delegates_to_repository():
    repo = AsyncMock()
    position = make_receipt_token_position()
    repo.list_receipt_token_positions.return_value = [position]
    service = AllocationService(repo)

    result = await service.list_receipt_token_positions(_VALID_ADDR)

    assert result == [position]
    repo.list_receipt_token_positions.assert_awaited_once_with(_VALID_ADDR)


@pytest.mark.asyncio
async def test_list_receipt_token_positions_returns_empty_for_unknown_prime():
    repo = AsyncMock()
    repo.list_receipt_token_positions.return_value = []
    service = AllocationService(repo)

    unknown_addr = EthAddress("0x" + "de" * 20)
    result = await service.list_receipt_token_positions(unknown_addr)

    assert result == []
