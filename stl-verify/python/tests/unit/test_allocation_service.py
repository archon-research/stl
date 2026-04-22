from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress, Prime
from app.services.allocation_service import AllocationService
from tests.conftest import make_allocation_position

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
async def test_list_allocations_by_prime_delegates_to_repository():
    repo = AsyncMock()
    position = make_allocation_position()
    repo.list_allocations_by_prime.return_value = [position]
    service = AllocationService(repo)

    result = await service.list_allocations_by_prime(_VALID_ADDR)

    assert result == [position]
    repo.list_allocations_by_prime.assert_awaited_once_with(_VALID_ADDR, None)


@pytest.mark.asyncio
async def test_list_allocations_by_prime_with_block_number_passes_it_to_repository():
    repo = AsyncMock()
    position = make_allocation_position(block_number=1000)
    repo.list_allocations_by_prime.return_value = [position]
    service = AllocationService(repo)

    result = await service.list_allocations_by_prime(_VALID_ADDR, block_number=1000)

    assert result == [position]
    repo.list_allocations_by_prime.assert_awaited_once_with(_VALID_ADDR, 1000)


@pytest.mark.asyncio
async def test_list_allocations_by_prime_returns_empty_for_unknown_prime():
    repo = AsyncMock()
    repo.list_allocations_by_prime.return_value = []
    service = AllocationService(repo)

    unknown_addr = EthAddress("0x" + "de" * 20)
    result = await service.list_allocations_by_prime(unknown_addr)

    assert result == []
