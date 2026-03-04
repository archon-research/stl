from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import Star
from app.services.allocation_service import AllocationService
from tests.conftest import make_allocation_position


@pytest.mark.asyncio
async def test_list_stars_returns_all_stars():
    repo = AsyncMock()
    repo.list_stars.return_value = [Star("grove"), Star("spark")]
    service = AllocationService(repo)

    result = await service.list_stars()

    assert result == [Star("grove"), Star("spark")]
    repo.list_stars.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_allocations_by_star_delegates_to_repository():
    repo = AsyncMock()
    position = make_allocation_position()
    repo.list_allocations_by_star.return_value = [position]
    service = AllocationService(repo)

    result = await service.list_allocations_by_star("spark")

    assert result == [position]
    repo.list_allocations_by_star.assert_awaited_once_with("spark")


@pytest.mark.asyncio
async def test_list_allocations_by_star_returns_empty_for_unknown_star():
    repo = AsyncMock()
    repo.list_allocations_by_star.return_value = []
    service = AllocationService(repo)

    result = await service.list_allocations_by_star("unknown")

    assert result == []
