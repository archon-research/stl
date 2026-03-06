from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)
from app.ports.backed_breakdown_repository import BackedBreakdownRepository
from app.services.backed_breakdown_service import BackedBreakdownService


@pytest.fixture
def mock_repository() -> AsyncMock:
    return AsyncMock(spec=BackedBreakdownRepository)


@pytest.fixture
def service(mock_repository: AsyncMock) -> BackedBreakdownService:
    return BackedBreakdownService(repository=mock_repository)


@pytest.mark.asyncio
async def test_delegates_to_repository(service: BackedBreakdownService, mock_repository: AsyncMock) -> None:
    expected = BackedBreakdown(
        debt_token_id=42,
        protocol_id=1,
        items=(
            CollateralContribution(
                token_id=10,
                symbol="WETH",
                total_backing_usd=Decimal("1000.00"),
                backing_pct=Decimal("60.0000"),
            ),
            CollateralContribution(
                token_id=11,
                symbol="cbBTC",
                total_backing_usd=Decimal("666.67"),
                backing_pct=Decimal("40.0000"),
            ),
        ),
    )
    mock_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, debt_token_id=42)

    assert result is expected
    mock_repository.get_backed_breakdown.assert_awaited_once_with(protocol_id=1, debt_token_id=42)


@pytest.mark.asyncio
async def test_returns_empty_breakdown_when_no_backing(
    service: BackedBreakdownService, mock_repository: AsyncMock
) -> None:
    expected = BackedBreakdown(debt_token_id=99, protocol_id=1, items=())
    mock_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, debt_token_id=99)

    assert result is expected
    assert result.items == ()


@pytest.mark.asyncio
async def test_propagates_repository_exception(service: BackedBreakdownService, mock_repository: AsyncMock) -> None:
    mock_repository.get_backed_breakdown.side_effect = RuntimeError("db down")

    with pytest.raises(RuntimeError, match="db down"):
        await service.get_backed_breakdown(protocol_id=1, debt_token_id=42)
