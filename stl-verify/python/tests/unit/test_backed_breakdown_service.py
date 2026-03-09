from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)
from app.ports.backed_breakdown_repository import BackedBreakdownRepository
from app.ports.morpho_repository import MorphoRepository
from app.services.backed_breakdown_service import BackedBreakdownService


@pytest.fixture
def mock_repository() -> AsyncMock:
    return AsyncMock(spec=BackedBreakdownRepository)


@pytest.fixture
def mock_morpho_repository() -> AsyncMock:
    return AsyncMock(spec=BackedBreakdownRepository)


@pytest.fixture
def mock_morpho() -> AsyncMock:
    mock = AsyncMock(spec=MorphoRepository)
    mock.is_morpho_vault.return_value = False
    return mock


@pytest.fixture
def service(
    mock_repository: AsyncMock, mock_morpho_repository: AsyncMock, mock_morpho: AsyncMock
) -> BackedBreakdownService:
    return BackedBreakdownService(
        repository=mock_repository,
        morpho_repository=mock_morpho_repository,
        morpho=mock_morpho,
    )


# ---------------------------------------------------------------------------
# Default (SparkLend) path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delegates_to_default_repository(
    service: BackedBreakdownService,
    mock_repository: AsyncMock,
    mock_morpho_repository: AsyncMock,
    mock_morpho: AsyncMock,
) -> None:
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
    mock_morpho.is_morpho_vault.return_value = False
    mock_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, debt_token_id=42)

    assert result is expected
    mock_morpho.is_morpho_vault.assert_awaited_once_with(42)
    mock_repository.get_backed_breakdown.assert_awaited_once_with(protocol_id=1, debt_token_id=42)
    mock_morpho_repository.get_backed_breakdown.assert_not_awaited()


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


# ---------------------------------------------------------------------------
# Morpho path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_routes_to_morpho_when_vault_exists(
    service: BackedBreakdownService,
    mock_repository: AsyncMock,
    mock_morpho_repository: AsyncMock,
    mock_morpho: AsyncMock,
) -> None:
    expected = BackedBreakdown(
        debt_token_id=7,
        protocol_id=1,
        items=(
            CollateralContribution(
                token_id=20,
                symbol="USDC",
                total_backing_usd=Decimal("530000.00"),
                backing_pct=Decimal("53.00"),
            ),
            CollateralContribution(
                token_id=21,
                symbol="WETH",
                total_backing_usd=Decimal("320000.00"),
                backing_pct=Decimal("32.00"),
            ),
        ),
    )
    mock_morpho.is_morpho_vault.return_value = True
    mock_morpho_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, debt_token_id=7)

    assert result is expected
    mock_morpho.is_morpho_vault.assert_awaited_once_with(7)
    mock_morpho_repository.get_backed_breakdown.assert_awaited_once_with(protocol_id=1, debt_token_id=7)
    mock_repository.get_backed_breakdown.assert_not_awaited()


@pytest.mark.asyncio
async def test_morpho_empty_breakdown(
    service: BackedBreakdownService,
    mock_morpho_repository: AsyncMock,
    mock_morpho: AsyncMock,
) -> None:
    expected = BackedBreakdown(debt_token_id=7, protocol_id=1, items=())
    mock_morpho.is_morpho_vault.return_value = True
    mock_morpho_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, debt_token_id=7)

    assert result is expected
    assert result.items == ()


@pytest.mark.asyncio
async def test_morpho_propagates_exception(
    service: BackedBreakdownService,
    mock_morpho_repository: AsyncMock,
    mock_morpho: AsyncMock,
) -> None:
    mock_morpho.is_morpho_vault.return_value = True
    mock_morpho_repository.get_backed_breakdown.side_effect = RuntimeError("morpho db down")

    with pytest.raises(RuntimeError, match="morpho db down"):
        await service.get_backed_breakdown(protocol_id=1, debt_token_id=7)


@pytest.mark.asyncio
async def test_morpho_check_failure_propagates(
    service: BackedBreakdownService,
    mock_morpho: AsyncMock,
) -> None:
    mock_morpho.is_morpho_vault.side_effect = RuntimeError("morpho down")

    with pytest.raises(RuntimeError, match="morpho down"):
        await service.get_backed_breakdown(protocol_id=1, debt_token_id=42)
