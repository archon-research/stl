from decimal import Decimal
from typing import Protocol
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)
from app.services.backed_breakdown_service import BackedBreakdownService


class ProtocolScopedBackedBreakdownRepository(Protocol):
    """Spec contract: once selected, repository only needs backed_asset_id."""

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown: ...


class BackedBreakdownRepositoryResolver(Protocol):
    """Spec contract: select repository explicitly from protocol_id."""

    async def resolve(self, protocol_id: int) -> ProtocolScopedBackedBreakdownRepository: ...


@pytest.fixture
def mock_resolver() -> AsyncMock:
    return AsyncMock(spec=BackedBreakdownRepositoryResolver)


@pytest.fixture
def mock_repository() -> AsyncMock:
    return AsyncMock(spec=ProtocolScopedBackedBreakdownRepository)


@pytest.fixture
def mock_morpho() -> AsyncMock:
    mock = AsyncMock()
    mock.is_morpho_vault.return_value = False
    return mock


@pytest.fixture
def service(
    mock_resolver: AsyncMock,
) -> BackedBreakdownService:
    return BackedBreakdownService(repository_resolver=mock_resolver)


# ---------------------------------------------------------------------------
# Spec section: caller-facing service delegates via protocol-based resolver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_calls_resolver_with_protocol_id_and_selected_repository_with_backed_asset_id_only(
    service: BackedBreakdownService,
    mock_resolver: AsyncMock,
    mock_repository: AsyncMock,
) -> None:
    expected = BackedBreakdown(
        backed_asset_id=42,
        protocol_id=1,
        items=(
            CollateralContribution(
                token_id=10,
                symbol="WETH",
                amount=Decimal("1000.00"),
                backing_pct=Decimal("60.0000"),
            ),
            CollateralContribution(
                token_id=11,
                symbol="cbBTC",
                amount=Decimal("666.67"),
                backing_pct=Decimal("40.0000"),
            ),
        ),
    )
    mock_resolver.resolve.return_value = mock_repository
    mock_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, backed_asset_id=42)

    assert result is expected
    mock_resolver.resolve.assert_awaited_once_with(1)
    mock_repository.get_backed_breakdown.assert_awaited_once_with(42)


@pytest.mark.asyncio
async def test_returns_empty_breakdown_from_selected_repository(
    service: BackedBreakdownService,
    mock_resolver: AsyncMock,
    mock_repository: AsyncMock,
) -> None:
    expected = BackedBreakdown(backed_asset_id=99, protocol_id=1, items=())
    mock_resolver.resolve.return_value = mock_repository
    mock_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=1, backed_asset_id=99)

    assert result is expected
    assert result.items == ()
    mock_resolver.resolve.assert_awaited_once_with(1)
    mock_repository.get_backed_breakdown.assert_awaited_once_with(99)


@pytest.mark.asyncio
async def test_propagates_resolver_errors_unchanged(
    service: BackedBreakdownService,
    mock_resolver: AsyncMock,
) -> None:
    mock_resolver.resolve.side_effect = RuntimeError("unsupported protocol")

    with pytest.raises(RuntimeError, match="unsupported protocol"):
        await service.get_backed_breakdown(protocol_id=999, backed_asset_id=42)


@pytest.mark.asyncio
async def test_propagates_selected_repository_errors_unchanged(
    service: BackedBreakdownService,
    mock_resolver: AsyncMock,
    mock_repository: AsyncMock,
) -> None:
    mock_resolver.resolve.return_value = mock_repository
    mock_repository.get_backed_breakdown.side_effect = RuntimeError("db down")

    with pytest.raises(RuntimeError, match="db down"):
        await service.get_backed_breakdown(protocol_id=1, backed_asset_id=42)


@pytest.mark.asyncio
async def test_service_does_not_require_morpho_target_id_probing(
    service: BackedBreakdownService,
    mock_resolver: AsyncMock,
    mock_repository: AsyncMock,
    mock_morpho: AsyncMock,
) -> None:
    """Spec regression guard: routing is from protocol_id, not backed_asset_id probing."""
    expected = BackedBreakdown(backed_asset_id=7, protocol_id=77, items=())
    mock_morpho.is_morpho_vault.side_effect = AssertionError("service must not probe Morpho vault IDs")
    mock_resolver.resolve.return_value = mock_repository
    mock_repository.get_backed_breakdown.return_value = expected

    result = await service.get_backed_breakdown(protocol_id=77, backed_asset_id=7)

    assert result is expected
    mock_resolver.resolve.assert_awaited_once_with(77)
    mock_repository.get_backed_breakdown.assert_awaited_once_with(7)
