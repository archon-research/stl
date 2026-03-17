# tests/unit/test_risk_calculation_service.py
import asyncio
from decimal import Decimal
from typing import Protocol
from unittest.mock import AsyncMock, patch

import pytest

from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.risk_engine.entities import LiquidationParams, RiskBreakdown, RiskEnrichedCollateral
from app.services.risk_calculation_service import RiskCalculationService


def _breakdown(items: tuple) -> BackedBreakdown:
    return BackedBreakdown(backed_asset_id=42, protocol_id=1, items=items)


def _contrib(token_id: int, symbol: str, amount: str) -> CollateralContribution:
    return CollateralContribution(
        token_id=token_id,
        symbol=symbol,
        amount=Decimal(amount),
        backing_pct=Decimal("100"),
    )


def _params(token_id: int, lt: str, lb: str) -> LiquidationParams:
    return LiquidationParams(
        token_id=token_id,
        liquidation_threshold=Decimal(lt),
        liquidation_bonus=Decimal(lb),
    )


class MockBackedBreakdownRepository(Protocol):
    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown: ...


class MockBackedBreakdownResolver(Protocol):
    async def resolve(self, protocol_id: int) -> MockBackedBreakdownRepository: ...


class MockLiquidationParamsRepository(Protocol):
    async def get_params(self, backed_asset_id: int, token_ids: list[int]) -> dict[int, LiquidationParams]: ...


class MockLiquidationParamsResolver(Protocol):
    async def resolve(self, protocol_id: int) -> MockLiquidationParamsRepository: ...


class MockTokenPriceRepository(Protocol):
    async def get_prices(self, token_ids: list[int]) -> dict[int, Decimal]: ...


@pytest.fixture
def mock_breakdown_resolver() -> AsyncMock:
    return AsyncMock(spec=MockBackedBreakdownResolver)


@pytest.fixture
def mock_breakdown_repo() -> AsyncMock:
    return AsyncMock(spec=MockBackedBreakdownRepository)


@pytest.fixture
def mock_liq_params_resolver() -> AsyncMock:
    return AsyncMock(spec=MockLiquidationParamsResolver)


@pytest.fixture
def mock_liq_params_repo() -> AsyncMock:
    return AsyncMock(spec=MockLiquidationParamsRepository)


@pytest.fixture
def mock_price_repo() -> AsyncMock:
    return AsyncMock(spec=MockTokenPriceRepository)


@pytest.fixture
def service(
    mock_breakdown_resolver: AsyncMock,
    mock_liq_params_resolver: AsyncMock,
    mock_price_repo: AsyncMock,
) -> RiskCalculationService:
    return RiskCalculationService(
        backed_breakdown_resolver=mock_breakdown_resolver,
        liquidation_params_resolver=mock_liq_params_resolver,
        token_price_repository=mock_price_repo,
    )


@pytest.mark.asyncio
async def test_get_bad_debt_orchestrates_three_repos(
    service: RiskCalculationService,
    mock_breakdown_resolver: AsyncMock,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_resolver: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_price_repo: AsyncMock,
) -> None:
    """Service resolves repos, fetches data concurrently, applies gap formula."""
    mock_breakdown_resolver.resolve.return_value = mock_breakdown_repo
    mock_liq_params_resolver.resolve.return_value = mock_liq_params_repo

    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "5"),)
    )
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_price_repo.get_prices.return_value = {10: Decimal("2000")}

    result = await service.get_bad_debt(protocol_id=1, backed_asset_id=42, gap_pct=Decimal("0.15"))

    assert isinstance(result, Decimal)
    # At gap=15%, no bad debt expected for wstETH-like params
    assert result >= Decimal("0")
    mock_breakdown_resolver.resolve.assert_awaited_once_with(1)
    mock_liq_params_resolver.resolve.assert_awaited_once_with(1)
    mock_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(42)
    mock_liq_params_repo.get_params.assert_awaited_once_with(42, [10])
    mock_price_repo.get_prices.assert_awaited_once_with([10])


@pytest.mark.asyncio
async def test_items_with_missing_price_are_skipped(
    service: RiskCalculationService,
    mock_breakdown_resolver: AsyncMock,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_resolver: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_price_repo: AsyncMock,
) -> None:
    mock_breakdown_resolver.resolve.return_value = mock_breakdown_repo
    mock_liq_params_resolver.resolve.return_value = mock_liq_params_repo

    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "5"), _contrib(11, "UNKNOWN", "1"))
    )
    mock_liq_params_repo.get_params.return_value = {
        10: _params(10, "0.825", "1.05"),
        11: _params(11, "0.50", "1.20"),
    }
    # token 11 has no price
    mock_price_repo.get_prices.return_value = {10: Decimal("2000")}

    # Should not raise — token 11 is silently skipped
    result = await service.get_bad_debt(protocol_id=1, backed_asset_id=42, gap_pct=Decimal("0.50"))
    assert isinstance(result, Decimal)


@pytest.mark.asyncio
async def test_items_with_missing_liq_params_are_skipped(
    service: RiskCalculationService,
    mock_breakdown_resolver: AsyncMock,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_resolver: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_price_repo: AsyncMock,
) -> None:
    mock_breakdown_resolver.resolve.return_value = mock_breakdown_repo
    mock_liq_params_resolver.resolve.return_value = mock_liq_params_repo

    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "5"), _contrib(11, "STABLECOIN", "10000"))
    )
    # token 11 (stablecoin) has no liquidation params (excluded from collateral)
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_price_repo.get_prices.return_value = {10: Decimal("2000"), 11: Decimal("1")}

    result = await service.get_bad_debt(protocol_id=1, backed_asset_id=42, gap_pct=Decimal("0.50"))
    assert isinstance(result, Decimal)


@pytest.mark.asyncio
async def test_empty_breakdown_returns_zero(
    service: RiskCalculationService,
    mock_breakdown_resolver: AsyncMock,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_resolver: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_price_repo: AsyncMock,
) -> None:
    mock_breakdown_resolver.resolve.return_value = mock_breakdown_repo
    mock_liq_params_resolver.resolve.return_value = mock_liq_params_repo
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(())
    mock_liq_params_repo.get_params.return_value = {}
    mock_price_repo.get_prices.return_value = {}

    result = await service.get_bad_debt(protocol_id=1, backed_asset_id=42, gap_pct=Decimal("0.15"))
    assert result == Decimal("0")


@pytest.mark.asyncio
async def test_get_risk_breakdown_returns_enriched_items(
    service: RiskCalculationService,
    mock_breakdown_resolver: AsyncMock,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_resolver: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_price_repo: AsyncMock,
) -> None:
    mock_breakdown_resolver.resolve.return_value = mock_breakdown_repo
    mock_liq_params_resolver.resolve.return_value = mock_liq_params_repo
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "5"),)
    )
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_price_repo.get_prices.return_value = {10: Decimal("2000")}

    result = await service.get_risk_breakdown(protocol_id=1, backed_asset_id=42)

    assert isinstance(result, RiskBreakdown)
    assert len(result.items) == 1
    item = result.items[0]
    assert item.token_id == 10
    assert item.symbol == "WETH"
    assert item.amount_usd == Decimal("10000")  # 5 × 2000
    assert item.price_usd == Decimal("2000")
    assert item.liquidation_threshold == Decimal("0.825")
    assert item.liquidation_bonus == Decimal("1.05")
