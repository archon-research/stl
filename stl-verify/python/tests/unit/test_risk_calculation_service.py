# tests/unit/test_risk_calculation_service.py
from decimal import Decimal
from typing import Protocol
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.risk import LiquidationParams, RiskBreakdown
from app.services.risk_calculation_service import RiskCalculationService


def _breakdown(items: tuple) -> BackedBreakdown:
    return BackedBreakdown(backed_asset_id=42, items=items)


def _contrib(token_id: int, symbol: str, backing_usd: str, price_usd: str | None = "2000") -> CollateralContribution:
    return CollateralContribution(
        token_id=token_id,
        symbol=symbol,
        backing_usd=Decimal(backing_usd),
        backing_pct=Decimal("100"),
        price_usd=Decimal(price_usd) if price_usd is not None else None,
    )


def _params(token_id: int, lt: str, lb: str) -> LiquidationParams:
    return LiquidationParams(
        token_id=token_id,
        liquidation_threshold=Decimal(lt),
        liquidation_bonus=Decimal(lb),
    )


class MockBackedBreakdownRepository(Protocol):
    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown: ...


class MockLiquidationParamsRepository(Protocol):
    async def get_params(self, backed_asset_id: int, token_ids: list[int]) -> dict[int, LiquidationParams]: ...


class MockAllocationSharePort(Protocol):
    async def get_share(self) -> Decimal: ...


@pytest.fixture
def mock_breakdown_repo() -> AsyncMock:
    return AsyncMock(spec=MockBackedBreakdownRepository)


@pytest.fixture
def mock_liq_params_repo() -> AsyncMock:
    return AsyncMock(spec=MockLiquidationParamsRepository)


@pytest.fixture
def mock_share_port() -> AsyncMock:
    mock = AsyncMock(spec=MockAllocationSharePort)
    # Default to full share so existing tests that don't care about scaling
    # still pass without each having to configure this mock explicitly.
    mock.get_share.return_value = Decimal("1")
    return mock


@pytest.fixture
def service(
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_share_port: AsyncMock,
) -> RiskCalculationService:
    return RiskCalculationService(
        breakdown_repo=mock_breakdown_repo,
        liq_params_repo=mock_liq_params_repo,
        share_port=mock_share_port,
    )


@pytest.mark.asyncio
async def test_get_bad_debt_orchestrates_repos(
    service: RiskCalculationService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    """Service fetches breakdown and liq params, applies gap formula."""
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

    result = await service.get_bad_debt(backed_asset_id=42, gap_pct=Decimal("0.15"))

    assert isinstance(result, Decimal)
    assert result >= Decimal("0")
    mock_breakdown_repo.get_backed_breakdown.assert_awaited_once_with(42)
    mock_liq_params_repo.get_params.assert_awaited_once_with(42, [10])


@pytest.mark.asyncio
async def test_items_with_missing_price_are_skipped(
    service: RiskCalculationService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "10000", "2000"), _contrib(11, "UNKNOWN", "500", None))
    )
    mock_liq_params_repo.get_params.return_value = {
        10: _params(10, "0.825", "1.05"),
        11: _params(11, "0.50", "1.20"),
    }

    # Should not raise — token 11 (no price) is silently skipped
    result = await service.get_bad_debt(backed_asset_id=42, gap_pct=Decimal("0.50"))
    assert isinstance(result, Decimal)


@pytest.mark.asyncio
async def test_items_with_missing_liq_params_are_skipped(
    service: RiskCalculationService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "10000", "2000"), _contrib(11, "STABLECOIN", "10000", "1"))
    )
    # token 11 (stablecoin) has no liquidation params
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

    result = await service.get_bad_debt(backed_asset_id=42, gap_pct=Decimal("0.50"))
    assert isinstance(result, Decimal)


@pytest.mark.asyncio
async def test_empty_breakdown_returns_zero(
    service: RiskCalculationService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(())
    mock_liq_params_repo.get_params.return_value = {}

    result = await service.get_bad_debt(backed_asset_id=42, gap_pct=Decimal("0.15"))
    assert result == Decimal("0")


@pytest.mark.asyncio
async def test_get_risk_breakdown_returns_enriched_items(
    service: RiskCalculationService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

    result = await service.get_risk_breakdown(backed_asset_id=42)

    assert isinstance(result, RiskBreakdown)
    assert len(result.items) == 1
    item = result.items[0]
    assert item.token_id == 10
    assert item.symbol == "WETH"
    assert item.amount_usd == Decimal("10000")  # backing_usd passed through
    assert item.price_usd == Decimal("2000")
    assert item.amount == Decimal("5")  # 10000 / 2000
    assert item.liquidation_threshold == Decimal("0.825")
    assert item.liquidation_bonus == Decimal("1.05")


@pytest.mark.asyncio
async def test_share_scales_backing_usd(
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_share_port: AsyncMock,
) -> None:
    """backing_usd is multiplied by share before the gap formula runs."""
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "100000", "2000"),)
    )
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_share_port.get_share.return_value = Decimal("0.5")

    svc = RiskCalculationService(
        breakdown_repo=mock_breakdown_repo,
        liq_params_repo=mock_liq_params_repo,
        share_port=mock_share_port,
    )
    breakdown = await svc.get_risk_breakdown(backed_asset_id=42)

    # backing_usd should be 100000 * 0.5 = 50000
    assert breakdown.items[0].amount_usd == Decimal("50000")
    mock_share_port.get_share.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_share_leaves_backing_usd_unchanged(
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_share_port: AsyncMock,
) -> None:
    """share=1.0 is a no-op."""
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "10000", "2000"),)
    )
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_share_port.get_share.return_value = Decimal("1")

    svc = RiskCalculationService(
        breakdown_repo=mock_breakdown_repo,
        liq_params_repo=mock_liq_params_repo,
        share_port=mock_share_port,
    )
    breakdown = await svc.get_risk_breakdown(backed_asset_id=42)

    assert breakdown.items[0].amount_usd == Decimal("10000")
