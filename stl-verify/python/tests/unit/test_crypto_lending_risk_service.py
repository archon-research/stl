from decimal import Decimal
from typing import Protocol
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.risk import GapSweepDetails, LiquidationParams, RiskBreakdown, RrcResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService


def _breakdown(items: tuple) -> BackedBreakdown:
    return BackedBreakdown(backed_asset_id=42, items=items)


def _contrib(token_id: int, symbol: str, backing_value: str, price_usd: str | None = "2000") -> CollateralContribution:
    return CollateralContribution(
        token_id=token_id,
        symbol=symbol,
        backing_value=Decimal(backing_value),
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
) -> CryptoLendingRiskService:
    return CryptoLendingRiskService(
        breakdown_repo=mock_breakdown_repo,
        liq_params_repo=mock_liq_params_repo,
        share_port=mock_share_port,
        default_gap_pct=Decimal("0.15"),
    )


@pytest.mark.asyncio
async def test_get_bad_debt_orchestrates_repos(
    service: CryptoLendingRiskService,
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
    service: CryptoLendingRiskService,
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
async def test_items_with_zero_price_are_skipped(
    service: CryptoLendingRiskService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(
        (_contrib(10, "WETH", "10000", "2000"), _contrib(11, "ZERO", "500", "0"))
    )
    mock_liq_params_repo.get_params.return_value = {
        10: _params(10, "0.825", "1.05"),
        11: _params(11, "0.50", "1.20"),
    }

    result = await service.get_bad_debt(backed_asset_id=42, gap_pct=Decimal("0.50"))
    assert isinstance(result, Decimal)


@pytest.mark.asyncio
async def test_items_with_missing_liq_params_are_skipped(
    service: CryptoLendingRiskService,
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
    service: CryptoLendingRiskService,
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
) -> None:
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(())
    mock_liq_params_repo.get_params.return_value = {}

    result = await service.get_bad_debt(backed_asset_id=42, gap_pct=Decimal("0.15"))
    assert result == Decimal("0")


@pytest.mark.asyncio
async def test_get_risk_breakdown_returns_enriched_items(
    service: CryptoLendingRiskService,
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
    assert item.amount_usd == Decimal("10000")  # backing_value passed through
    assert item.price_usd == Decimal("2000")
    assert item.amount == Decimal("5")  # 10000 / 2000
    assert item.liquidation_threshold == Decimal("0.825")
    assert item.liquidation_bonus == Decimal("1.05")


@pytest.mark.asyncio
async def test_share_scales_backing_value(
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_share_port: AsyncMock,
) -> None:
    """backing_value is multiplied by share before the gap formula runs."""
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "100000", "2000"),))
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_share_port.get_share.return_value = Decimal("0.5")

    svc = CryptoLendingRiskService(
        breakdown_repo=mock_breakdown_repo,
        liq_params_repo=mock_liq_params_repo,
        share_port=mock_share_port,
        default_gap_pct=Decimal("0.15"),
    )
    breakdown = await svc.get_risk_breakdown(backed_asset_id=42)

    # backing_value should be 100000 * 0.5 = 50000
    assert breakdown.items[0].amount_usd == Decimal("50000")
    mock_share_port.get_share.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_share_leaves_backing_value_unchanged(
    mock_breakdown_repo: AsyncMock,
    mock_liq_params_repo: AsyncMock,
    mock_share_port: AsyncMock,
) -> None:
    """share=1.0 is a no-op."""
    mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
    mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}
    mock_share_port.get_share.return_value = Decimal("1")

    svc = CryptoLendingRiskService(
        breakdown_repo=mock_breakdown_repo,
        liq_params_repo=mock_liq_params_repo,
        share_port=mock_share_port,
        default_gap_pct=Decimal("0.15"),
    )
    breakdown = await svc.get_risk_breakdown(backed_asset_id=42)

    assert breakdown.items[0].amount_usd == Decimal("10000")


# ---------------------------------------------------------------------------
# RiskModel tests
# ---------------------------------------------------------------------------

DUMMY_PRIME = EthAddress("0x" + "ab" * 20)
ASSET_ID = 42


class TestModelAttribute:
    def test_model_attribute_is_gap_sweep(
        self,
        service: CryptoLendingRiskService,
    ) -> None:
        assert service.model == "gap_sweep"


class TestAppliesTo:
    @pytest.mark.parametrize(
        "asset_id, prime_id",
        [
            (1, EthAddress("0x" + "00" * 20)),
            (999, EthAddress("0x" + "ff" * 20)),
            (ASSET_ID, DUMMY_PRIME),
        ],
        ids=["zero-addr", "high-ids", "standard"],
    )
    def test_applies_to_always_returns_true(
        self,
        service: CryptoLendingRiskService,
        asset_id: int,
        prime_id: EthAddress,
    ) -> None:
        assert service.applies_to(asset_id, prime_id) is True


class TestCompute:
    """Tests for the RiskModel.compute() implementation."""

    @pytest.mark.asyncio
    async def test_compute_with_default_gap_pct(
        self,
        service: CryptoLendingRiskService,
        mock_breakdown_repo: AsyncMock,
        mock_liq_params_repo: AsyncMock,
    ) -> None:
        """compute() with empty overrides uses the default gap_pct (0.15)."""
        mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
        mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

        result = await service.compute(ASSET_ID, DUMMY_PRIME, overrides={})

        assert isinstance(result, RrcResult)
        assert result.asset_id == ASSET_ID
        assert result.prime_id == str(DUMMY_PRIME)
        assert result.model == "gap_sweep"
        assert result.rrc_usd >= Decimal("0")
        assert isinstance(result.details, GapSweepDetails)
        assert result.details.gap_pct == Decimal("0.15")
        assert result.details.bad_debt_usd == result.rrc_usd

    @pytest.mark.asyncio
    async def test_compute_with_gap_pct_override(
        self,
        service: CryptoLendingRiskService,
        mock_breakdown_repo: AsyncMock,
        mock_liq_params_repo: AsyncMock,
    ) -> None:
        """compute() with gap_pct override uses the provided value."""
        mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
        mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

        result = await service.compute(ASSET_ID, DUMMY_PRIME, overrides={"gap_pct": Decimal("0.30")})

        assert isinstance(result, RrcResult)
        assert result.details.gap_pct == Decimal("0.30")
        # A larger gap should produce more bad debt than the default 0.15
        default_result = await service.compute(ASSET_ID, DUMMY_PRIME, overrides={})
        assert result.rrc_usd >= default_result.rrc_usd

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "overrides",
        [
            {"unknown_key": 42},
            {"gap_pct": Decimal("0.10"), "extra": "bad"},
            {"foo": "bar"},
        ],
        ids=["single-unknown", "valid-plus-unknown", "arbitrary-key"],
    )
    async def test_compute_rejects_unknown_override_keys(
        self,
        service: CryptoLendingRiskService,
        overrides: dict,
    ) -> None:
        """compute() raises ValueError when unknown override keys are provided."""
        with pytest.raises(ValueError, match="unknown override"):
            await service.compute(ASSET_ID, DUMMY_PRIME, overrides=overrides)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_gap_pct",
        [Decimal("-0.01"), Decimal("-1"), Decimal("1.01"), Decimal("2")],
        ids=["slightly-negative", "very-negative", "slightly-over-1", "way-over-1"],
    )
    async def test_compute_rejects_gap_pct_out_of_range(
        self,
        service: CryptoLendingRiskService,
        bad_gap_pct: Decimal,
    ) -> None:
        """compute() raises ValueError when gap_pct is outside [0, 1]."""
        with pytest.raises(ValueError, match="gap_pct"):
            await service.compute(ASSET_ID, DUMMY_PRIME, overrides={"gap_pct": bad_gap_pct})

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "boundary_gap_pct",
        [Decimal("0"), Decimal("1")],
        ids=["zero", "one"],
    )
    async def test_compute_accepts_boundary_gap_pct(
        self,
        service: CryptoLendingRiskService,
        mock_breakdown_repo: AsyncMock,
        mock_liq_params_repo: AsyncMock,
        boundary_gap_pct: Decimal,
    ) -> None:
        """compute() succeeds with gap_pct at the boundaries 0 and 1."""
        mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
        mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

        result = await service.compute(ASSET_ID, DUMMY_PRIME, overrides={"gap_pct": boundary_gap_pct})

        assert isinstance(result, RrcResult)
        assert result.details.gap_pct == boundary_gap_pct
        assert result.rrc_usd >= Decimal("0")

    @pytest.mark.asyncio
    async def test_compute_rrc_usd_is_positive(
        self,
        service: CryptoLendingRiskService,
        mock_breakdown_repo: AsyncMock,
        mock_liq_params_repo: AsyncMock,
    ) -> None:
        """compute() returns rrc_usd as a positive (abs'd) value, even though
        gap_sweep.total_bad_debt returns a negative value."""
        mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown((_contrib(10, "WETH", "10000", "2000"),))
        mock_liq_params_repo.get_params.return_value = {10: _params(10, "0.825", "1.05")}

        result = await service.compute(ASSET_ID, DUMMY_PRIME, overrides={"gap_pct": Decimal("0.50")})

        assert result.rrc_usd >= Decimal("0")
        assert result.details.bad_debt_usd >= Decimal("0")

    @pytest.mark.asyncio
    async def test_compute_empty_breakdown_returns_zero_rrc(
        self,
        service: CryptoLendingRiskService,
        mock_breakdown_repo: AsyncMock,
        mock_liq_params_repo: AsyncMock,
    ) -> None:
        """compute() returns rrc_usd=0 when the breakdown is empty."""
        mock_breakdown_repo.get_backed_breakdown.return_value = _breakdown(())
        mock_liq_params_repo.get_params.return_value = {}

        result = await service.compute(ASSET_ID, DUMMY_PRIME, overrides={})

        assert result.rrc_usd == Decimal("0")
        assert result.details.bad_debt_usd == Decimal("0")
