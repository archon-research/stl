from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import (
    InvalidRiskModelOverridesError,
    ResolvedRiskPosition,
    RiskBreakdown,
    RiskEnrichedCollateral,
    RiskModelInputsUnavailableError,
    RiskModelNotApplicableError,
    RiskPositionNotFoundError,
)
from app.risk_engine.crypto_lending import gap_sweep
from app.services.gap_sweep_risk_model import GapSweepRiskModel
from app.services.risk_service_factory import GapSweepConstruction

_VALID_PRIME = EthAddress("0x" + "1" * 40)


class FakeBreakdownService:
    def __init__(self, breakdown: RiskBreakdown) -> None:
        self._breakdown = breakdown
        self.calls: list[int] = []

    async def get_risk_breakdown(self, backed_asset_id: int) -> RiskBreakdown:
        self.calls.append(backed_asset_id)
        return self._breakdown


class FakeFactory:
    def __init__(self, construction: GapSweepConstruction | Exception) -> None:
        self._construction = construction
        self.calls: list[ResolvedRiskPosition] = []

    async def create_for_position(self, position: ResolvedRiskPosition) -> GapSweepConstruction:
        self.calls.append(position)
        if isinstance(self._construction, Exception):
            raise self._construction
        return self._construction


@pytest.fixture
def position_resolver() -> AsyncMock:
    return AsyncMock()


def _position(**overrides) -> ResolvedRiskPosition:
    defaults = dict(
        chain_id=1,
        prime_id=_VALID_PRIME,
        receipt_token_id=1,
        receipt_token_address="0x" + "a" * 40,
        underlying_token_id=10,
        underlying_token_address="0x" + "b" * 40,
        protocol_id=7,
        symbol="aUSDC",
        underlying_symbol="USDC",
        protocol_name="Aave V3",
        balance=Decimal("1000"),
        usd_exposure=Decimal("1000"),
    )
    defaults.update(overrides)
    return ResolvedRiskPosition(**defaults)


def _breakdown() -> RiskBreakdown:
    return RiskBreakdown(
        backed_asset_id=42,
        items=(
            RiskEnrichedCollateral(
                token_id=10,
                symbol="WETH",
                amount=Decimal("1"),
                backing_pct=Decimal("100"),
                amount_usd=Decimal("1000"),
                price_usd=Decimal("1000"),
                liquidation_threshold=Decimal("0.825"),
                liquidation_bonus=Decimal("1.05"),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_applies_to_returns_true_for_supported_protocol(position_resolver: AsyncMock) -> None:
    position_resolver.resolve.return_value = _position(protocol_name="Aave V3")
    model = GapSweepRiskModel(position_resolver, FakeFactory(Exception("unused")), default_gap_pct=Decimal("0.15"))

    assert await model.applies_to(1, _VALID_PRIME) is True


@pytest.mark.asyncio
async def test_applies_to_returns_false_for_unsupported_protocol(position_resolver: AsyncMock) -> None:
    position_resolver.resolve.return_value = _position(protocol_name="Uniswap V3")
    model = GapSweepRiskModel(position_resolver, FakeFactory(Exception("unused")), default_gap_pct=Decimal("0.15"))

    assert await model.applies_to(1, _VALID_PRIME) is False


@pytest.mark.asyncio
async def test_applies_to_propagates_missing_position(position_resolver: AsyncMock) -> None:
    position_resolver.resolve.side_effect = RiskPositionNotFoundError(1, _VALID_PRIME)
    model = GapSweepRiskModel(position_resolver, FakeFactory(Exception("unused")), default_gap_pct=Decimal("0.15"))

    with pytest.raises(RiskPositionNotFoundError):
        await model.applies_to(1, _VALID_PRIME)


@pytest.mark.asyncio
async def test_compute_uses_default_gap_pct_and_returns_positive_rrc(position_resolver: AsyncMock) -> None:
    position = _position(protocol_name="Morpho Blue", symbol="mUSDC")
    breakdown = _breakdown()
    fake_service = FakeBreakdownService(breakdown)
    factory = FakeFactory(GapSweepConstruction(service=fake_service, backed_asset_id=42))
    model = GapSweepRiskModel(position_resolver, factory, default_gap_pct=Decimal("0.40"))
    position_resolver.resolve.return_value = position

    result = await model.compute(1, _VALID_PRIME)

    expected = abs(gap_sweep.total_bad_debt(breakdown.items, Decimal("0.40")))
    assert result.asset_id == 1
    assert result.prime_id == _VALID_PRIME
    assert result.rrc_usd == expected
    assert result.details.model == "gap_sweep"
    assert result.details.protocol_name == "Morpho Blue"
    assert result.details.effective_gap_pct == Decimal("0.40")
    assert result.details.backed_asset_id == 42
    assert result.details.collateral_item_count == 1
    assert fake_service.calls == [42]
    assert factory.calls == [position]


@pytest.mark.asyncio
async def test_compute_accepts_gap_pct_override(position_resolver: AsyncMock) -> None:
    breakdown = _breakdown()
    fake_service = FakeBreakdownService(breakdown)
    model = GapSweepRiskModel(
        position_resolver,
        FakeFactory(GapSweepConstruction(service=fake_service, backed_asset_id=42)),
        default_gap_pct=Decimal("0.15"),
    )
    position_resolver.resolve.return_value = _position()

    result = await model.compute(1, _VALID_PRIME, {"gap_pct": Decimal("0.40")})

    assert result.rrc_usd == abs(gap_sweep.total_bad_debt(breakdown.items, Decimal("0.40")))
    assert result.details.effective_gap_pct == Decimal("0.40")


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"usd_exposure": Decimal("100")}, "unknown override keys"),
        ({"gap_pct": Decimal("-0.01")}, "gap_pct must be between 0 and 1"),
        ({"gap_pct": Decimal("1.01")}, "gap_pct must be between 0 and 1"),
    ],
)
@pytest.mark.asyncio
async def test_compute_validates_overrides_strictly(
    position_resolver: AsyncMock,
    overrides: dict[str, Decimal],
    match: str,
) -> None:
    model = GapSweepRiskModel(position_resolver, FakeFactory(Exception("unused")), default_gap_pct=Decimal("0.15"))

    with pytest.raises(InvalidRiskModelOverridesError, match=match):
        await model.compute(1, _VALID_PRIME, overrides)


@pytest.mark.asyncio
async def test_compute_raises_not_applicable_for_unsupported_protocol(position_resolver: AsyncMock) -> None:
    position_resolver.resolve.return_value = _position(protocol_name="Curve")
    model = GapSweepRiskModel(position_resolver, FakeFactory(Exception("unused")), default_gap_pct=Decimal("0.15"))

    with pytest.raises(RiskModelNotApplicableError, match="does not apply"):
        await model.compute(1, _VALID_PRIME)


@pytest.mark.asyncio
async def test_compute_wraps_factory_failures_as_typed_input_errors(position_resolver: AsyncMock) -> None:
    position_resolver.resolve.return_value = _position(protocol_name="Morpho Blue")
    model = GapSweepRiskModel(
        position_resolver,
        FakeFactory(ValueError("morpho vault not found for receipt token 1")),
        default_gap_pct=Decimal("0.15"),
    )

    with pytest.raises(RiskModelInputsUnavailableError, match="morpho vault not found"):
        await model.compute(1, _VALID_PRIME)
