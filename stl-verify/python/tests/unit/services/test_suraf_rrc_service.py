from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import (
    InvalidRiskModelOverridesError,
    ResolvedRiskPosition,
    RiskModelInputsUnavailableError,
    RiskModelNotApplicableError,
    RiskPositionNotFoundError,
)
from app.risk_engine.suraf.result import SurafResult
from app.services.suraf_rrc_service import SurafRrcService

_VALID_PRIME = EthAddress("0x" + "1" * 40)


def _rating(rating_id: str, crr_pct: str, sha: str = "abc123", version: str = "v1") -> SurafResult:
    return SurafResult(
        rating_id=rating_id,
        version=version,
        crr_pct=Decimal(crr_pct),
        unadjusted_crr_pct=Decimal(crr_pct),
        penalty_pp=Decimal("0"),
        avg_score=Decimal("3"),
        source_commit_sha=sha,
        loaded_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


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


@pytest.fixture
def position_resolver() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def service(position_resolver: AsyncMock) -> SurafRrcService:
    return SurafRrcService(
        asset_to_rating={"ausdc": "aave_ausdc"},
        suraf_ratings={"aave_ausdc": _rating("aave_ausdc", "33.7", version="v7")},
        position_resolver=position_resolver,
    )


@pytest.mark.asyncio
async def test_applies_to_returns_true_when_rating_exists(
    service: SurafRrcService,
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.return_value = _position()

    assert await service.applies_to(1, _VALID_PRIME) is True
    position_resolver.resolve.assert_awaited_once_with(1, _VALID_PRIME)


@pytest.mark.asyncio
async def test_applies_to_returns_false_for_unrated_position(
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.return_value = _position(symbol="WBTC")
    service = SurafRrcService(asset_to_rating={}, suraf_ratings={}, position_resolver=position_resolver)

    assert await service.applies_to(1, _VALID_PRIME) is False


@pytest.mark.asyncio
async def test_applies_to_propagates_missing_position(
    service: SurafRrcService,
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.side_effect = RiskPositionNotFoundError(1, _VALID_PRIME)

    with pytest.raises(RiskPositionNotFoundError):
        await service.applies_to(1, _VALID_PRIME)


@pytest.mark.asyncio
async def test_compute_uses_resolved_position_exposure_and_populates_details(
    service: SurafRrcService,
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.return_value = _position(usd_exposure=Decimal("250"))

    result = await service.compute(1, _VALID_PRIME)

    assert result.asset_id == 1
    assert result.prime_id == _VALID_PRIME
    assert result.rrc_usd == Decimal("84.25")
    assert result.details.model == "suraf"
    assert result.details.symbol == "aUSDC"
    assert result.details.usd_exposure == Decimal("250")
    assert result.details.usd_exposure_source == "position"
    assert result.details.rating_id == "aave_ausdc"
    assert result.details.rating_version == "v7"
    assert result.details.crr_pct == Decimal("33.7")
    assert result.details.source_commit_sha == "abc123"


@pytest.mark.asyncio
async def test_compute_accepts_positive_usd_exposure_override(
    service: SurafRrcService,
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.return_value = _position(usd_exposure=None)

    result = await service.compute(1, _VALID_PRIME, {"usd_exposure": Decimal("500")})

    assert result.rrc_usd == Decimal("168.5")
    assert result.details.usd_exposure == Decimal("500")
    assert result.details.usd_exposure_source == "override"


@pytest.mark.asyncio
async def test_compute_raises_not_found_for_missing_position(
    service: SurafRrcService,
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.side_effect = RiskPositionNotFoundError(1, _VALID_PRIME)

    with pytest.raises(RiskPositionNotFoundError):
        await service.compute(1, _VALID_PRIME)


@pytest.mark.asyncio
async def test_compute_raises_when_default_usd_exposure_is_unavailable(
    service: SurafRrcService,
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.return_value = _position(usd_exposure=None)

    with pytest.raises(RiskModelInputsUnavailableError, match="usd_exposure is unavailable"):
        await service.compute(1, _VALID_PRIME)


@pytest.mark.asyncio
async def test_compute_rejects_unknown_override_keys(service: SurafRrcService) -> None:
    with pytest.raises(InvalidRiskModelOverridesError, match="unknown override keys"):
        await service.compute(1, _VALID_PRIME, {"gap_pct": Decimal("0.1")})


@pytest.mark.asyncio
async def test_compute_rejects_non_positive_override(service: SurafRrcService) -> None:
    with pytest.raises(InvalidRiskModelOverridesError, match="usd_exposure must be positive"):
        await service.compute(1, _VALID_PRIME, {"usd_exposure": Decimal("0")})


@pytest.mark.asyncio
async def test_compute_raises_not_applicable_when_position_is_unrated(
    position_resolver: AsyncMock,
) -> None:
    position_resolver.resolve.return_value = _position(symbol="WBTC")
    service = SurafRrcService(asset_to_rating={}, suraf_ratings={}, position_resolver=position_resolver)

    with pytest.raises(RiskModelNotApplicableError, match="does not apply"):
        await service.compute(1, _VALID_PRIME)


def test_compute_scenario_keeps_legacy_case_insensitive_symbol_lookup(
    service: SurafRrcService,
) -> None:
    result = service.compute_scenario("AUSDC", Decimal("100"))

    assert result is not None
    assert result.asset == "AUSDC"
    assert result.rating_id == "aave_ausdc"
    assert result.rrc_usd == Decimal("33.7")


def test_compute_scenario_returns_none_for_unmapped_asset(service: SurafRrcService) -> None:
    assert service.compute_scenario("WBTC", Decimal("1000")) is None
