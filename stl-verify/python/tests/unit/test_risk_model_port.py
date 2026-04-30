"""Table-driven tests for the RiskModel port, RrcResult, and FakeRiskModel."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import (
    GapSweepDetails,
    RrcResult,
    SurafDetails,
)
from app.ports.risk_model import RiskModel

_PRIME_A = EthAddress("0x" + "aa" * 20)


class FakeRiskModel:
    """Test-double that satisfies RiskModel with canned answers."""

    def __init__(
        self,
        model: str,
        supported: set[tuple[int, EthAddress]],
        result: RrcResult,
    ) -> None:
        self.model = model
        self._supported = supported
        self._result = result

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:
        return (asset_id, prime_id) in self._supported

    async def compute(self, asset_id: int, prime_id: EthAddress, overrides: Mapping[str, Any]) -> RrcResult:
        return self._result


_PRIME_B = EthAddress("0x" + "bb" * 20)

_SURAF_RESULT = RrcResult(
    asset_id=1,
    prime_id=_PRIME_A,
    rrc_usd=Decimal("337.00"),
    model="suraf",
    details=SurafDetails(
        rating_id="aave_ausdc",
        rating_version="v7",
        crr_pct=Decimal("33.7"),
        source_commit_sha="abc123",
    ),
)

_GAP_SWEEP_RESULT = RrcResult(
    asset_id=2,
    prime_id=_PRIME_A,
    rrc_usd=Decimal("1200.50"),
    model="gap_sweep",
    details=GapSweepDetails(
        gap_pct=Decimal("0.15"),
        bad_debt_usd=Decimal("1200.50"),
    ),
)


class TestRrcResult:
    """RrcResult construction and field access."""

    CASES = [
        pytest.param(
            _SURAF_RESULT,
            "suraf",
            SurafDetails,
            id="suraf-details",
        ),
        pytest.param(
            _GAP_SWEEP_RESULT,
            "gap_sweep",
            GapSweepDetails,
            id="gap-sweep-details",
        ),
    ]

    @pytest.mark.parametrize("result,expected_model,expected_details_type", CASES)
    def test_model_discriminator(
        self,
        result: RrcResult,
        expected_model: str,
        expected_details_type: type,
    ) -> None:
        assert result.model == expected_model
        assert isinstance(result.details, expected_details_type)

    def test_common_fields(self) -> None:
        assert _SURAF_RESULT.asset_id == 1
        assert _SURAF_RESULT.prime_id == str(_PRIME_A)
        assert _SURAF_RESULT.rrc_usd == Decimal("337.00")

    def test_suraf_details_fields(self) -> None:
        d = _SURAF_RESULT.details
        assert isinstance(d, SurafDetails)
        assert d.rating_id == "aave_ausdc"
        assert d.crr_pct == Decimal("33.7")

    def test_gap_sweep_details_fields(self) -> None:
        d = _GAP_SWEEP_RESULT.details
        assert isinstance(d, GapSweepDetails)
        assert d.gap_pct == Decimal("0.15")
        assert d.bad_debt_usd == Decimal("1200.50")

    def test_frozen(self) -> None:
        with pytest.raises(ValidationError):
            _SURAF_RESULT.rrc_usd = Decimal("0")  # type: ignore[misc]

    INVALID_CASES = [
        pytest.param(
            "suraf",
            GapSweepDetails(gap_pct=Decimal("0.15"), bad_debt_usd=Decimal("100")),
            id="suraf-model-with-gap-sweep-details",
        ),
        pytest.param(
            "gap_sweep",
            SurafDetails(rating_id="x", rating_version="v1", crr_pct=Decimal("10"), source_commit_sha="abc"),
            id="gap-sweep-model-with-suraf-details",
        ),
    ]

    @pytest.mark.parametrize("model,details", INVALID_CASES)
    def test_mismatched_model_and_details_rejected(self, model: str, details: object) -> None:
        with pytest.raises(ValidationError):
            RrcResult(
                asset_id=1,
                prime_id=_PRIME_A,
                rrc_usd=Decimal("100"),
                model=model,
                details=details,  # type: ignore[arg-type]
            )

    def test_unknown_model_string_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RrcResult(
                asset_id=1,
                prime_id=_PRIME_A,
                rrc_usd=Decimal("100"),
                model="typo_model",
                details=SurafDetails(
                    rating_id="x",
                    rating_version="v1",
                    crr_pct=Decimal("10"),
                    source_commit_sha="abc",
                ),
            )


class TestFakeRiskModel:
    """FakeRiskModel satisfies the RiskModel protocol and behaves correctly."""

    def test_satisfies_protocol(self) -> None:
        """FakeRiskModel is a structural subtype of RiskModel."""
        fake: RiskModel = FakeRiskModel(
            model="suraf",
            supported={(1, _PRIME_A)},
            result=_SURAF_RESULT,
        )
        assert isinstance(fake, FakeRiskModel)

    def test_model_attribute(self) -> None:
        fake = FakeRiskModel(model="suraf", supported={(1, _PRIME_A)}, result=_SURAF_RESULT)
        assert fake.model == "suraf"

    APPLIES_TO_CASES = [
        pytest.param(1, _PRIME_A, True, id="supported-pair"),
        pytest.param(1, _PRIME_B, False, id="wrong-prime"),
        pytest.param(99, _PRIME_A, False, id="wrong-asset"),
        pytest.param(99, _PRIME_B, False, id="both-wrong"),
    ]

    @pytest.mark.parametrize("asset_id,prime_id,expected", APPLIES_TO_CASES)
    def test_applies_to(
        self,
        asset_id: int,
        prime_id: EthAddress,
        expected: bool,
    ) -> None:
        fake = FakeRiskModel(model="suraf", supported={(1, _PRIME_A)}, result=_SURAF_RESULT)
        assert fake.applies_to(asset_id, prime_id) is expected

    async def test_compute_returns_configured_result(self) -> None:
        fake = FakeRiskModel(model="suraf", supported={(1, _PRIME_A)}, result=_SURAF_RESULT)
        got = await fake.compute(asset_id=1, prime_id=_PRIME_A, overrides={})
        assert got is _SURAF_RESULT

    async def test_compute_with_gap_sweep_result(self) -> None:
        fake = FakeRiskModel(model="gap_sweep", supported={(2, _PRIME_A)}, result=_GAP_SWEEP_RESULT)
        got = await fake.compute(asset_id=2, prime_id=_PRIME_A, overrides={})
        assert got.model == "gap_sweep"
        assert got.rrc_usd == Decimal("1200.50")
