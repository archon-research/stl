from collections.abc import Mapping
from decimal import Decimal
from typing import Any, cast

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import GapSweepDetails, ModelName, RrcResult, SurafDetails
from app.services.model_registry import ModelRegistry

_PRIME_A = EthAddress("0x" + "aa" * 20)
_PRIME_B = EthAddress("0x" + "bb" * 20)


class FakeRiskModel:
    risk_model: ModelName

    def __init__(self, risk_model: str, supported: set[tuple[int, EthAddress]]) -> None:
        self.risk_model = cast(ModelName, risk_model)
        self._supported = supported

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:
        return (asset_id, prime_id) in self._supported

    async def compute(self, asset_id: int, prime_id: EthAddress, overrides: Mapping[str, Any]) -> RrcResult:  # noqa: ARG002
        if self.risk_model == "suraf":
            return RrcResult(
                asset_id=asset_id,
                prime_id=prime_id,
                rrc_usd=Decimal("100"),
                comparable_crr_pct=Decimal("10"),
                risk_model="suraf",
                details=SurafDetails(
                    risk_model="suraf",
                    rating_id="rating",
                    rating_version="v1",
                    crr_pct=Decimal("10"),
                    unadjusted_crr_pct=Decimal("10"),
                    penalty_pp=Decimal("0"),
                    source_commit_sha="abc123",
                ),
            )
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=Decimal("200"),
            comparable_crr_pct=Decimal("20"),
            risk_model="gap_sweep",
            details=GapSweepDetails(
                risk_model="gap_sweep",
                gap_pct=Decimal("0.15"),
                loss_usd=Decimal("200"),
            ),
        )


def test_applicable_returns_empty_list_when_registry_has_no_models() -> None:
    registry = ModelRegistry([])

    assert registry.applicable(asset_id=1, prime_id=_PRIME_A) == []


def test_applicable_returns_single_matching_model() -> None:
    matching = FakeRiskModel("suraf", supported={(1, _PRIME_A)})
    registry = ModelRegistry([matching])

    assert registry.applicable(asset_id=1, prime_id=_PRIME_A) == [matching]


def test_applicable_returns_empty_list_when_no_models_apply() -> None:
    registry = ModelRegistry(
        [
            FakeRiskModel("suraf", supported={(1, _PRIME_A)}),
            FakeRiskModel("gap_sweep", supported={(2, _PRIME_B)}),
        ]
    )

    assert registry.applicable(asset_id=99, prime_id=_PRIME_A) == []


def test_applicable_returns_all_matching_models_in_construction_order() -> None:
    first = FakeRiskModel("suraf", supported={(1, _PRIME_A)})
    second = FakeRiskModel("gap_sweep", supported={(1, _PRIME_A)})
    third = FakeRiskModel("third_model", supported={(2, _PRIME_A)})
    registry = ModelRegistry([first, second, third])

    assert registry.applicable(asset_id=1, prime_id=_PRIME_A) == [first, second]


def test_applicable_order_is_stable_across_calls() -> None:
    first = FakeRiskModel("suraf", supported={(1, _PRIME_A)})
    second = FakeRiskModel("gap_sweep", supported={(1, _PRIME_A)})
    registry = ModelRegistry([first, second])

    first_result = registry.applicable(asset_id=1, prime_id=_PRIME_A)
    second_result = registry.applicable(asset_id=1, prime_id=_PRIME_A)

    assert first_result == [first, second]
    assert second_result == [first, second]


def test_applicable_returns_all_models_when_all_apply() -> None:
    first = FakeRiskModel("suraf", supported={(1, _PRIME_A)})
    second = FakeRiskModel("gap_sweep", supported={(1, _PRIME_A)})
    third = FakeRiskModel("third_model", supported={(1, _PRIME_A)})
    registry = ModelRegistry([first, second, third])

    assert registry.applicable(asset_id=1, prime_id=_PRIME_A) == [first, second, third]


def test_registry_rejects_duplicate_risk_model_discriminators() -> None:
    """Two services with the same discriminator make override dispatch ambiguous."""
    first = FakeRiskModel("suraf", supported={(1, _PRIME_A)})
    duplicate = FakeRiskModel("suraf", supported={(2, _PRIME_A)})

    with pytest.raises(ValueError, match="duplicate risk_model discriminators"):
        ModelRegistry([first, duplicate])
