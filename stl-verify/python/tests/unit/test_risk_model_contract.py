from decimal import Decimal

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import GapSweepRrcDetails, RrcResult
from app.ports.risk_model import RiskModel
from tests.unit.fakes.fake_risk_model import FakeRiskModel

_VALID_PRIME = EthAddress("0x" + "1" * 40)


@pytest.mark.asyncio
async def test_fake_model_satisfies_shared_risk_model_contract() -> None:
    model = FakeRiskModel()

    assert isinstance(model, RiskModel)
    assert await model.applies_to(1, _VALID_PRIME) is True

    result = await model.compute(1, _VALID_PRIME, {"usd_exposure": Decimal("12")})

    assert result.asset_id == 1
    assert result.prime_id == _VALID_PRIME
    assert result.rrc_usd == Decimal("5")
    assert result.details.model == "suraf"
    assert result.details.usd_exposure == Decimal("12")

    dumped = result.model_dump(mode="json")
    assert dumped["prime_id"] == str(_VALID_PRIME)
    assert dumped["details"]["model"] == "suraf"

    restored = RrcResult.model_validate(dumped)
    assert restored == result


def test_rrc_result_round_trips_with_gap_sweep_details() -> None:
    result = RrcResult(
        asset_id=99,
        prime_id=_VALID_PRIME,
        rrc_usd=Decimal("123.45"),
        details=GapSweepRrcDetails(
            chain_id=1,
            protocol_name="Morpho Blue",
            symbol="mUSDC",
            underlying_symbol="USDC",
            receipt_token_address="0x" + "a" * 40,
            underlying_token_address="0x" + "b" * 40,
            effective_gap_pct=Decimal("0.25"),
            backed_asset_id=7,
            collateral_item_count=3,
        ),
    )

    payload = result.model_dump_json()
    restored = RrcResult.model_validate_json(payload)

    assert restored == result
    assert restored.details.model == "gap_sweep"
