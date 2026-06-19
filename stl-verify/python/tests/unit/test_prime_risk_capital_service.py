from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.ports.allocation_repository import AllocationRepositoryPort
from app.services.prime_risk_capital_service import PrimeRiskCapitalService
from tests.factories import make_receipt_token_position

_PRIME = EthAddress("0x" + "ab" * 20)


class _FakeModel:
    def __init__(self, name: str, applies_ids: set[int], rrc: Decimal, crr: Decimal) -> None:
        self.risk_model = name
        self._ids = applies_ids
        self._rrc = rrc
        self._crr = crr

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:
        return asset_id in self._ids

    async def compute(self, asset_id, prime_id, overrides):
        return SimpleNamespace(rrc_usd=self._rrc, comparable_crr_pct=self._crr, risk_model=self.risk_model)


class _FakeRegistry:
    def __init__(self, models: list[_FakeModel]) -> None:
        self._models = models

    def applicable(self, asset_id: int, prime_id: EthAddress):
        return [m for m in self._models if m.applies_to(asset_id, prime_id)]


def _repo(positions, total_rc):
    repo = AsyncMock(spec=AllocationRepositoryPort)
    repo.list_receipt_token_positions.return_value = positions
    repo.get_latest_total_capital_usd.return_value = total_rc
    return repo


@pytest.mark.asyncio
async def test_compute_mixes_modeled_and_unmodeled_allocations():
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600")),
        make_receipt_token_position(receipt_token_id=2, symbol="spDAI", amount_usd=Decimal("400")),
    ]
    # gap_sweep applies only to asset 1.
    registry = _FakeRegistry([_FakeModel("gap_sweep", {1}, rrc=Decimal("30"), crr=Decimal("5"))])
    service = PrimeRiskCapitalService(_repo(positions, Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.model == "gap_sweep"
    assert result.exposure_usd == Decimal("1000")
    assert result.total_risk_capital_usd == Decimal("100")
    assert result.required_risk_capital_usd == Decimal("30")
    assert result.modeled_exposure_usd == Decimal("600")
    assert result.modeled_pct == Decimal("0.6000")
    assert result.encumbrance_ratio == Decimal("0.3000")  # 30 / 100

    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is True
    assert by_id[1].required_risk_capital_usd == Decimal("30")
    assert by_id[1].crr_pct == Decimal("5")
    assert by_id[1].model == "gap_sweep"
    assert by_id[2].applied is False
    assert by_id[2].required_risk_capital_usd is None
    assert by_id[2].crr_pct is None
    assert by_id[2].model is None


@pytest.mark.asyncio
async def test_compute_encumbrance_none_when_no_total_risk_capital():
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600"))]
    registry = _FakeRegistry([_FakeModel("gap_sweep", {1}, rrc=Decimal("30"), crr=Decimal("5"))])
    service = PrimeRiskCapitalService(_repo(positions, None), registry)

    result = await service.compute(_PRIME)

    assert result.total_risk_capital_usd is None
    assert result.encumbrance_ratio is None
    assert result.required_risk_capital_usd == Decimal("30")


@pytest.mark.asyncio
async def test_compute_empty_positions_yields_zeroes_and_null_ratios():
    registry = _FakeRegistry([_FakeModel("gap_sweep", set(), rrc=Decimal("0"), crr=Decimal("0"))])
    service = PrimeRiskCapitalService(_repo([], Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.exposure_usd == Decimal("0")
    assert result.required_risk_capital_usd == Decimal("0")
    assert result.modeled_pct is None
    assert result.encumbrance_ratio == Decimal("0")  # 0 / 100
    assert result.per_allocation == []


@pytest.mark.asyncio
async def test_compute_skips_zero_exposure_positions():
    # Asset 1 has zero balance: the model applies, but we must not run a compute
    # for it (it contributes nothing) and it is reported as not modeled.
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aEthUSDT", amount_usd=Decimal("0")),
        make_receipt_token_position(receipt_token_id=2, symbol="spUSDT", amount_usd=Decimal("600")),
    ]
    boom = _FakeModel("gap_sweep", {1, 2}, rrc=Decimal("30"), crr=Decimal("5"))

    async def _fail_on_zero(asset_id, prime_id, overrides):
        assert asset_id != 1, "must not compute a zero-exposure position"
        return SimpleNamespace(rrc_usd=Decimal("30"), comparable_crr_pct=Decimal("5"), risk_model="gap_sweep")

    boom.compute = _fail_on_zero  # type: ignore[method-assign]
    service = PrimeRiskCapitalService(_repo(positions, Decimal("100")), _FakeRegistry([boom]))

    result = await service.compute(_PRIME)

    by_id = {a.receipt_token_id: a for a in result.per_allocation}
    assert by_id[1].applied is False
    assert by_id[2].applied is True
    assert result.required_risk_capital_usd == Decimal("30")
    assert result.modeled_exposure_usd == Decimal("600")


@pytest.mark.asyncio
async def test_compute_ignores_non_default_models():
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600"))]
    # Only a non-default model applies; the default (gap_sweep) does not.
    registry = _FakeRegistry([_FakeModel("suraf", {1}, rrc=Decimal("99"), crr=Decimal("9"))])
    service = PrimeRiskCapitalService(_repo(positions, Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.required_risk_capital_usd == Decimal("0")
    assert result.per_allocation[0].applied is False
    assert result.modeled_exposure_usd == Decimal("0")
