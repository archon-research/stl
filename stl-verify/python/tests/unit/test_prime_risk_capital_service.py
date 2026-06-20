from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.ports.allocation_repository import AllocationRepositoryPort
from app.services.model_registry import ModelRegistry
from app.services.prime_risk_capital_service import PrimeRiskCapitalService
from tests.factories import make_receipt_token_position

_PRIME = EthAddress("0x" + "ab" * 20)


class _FakeModel:
    def __init__(self, name: str, applies_ids: set[int], rrc: Decimal, crr: Decimal) -> None:
        self.risk_model = name
        self._ids = applies_ids
        self._rrc = rrc
        self._crr = crr
        self.computed_ids: list[int] = []

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:
        return asset_id in self._ids

    async def compute(self, asset_id, prime_id, overrides):
        self.computed_ids.append(asset_id)
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


def _service(repo: AllocationRepositoryPort, registry: _FakeRegistry) -> PrimeRiskCapitalService:
    # _FakeRegistry / _FakeModel are structural stand-ins for the concrete
    # ModelRegistry / RiskModel; the service only reads registry.applicable().
    return PrimeRiskCapitalService(repo, cast(ModelRegistry, registry))


@pytest.mark.asyncio
async def test_compute_mixes_modeled_and_unmodeled_allocations():
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="spUSDT", amount_usd=Decimal("600")),
        make_receipt_token_position(receipt_token_id=2, symbol="spDAI", amount_usd=Decimal("400")),
    ]
    # gap_sweep applies only to asset 1.
    registry = _FakeRegistry([_FakeModel("gap_sweep", {1}, rrc=Decimal("30"), crr=Decimal("5"))])
    service = _service(_repo(positions, Decimal("100")), registry)

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
    service = _service(_repo(positions, None), registry)

    result = await service.compute(_PRIME)

    assert result.total_risk_capital_usd is None
    assert result.encumbrance_ratio is None
    assert result.required_risk_capital_usd == Decimal("30")


@pytest.mark.asyncio
async def test_compute_empty_positions_yields_zeroes_and_null_ratios():
    registry = _FakeRegistry([_FakeModel("gap_sweep", set(), rrc=Decimal("0"), crr=Decimal("0"))])
    service = _service(_repo([], Decimal("100")), registry)

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
    model = _FakeModel("gap_sweep", {1, 2}, rrc=Decimal("30"), crr=Decimal("5"))
    service = _service(_repo(positions, Decimal("100")), _FakeRegistry([model]))

    result = await service.compute(_PRIME)

    # The zero-exposure position must be reported as not modeled and, crucially,
    # must never trigger a (costly) model compute.
    assert model.computed_ids == [2]
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
    service = _service(_repo(positions, Decimal("100")), registry)

    result = await service.compute(_PRIME)

    assert result.required_risk_capital_usd == Decimal("0")
    assert result.per_allocation[0].applied is False
    assert result.modeled_exposure_usd == Decimal("0")


# ----------------------------------------------------------------------
# Share fan-out elimination
# ----------------------------------------------------------------------

from app.adapters.postgres.crypto_lending_reader import PostgresCryptoLendingReader  # noqa: E402
from app.domain.entities.backed_breakdown import BackedBreakdown  # noqa: E402
from app.domain.entities.receipt_token import ReceiptTokenInfo  # noqa: E402
from app.services.crypto_lending_risk_service import CryptoLendingRiskService  # noqa: E402


def _info(receipt_token_id: int, receipt_token_token_id: int = 777) -> ReceiptTokenInfo:
    return ReceiptTokenInfo(
        receipt_token_id=receipt_token_id,
        protocol_id=1,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f"),
        chain_id=1,
        protocol_name="Aave V3",
        receipt_token_token_id=receipt_token_token_id,
    )


def _crypto_lending_service(reader) -> CryptoLendingRiskService:
    return CryptoLendingRiskService(
        reader=reader,
        default_gap_pct=Decimal("0.15"),
        supported_asset_ids={1, 2, 3},
    )


@pytest.mark.asyncio
async def test_prime_compute_uses_batch_get_shares_and_skips_per_asset_get_share():
    """The prime service must collapse per-allocation share lookups into one DB call.

    Regression check: if someone re-introduces ``reader.get_share`` inside the
    ``asyncio.gather`` loop, this test catches it — ``get_share`` must remain
    un-awaited and ``batch_get_shares`` must be called exactly once with all
    crypto-lending asset infos.
    """
    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100")),
        make_receipt_token_position(receipt_token_id=2, symbol="aDAI", amount_usd=Decimal("200")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    infos = {1: _info(1, 777), 2: _info(2, 888)}
    # AsyncMock with a sync callable side_effect calls it for each invocation
    # and wraps the return value into a coroutine. Using a callable avoids
    # StopAsyncIteration when ``compute_with_share`` re-fetches infos.
    reader.get_receipt_token.side_effect = lambda aid: infos[aid]
    reader.batch_get_shares.return_value = {1: Decimal("0.4"), 2: Decimal("0.25")}
    reader.get_breakdown.return_value = BackedBreakdown(backed_asset_id=42, items=())

    model = _crypto_lending_service(reader)
    registry = _FakeRegistry([model])
    service = _service(_repo(positions, Decimal("1000")), registry)

    result = await service.compute(_PRIME)

    # batch_get_shares hit once, get_share never hit.
    reader.batch_get_shares.assert_awaited_once()
    reader.get_share.assert_not_awaited()
    # The result still surfaces the per-allocation entries even though the
    # gap-sweep RRC happens to be zero (no breakdown items in the test fixture).
    assert len(result.per_allocation) == 2


@pytest.mark.asyncio
async def test_prime_compute_propagates_per_asset_share_errors():
    """A ``MissingShareError`` returned by the batch must surface like the un-batched path.

    The endpoint translates ``MissingShareError`` to ``503 share_data_missing``;
    swallowing it inside the dispatch shim would silently hide the warm-up
    window from clients.
    """
    from app.domain.exceptions import MissingShareError

    positions = [
        make_receipt_token_position(receipt_token_id=1, symbol="aWETH", amount_usd=Decimal("100")),
    ]
    reader = AsyncMock(spec=PostgresCryptoLendingReader)
    reader.get_receipt_token.return_value = _info(1, 777)
    reader.batch_get_shares.return_value = {1: MissingShareError("warm-up")}

    model = _crypto_lending_service(reader)
    registry = _FakeRegistry([model])
    service = _service(_repo(positions, Decimal("1000")), registry)

    with pytest.raises(MissingShareError, match="warm-up"):
        await service.compute(_PRIME)


@pytest.mark.asyncio
async def test_prime_compute_unaffected_for_non_crypto_lending_models():
    """The legacy ``model.compute`` path must remain unchanged for non-crypto-lending models.

    SURAF/CORE are not crypto-lending and will route through the unbatched
    dispatch. The ``isinstance`` check must not poison their flow.
    """
    positions = [make_receipt_token_position(receipt_token_id=1, symbol="X", amount_usd=Decimal("100"))]
    fake = _FakeModel("gap_sweep", {1}, rrc=Decimal("7"), crr=Decimal("1"))
    service = _service(_repo(positions, Decimal("100")), _FakeRegistry([fake]))

    result = await service.compute(_PRIME)

    assert fake.computed_ids == [1]
    assert result.required_risk_capital_usd == Decimal("7")
