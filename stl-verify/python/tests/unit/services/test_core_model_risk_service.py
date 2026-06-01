"""Unit tests for CoreModelRiskService — 100% coverage required."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import CoreModelDetails, RrcResult
from app.domain.exceptions import InvalidOverrideError
from app.ports.core_model_results_reader import CoreModelResult
from app.services.core_model_risk_service import CoreModelRiskService

_PRIME = EthAddress("0xBcca60bB61934080951369a648Fb03DF4F96263C")
_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

_RESULT = CoreModelResult(
    market_key="sparklend_usdc",
    crr_el_pct=Decimal("12.5"),
    crr_es_pct=Decimal("15.0"),
    crr_var_pct=Decimal("10.0"),
    hhi=Decimal("22.3"),
    protocol="SPARKLEND",
    forecast_step=14,
    n_mc=10000,
    copula_type="T-COPULA",
    computed_at=_NOW,
)


def _service(
    asset_to_market_key: dict | None = None,
    get_latest_return: CoreModelResult | None = _RESULT,
    usd_exposure: Decimal = Decimal("10000.00"),
) -> CoreModelRiskService:
    results_reader = AsyncMock()
    results_reader.get_latest.return_value = get_latest_return
    allocation_repo = AsyncMock()
    allocation_repo.get_usd_exposure.return_value = usd_exposure
    return CoreModelRiskService(
        asset_to_market_key=asset_to_market_key or {1: "sparklend_usdc"},
        results_reader=results_reader,
        allocation_repo=allocation_repo,
    )


def test_applies_to_known_asset():
    svc = _service()
    assert svc.applies_to(1, _PRIME) is True


def test_applies_to_unknown_asset():
    svc = _service()
    assert svc.applies_to(99, _PRIME) is False


async def test_compute_returns_rrc_result():
    svc = _service(usd_exposure=Decimal("10000.00"))
    result = await svc.compute(1, _PRIME, {})
    assert isinstance(result, RrcResult)
    assert result.risk_model == "core_model"
    # rrc_usd = 10000 * 12.5 / 100 = 1250.00
    assert result.rrc_usd == Decimal("1250.00")
    assert result.comparable_crr_pct == Decimal("12.5")


async def test_compute_details_populated():
    svc = _service()
    result = await svc.compute(1, _PRIME, {})
    assert isinstance(result.details, CoreModelDetails)
    assert result.details.protocol == "SPARKLEND"
    assert result.details.forecast_step == 14
    assert result.details.n_mc == 10000
    assert result.details.hhi == Decimal("22.3")


async def test_compute_raises_when_no_precomputed_result():
    svc = _service(get_latest_return=None)
    with pytest.raises(ValueError, match="no pre-computed result"):
        await svc.compute(1, _PRIME, {})


async def test_compute_with_usd_exposure_override():
    svc = _service(usd_exposure=Decimal("99999.00"))
    result = await svc.compute(1, _PRIME, {"usd_exposure": "5000"})
    # rrc_usd = 5000 * 12.5 / 100 = 625.00
    assert result.rrc_usd == Decimal("625.00")


async def test_compute_rejects_unknown_override():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="unknown override keys"):
        await svc.compute(1, _PRIME, {"unknown_key": 1})


async def test_compute_rejects_none_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="invalid usd_exposure"):
        await svc.compute(1, _PRIME, {"usd_exposure": None})


async def test_compute_rejects_non_positive_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="positive finite number"):
        await svc.compute(1, _PRIME, {"usd_exposure": "0"})


async def test_compute_rejects_infinite_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="positive finite number"):
        await svc.compute(1, _PRIME, {"usd_exposure": "Infinity"})


async def test_compute_rejects_oversized_usd_exposure_string():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="too long"):
        await svc.compute(1, _PRIME, {"usd_exposure": "1" * 65})


async def test_compute_rejects_exceeding_max_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="must be <="):
        await svc.compute(1, _PRIME, {"usd_exposure": "2e15"})
