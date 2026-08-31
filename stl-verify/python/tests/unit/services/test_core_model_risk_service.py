"""Unit tests for CoreModelRiskService — 100% coverage required."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import CoreModelDetails, RrcResult
from app.domain.exceptions import InvalidOverrideError, ModelDataUnavailableError
from app.ports.core_model_results_reader import CoreModelResult
from app.ports.morpho_vault_allocations import MorphoVaultAllocations, MorphoVaultMarketAllocation
from app.services.core_model_risk_service import CoreModelRiskService, morpho_market_key_index

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

# The two MORPHO market pairs market_configs.json carries today.
_MORPHO_KEYS = {
    ("CBBTC", "USDC"): "morpho_cbbtc-usdc",
    ("WETH", "USDC"): "morpho_weth-usdc",
}
_MORPHO_ASSET = 7

_INFO = ReceiptTokenInfo(
    receipt_token_id=_MORPHO_ASSET,
    protocol_id=3,
    underlying_token_id=2,
    receipt_token_address=b"\xaa" * 20,
    chain_id=1,
    protocol_name="Morpho Blue",
    receipt_token_token_id=None,
)


def _morpho_result(
    market_key: str,
    crr_el: str,
    *,
    crr_es: str = "15.0",
    crr_var: str = "10.0",
    n_mc: int = 100,
    forecast_step: int = 14,
) -> CoreModelResult:
    return CoreModelResult(
        market_key=market_key,
        crr_el_pct=Decimal(crr_el),
        crr_es_pct=Decimal(crr_es),
        crr_var_pct=Decimal(crr_var),
        hhi=None,
        protocol="MORPHO",
        forecast_step=forecast_step,
        n_mc=n_mc,
        copula_type="T-COPULA",
        computed_at=_NOW,
    )


def _alloc(collateral: str, supply: str, *, loan: str = "USDC", market_id: int = 1) -> MorphoVaultMarketAllocation:
    return MorphoVaultMarketAllocation(
        morpho_market_id=market_id,
        collateral_symbol=collateral,
        loan_symbol=loan,
        supply_assets=Decimal(supply),
    )


def _vault(
    *allocations: MorphoVaultMarketAllocation,
    total_assets: str = "1000000",
) -> MorphoVaultAllocations:
    return MorphoVaultAllocations(
        vault_id=11,
        loan_token_symbol="USDC",
        total_assets=Decimal(total_assets),
        allocations=allocations,
    )


def _service(
    asset_to_market_key: dict | None = None,
    get_latest_return: CoreModelResult | None = _RESULT,
    usd_exposure: Decimal = Decimal("10000.00"),
    usd_exposure_error: Exception | None = None,
    *,
    results_by_key: dict[str, CoreModelResult] | None = None,
    receipt_token: ReceiptTokenInfo | None = _INFO,
    vault: MorphoVaultAllocations | None = None,
    morpho_asset_ids: frozenset[int] = frozenset(),
    min_coverage_pct: Decimal = Decimal("50"),
) -> CoreModelRiskService:
    results_reader = AsyncMock()
    if results_by_key is not None:
        results_reader.get_latest.side_effect = lambda key: results_by_key.get(key)
    else:
        results_reader.get_latest.return_value = get_latest_return
    allocation_repo = AsyncMock()
    if usd_exposure_error is not None:
        allocation_repo.get_usd_exposure.side_effect = usd_exposure_error
    else:
        allocation_repo.get_usd_exposure.return_value = usd_exposure
    receipt_tokens = AsyncMock()
    receipt_tokens.get.return_value = receipt_token
    morpho_allocations = AsyncMock()
    morpho_allocations.get_vault_allocations.return_value = vault
    return CoreModelRiskService(
        asset_to_market_key=asset_to_market_key or {1: "sparklend_usdc"},
        results_reader=results_reader,
        allocation_repo=allocation_repo,
        receipt_tokens=receipt_tokens,
        morpho_allocations=morpho_allocations,
        morpho_market_keys=_MORPHO_KEYS,
        morpho_asset_ids=morpho_asset_ids,
        min_coverage_pct=min_coverage_pct,
    )


def _morpho_service(**kwargs) -> CoreModelRiskService:
    kwargs.setdefault("morpho_asset_ids", frozenset({_MORPHO_ASSET}))
    return _service(**kwargs)


def test_applies_to_known_asset():
    svc = _service()
    assert svc.applies_to(1, _PRIME) is True


def test_applies_to_unknown_asset():
    svc = _service()
    assert svc.applies_to(99, _PRIME) is False


def test_applies_to_morpho_snapshot_asset():
    svc = _morpho_service()
    assert svc.applies_to(_MORPHO_ASSET, _PRIME) is True


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


async def test_compute_direct_result_carries_no_vault_fields():
    svc = _service()
    result = await svc.compute(1, _PRIME, {})
    assert isinstance(result.details, CoreModelDetails)
    assert result.details.coverage_pct is None
    assert result.details.markets is None


async def test_compute_raises_typed_error_when_no_precomputed_result():
    svc = _service(get_latest_return=None)
    with pytest.raises(ModelDataUnavailableError, match="no pre-computed result"):
        await svc.compute(1, _PRIME, {})


async def test_compute_raises_typed_error_when_prime_has_no_position():
    svc = _service(usd_exposure_error=ValueError("no position or price found"))
    with pytest.raises(ModelDataUnavailableError, match="no resolvable position"):
        await svc.compute(1, _PRIME, {})


async def test_compute_rejects_asset_no_shape_serves():
    svc = _service()
    with pytest.raises(ValueError, match="unsupported asset_id=99"):
        await svc.compute(99, _PRIME, {})


async def test_compute_with_override_skips_position_lookup_entirely():
    svc = _service(usd_exposure_error=ValueError("no position or price found"))
    result = await svc.compute(1, _PRIME, {"usd_exposure": "5000"})
    assert result.rrc_usd == Decimal("625.00")


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


async def test_get_latest_result_returns_result_for_known_asset():
    svc = _service()
    result = await svc.get_latest_result(1)
    assert result is not None
    assert result.market_key == "sparklend_usdc"
    assert result.crr_el_pct == Decimal("12.5")


async def test_get_latest_result_returns_none_for_unknown_asset():
    svc = _service()
    result = await svc.get_latest_result(99)
    assert result is None


async def test_get_latest_result_returns_none_when_no_db_row():
    svc = _service(get_latest_return=None)
    result = await svc.get_latest_result(1)
    assert result is None


async def test_get_latest_result_returns_none_for_morpho_asset():
    # A vault share has no single market, so the 1:1 lookup answers None.
    svc = _morpho_service(vault=_vault(_alloc("CBBTC", "400000")))
    assert await svc.get_latest_result(_MORPHO_ASSET) is None


# ---------------------------------------------------------------------------
# Morpho vault-share aggregation
# ---------------------------------------------------------------------------


async def test_morpho_fully_covered_vault_weights_crr_by_allocation():
    # 1M total: 400K cbBTC (el 2.0), 300K WETH (el 1.0), 300K idle at 0.
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000"), _alloc("WETH", "300000")),
        results_by_key={
            "morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0", n_mc=5000, forecast_step=30),
            "morpho_weth-usdc": _morpho_result("morpho_weth-usdc", "1.0", n_mc=100, forecast_step=14),
        },
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    # crr_el = (400K*2.0 + 300K*1.0) / 1M = 1.1
    assert result.comparable_crr_pct == Decimal("1.1000")
    assert result.rrc_usd == Decimal("110.00")  # 10000 * 1.1%
    details = result.details
    assert isinstance(details, CoreModelDetails)
    assert details.protocol == "MORPHO"
    assert details.hhi is None
    assert details.coverage_pct == Decimal("100.00")
    assert details.n_mc == 100  # min across slices
    assert details.forecast_step == 14  # min across slices
    assert details.copula_type == "T-COPULA"
    assert details.markets is not None
    assert [(m.market_key, m.allocation_pct) for m in details.markets] == [
        ("morpho_cbbtc-usdc", Decimal("40.00")),
        ("morpho_weth-usdc", Decimal("30.00")),
    ]
    assert details.markets[0].computed_at == _NOW


async def test_morpho_lltv_tranches_of_one_pair_merge_into_one_slice():
    # Two Blue markets share the cbBTC/USDC pair (different LLTVs): one CORE
    # market key, so their weights add into a single slice and a single read.
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000", market_id=1), _alloc("CBBTC", "200000", market_id=2)),
        results_by_key={"morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0")},
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    details = result.details
    assert isinstance(details, CoreModelDetails)
    assert details.markets is not None
    assert [(m.market_key, m.allocation_pct) for m in details.markets] == [
        ("morpho_cbbtc-usdc", Decimal("60.00")),
    ]
    # crr_el = 600K*2.0 / (600K + 400K idle)
    assert result.comparable_crr_pct == Decimal("1.2000")


async def test_morpho_coverage_equal_to_the_minimum_is_served():
    # covered = 400K + 300K idle = 70% with the threshold set exactly there.
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000"), _alloc("XAUT", "300000")),
        results_by_key={"morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0")},
        min_coverage_pct=Decimal("70"),
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    assert isinstance(result.details, CoreModelDetails)
    assert result.details.coverage_pct == Decimal("70.00")


async def test_morpho_uncovered_slice_is_excluded_and_reported_as_coverage():
    # 400K cbBTC covered, 300K XAUT with no computed market, 300K idle.
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000"), _alloc("XAUT", "300000")),
        results_by_key={"morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0")},
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    details = result.details
    assert isinstance(details, CoreModelDetails)
    # covered weight = 400K + 300K idle = 700K of 1M
    assert details.coverage_pct == Decimal("70.00")
    # crr_el = 400K*2.0 / 700K
    assert result.comparable_crr_pct == Decimal("1.1429")
    assert details.markets is not None
    assert len(details.markets) == 1


async def test_morpho_coverage_below_minimum_is_not_served():
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000"), _alloc("XAUT", "300000")),
        results_by_key={"morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0")},
        min_coverage_pct=Decimal("80"),
    )
    with pytest.raises(ModelDataUnavailableError, match="below the 80% minimum"):
        await svc.compute(_MORPHO_ASSET, _PRIME, {})


async def test_morpho_no_covered_market_is_not_served():
    svc = _morpho_service(
        vault=_vault(_alloc("XAUT", "400000")),
        results_by_key={},
    )
    with pytest.raises(ModelDataUnavailableError, match="coverage is 0%"):
        await svc.compute(_MORPHO_ASSET, _PRIME, {})


async def test_morpho_allocations_exceeding_stale_total_assets_stay_a_partition():
    # Position snapshots newer than the vault state: weights re-base on the
    # allocation sum so coverage stays <= 100 and idle >= 0.
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000"), _alloc("WETH", "300000"), total_assets="500000"),
        results_by_key={
            "morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0"),
            "morpho_weth-usdc": _morpho_result("morpho_weth-usdc", "1.0"),
        },
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    details = result.details
    assert isinstance(details, CoreModelDetails)
    assert details.coverage_pct == Decimal("100.00")
    # crr_el = (400K*2.0 + 300K*1.0) / 700K
    assert result.comparable_crr_pct == Decimal("1.5714")


async def test_morpho_collateral_symbols_match_case_insensitively():
    svc = _morpho_service(
        vault=_vault(_alloc("cbBTC", "400000"), _alloc("weth", "600000")),
        results_by_key={
            "morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0"),
            "morpho_weth-usdc": _morpho_result("morpho_weth-usdc", "1.0"),
        },
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    details = result.details
    assert isinstance(details, CoreModelDetails)
    assert details.coverage_pct == Decimal("100.00")


async def test_morpho_usd_exposure_override_applies():
    svc = _morpho_service(
        vault=_vault(_alloc("CBBTC", "400000"), _alloc("WETH", "300000")),
        results_by_key={
            "morpho_cbbtc-usdc": _morpho_result("morpho_cbbtc-usdc", "2.0"),
            "morpho_weth-usdc": _morpho_result("morpho_weth-usdc", "1.0"),
        },
        usd_exposure_error=ValueError("no position or price found"),
    )
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {"usd_exposure": "5000"})
    assert result.rrc_usd == Decimal("55.00")  # 5000 * 1.1%


async def test_morpho_unknown_vault_is_not_served():
    svc = _morpho_service(vault=None)
    with pytest.raises(ModelDataUnavailableError, match="no indexed morpho vault"):
        await svc.compute(_MORPHO_ASSET, _PRIME, {})


async def test_morpho_missing_receipt_token_record_is_not_served():
    svc = _morpho_service(receipt_token=None)
    with pytest.raises(ModelDataUnavailableError, match="no receipt-token record"):
        await svc.compute(_MORPHO_ASSET, _PRIME, {})


async def test_morpho_static_mapping_wins_over_vault_aggregation():
    # An asset in both shapes serves the 1:1 market, not the vault aggregate.
    svc = _morpho_service(asset_to_market_key={_MORPHO_ASSET: "sparklend_usdc"})
    result = await svc.compute(_MORPHO_ASSET, _PRIME, {})
    assert isinstance(result.details, CoreModelDetails)
    assert result.details.protocol == "SPARKLEND"


# ---------------------------------------------------------------------------
# morpho_market_key_index
# ---------------------------------------------------------------------------


def test_morpho_market_key_index_indexes_morpho_pairs_uppercased():
    configs = {
        "morpho_cbbtc-usdc": {"PROTOCOL": "MORPHO", "MORPHO_MARKET": "cbBTC", "LOAN_TOKEN": "usdc"},
        "sparklend_usdc": {"PROTOCOL": "SPARKLEND", "LOAN_TOKEN": "USDC"},
    }
    assert morpho_market_key_index(configs) == {("CBBTC", "USDC"): "morpho_cbbtc-usdc"}


def test_morpho_market_key_index_rejects_duplicate_pairs():
    configs = {
        "morpho_a": {"PROTOCOL": "MORPHO", "MORPHO_MARKET": "CBBTC", "LOAN_TOKEN": "USDC"},
        "morpho_b": {"PROTOCOL": "MORPHO", "MORPHO_MARKET": "cbbtc", "LOAN_TOKEN": "usdc"},
    }
    with pytest.raises(ValueError, match="duplicate MORPHO market pair"):
        morpho_market_key_index(configs)


def test_morpho_market_key_index_indexes_the_packaged_market_configs():
    """The real market_configs.json parses and carries the two live Blue pairs."""
    from pathlib import Path

    from app.risk_engine.core_model.config import INPUTS_DIR, load_commented_json

    index = morpho_market_key_index(load_commented_json(Path(INPUTS_DIR) / "market_configs.json"))
    assert index[("CBBTC", "USDC")] == "morpho_cbbtc-usdc"
    assert index[("WETH", "USDC")] == "morpho_weth-usdc"
