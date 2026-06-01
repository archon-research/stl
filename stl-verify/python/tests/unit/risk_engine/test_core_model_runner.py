"""Smoke test for the CORE model runner.

Marked as integration because it runs GARCH calibration + Monte Carlo
against real parquet snapshots. Excluded from the standard unit test run.

Run explicitly with:
    pytest tests/unit/risk_engine/test_core_model_runner.py -v -m integration
"""

from decimal import Decimal
from pathlib import Path

import pytest

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model.config import load_params
from app.risk_engine.core_model.runner import CoreModelConfig, run

INPUTS_DIR = Path(__file__).resolve().parents[3] / "app" / "risk_engine" / "core_model" / "inputs"

pytestmark = pytest.mark.integration


@pytest.mark.skipif(not INPUTS_DIR.exists(), reason="core_model inputs not present")
async def test_runner_returns_valid_result():
    """Full pipeline smoke test with tiny N_MC=10, FORECAST_STEP=2."""
    params = load_params(overrides={
        "PROTOCOL": "SPARKLEND",
        "LOAN_TOKEN": "USDC",
        "N_MC": 10,
        "FORECAST_STEP": 2,
        "LIQ_ANALYSIS": "YES",
        "JUMPS": False,
    })
    config = CoreModelConfig(market_key="sparklend_usdc", params=params)
    data_reader = ParquetCoreModelDataReader(INPUTS_DIR)

    result = await run(config, data_reader, INPUTS_DIR)

    assert result.market_key == "sparklend_usdc"
    assert result.crr_el_pct >= Decimal("0")
    assert result.forecast_step == 2
    assert result.n_mc == 10
    assert result.copula_type == "T-COPULA"
    assert result.computed_at is not None
