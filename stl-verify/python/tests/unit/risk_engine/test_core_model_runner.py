"""Smoke test for the CORE model runner.

Marked as integration because it runs GARCH calibration + Monte Carlo
against real parquet snapshots. Excluded from the standard unit test run.

Run explicitly with:
    pytest tests/unit/risk_engine/test_core_model_runner.py -v -m integration

Speed notes:
- Uses Morpho CBBTC-USDC (1 collateral token) to minimise GARCH grid-search cost.
- Price history is truncated to TRAIN_SIZE + 20 rows so the rolling backtest
  has only 20 out-of-sample points instead of 4000+.
"""

from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model.config import load_params
from app.risk_engine.core_model.runner import CoreModelConfig, run

INPUTS_DIR = Path(__file__).resolve().parents[3] / "app" / "risk_engine" / "core_model" / "inputs"

pytestmark = pytest.mark.integration

_TRAIN_SIZE = 180
_BACKTEST_ROWS = 20


class _TruncatedReader(ParquetCoreModelDataReader):
    """Returns price history truncated to TRAIN_SIZE + BACKTEST_ROWS rows."""

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        df = await super().get_prices(collateral_list)
        return df.tail(_TRAIN_SIZE + _BACKTEST_ROWS)


@pytest.mark.skipif(not INPUTS_DIR.exists(), reason="core_model inputs not present")
async def test_runner_returns_valid_result():
    """Full pipeline smoke test: 1 token, truncated price history, N_MC=10."""
    params = load_params(
        overrides={
            "PROTOCOL": "MORPHO",
            "MORPHO_MARKET": "CBBTC",
            "LOAN_TOKEN": "USDC",
            "TRAIN_SIZE": _TRAIN_SIZE,
            "N_MC": 10,
            "FORECAST_STEP": 2,
            "LIQ_ANALYSIS": "YES",
            "JUMPS": False,
        }
    )
    config = CoreModelConfig(market_key="morpho_cbbtc_usdc", params=params)
    data_reader = _TruncatedReader(INPUTS_DIR)

    result = await run(config, data_reader, INPUTS_DIR)

    assert result.market_key == "morpho_cbbtc_usdc"
    assert result.crr_el_pct >= Decimal("0")
    assert result.forecast_step == 2
    assert result.n_mc == 10
    assert result.computed_at is not None
