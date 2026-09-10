"""Golden regression test for the CORE model runner (VEC-769).

Runs the full pipeline — calibration, copula simulation, liquidation — on the
static parquet snapshots with a pinned seed, and asserts the exact metric
values a known-good build produced. Any change that moves the numbers fails
here, instead of relying on someone hand-running before/after CRR comparisons.

Lives in tests/integration/ because it runs GARCH calibration + Monte Carlo
against real parquet snapshots; test selection in this repo is path-based
(make test-unit runs tests/unit only), so placement is what excludes it
from the unit run.

Speed notes:
- Price history is truncated to TRAIN_SIZE + 20 rows so the rolling backtest
  has only 20 out-of-sample points instead of 4000+.
- The Morpho case has 1 collateral token to minimise GARCH grid-search cost;
  the Syrup case pays for its extra tokens with the copula + jumps coverage.

Updating the golden values after an INTENTIONAL model change:
1. Run this file; the assertion message shows expected vs actual.
2. Copy the actual values into _GOLDEN_CASES in the same PR as the model
   change, alongside the full-size before/after CRR table that justifies the
   move (the golden-run procedure: sparklend_dai at three MIN_BORROW_USD
   values plus one other market, N_MC=1000, SEED=0).
A golden update with no model change in the PR is a red flag: it means the
pipeline stopped being deterministic or an input snapshot changed.
"""

from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model import config as core_model_config
from app.risk_engine.core_model.config import load_params
from app.risk_engine.core_model.runner import CoreModelConfig, run

INPUTS_DIR = Path(core_model_config.INPUTS_DIR)

_TRAIN_SIZE = 180
_BACKTEST_ROWS = 20

# Metric agreement bound. The pipeline is bit-deterministic on one machine
# (verified: two full runs, identical output); the tolerance absorbs only
# cross-platform BLAS/optimizer noise, measured at ~1.2e-6 relative between
# macOS-arm64 and Linux-x86_64 CI — which also flips the last digit of the
# 6-dp-rounded CRRs (abs 1e-6, rel 2e-4 on the smallest metric). 1e-3 covers
# both with headroom while staying far below any real change: the smallest
# historical model fix moved CRRs ~30%, and a seed change moves them 2000x.
_REL_TOL = 1e-3


class _TruncatedReader(ParquetCoreModelDataReader):
    """Returns price history truncated to TRAIN_SIZE + BACKTEST_ROWS rows."""

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        df = await super().get_prices(collateral_list)
        return df.tail(_TRAIN_SIZE + _BACKTEST_ROWS)


# Two cases, chosen to cover the paths the known model bugs live on:
# - morpho: single token, no jumps. Its winning GARCH spec is FIGARCH with
#   Standardized Student's t innovations, so the C-05 fix (t draws instead of
#   the silent Gaussian fallback) must move these numbers.
# - syrup: five collateral tokens with JUMPS on (the production default), so
#   it exercises the cross-asset t-Copula, the jump calibration the C-08 fix
#   changes, and margin-call liquidations with non-zero losses (3 of 50
#   scenarios lose).
_GOLDEN_CASES = {
    "morpho_cbbtc_usdc": {
        "overrides": {
            "PROTOCOL": "MORPHO",
            "MORPHO_MARKET": "CBBTC",
            "LOAN_TOKEN": "USDC",
            "TRAIN_SIZE": _TRAIN_SIZE,
            "N_MC": 10,
            "FORECAST_STEP": 2,
            "LIQ_ANALYSIS": "YES",
            "JUMPS": False,
        },
        "expected": {
            "crr_el_pct": Decimal("0.005557"),
            "crr_es_pct": Decimal("0.055573"),
            "crr_var_pct": Decimal("0.055573"),
            "hhi": Decimal("0.096862"),
            "n_scenarios": 10,
            "n_loss_scenarios": 2,
            "n_catastrophic_scenarios": 0,
            "crr_el_se_pct": 0.005557297969860614,
            "crr_el_rel_se": 0.999984149705072,
        },
    },
    "syrup_usdc": {
        "overrides": {
            "PROTOCOL": "SYRUP",
            "LOAN_TOKEN": "USDC",
            "TRAIN_SIZE": _TRAIN_SIZE,
            "N_MC": 50,
            "FORECAST_STEP": 14,
            "LIQ_ANALYSIS": "YES",
            # JUMPS deliberately left at its default (true).
        },
        "expected": {
            "crr_el_pct": Decimal("0.605311"),
            "crr_es_pct": Decimal("13.138924"),
            "crr_var_pct": Decimal("4.932652"),
            "hhi": Decimal("14.069008"),
            "n_scenarios": 50,
            "n_loss_scenarios": 4,
            "n_catastrophic_scenarios": 4,
            "crr_el_se_pct": 0.43821062711199477,
            "crr_el_rel_se": 0.7239429028063681,
        },
    },
}


@pytest.mark.parametrize("market_key", _GOLDEN_CASES, ids=_GOLDEN_CASES.keys())
async def test_runner_reproduces_golden_metrics(market_key: str):
    case = _GOLDEN_CASES[market_key]
    params = load_params(overrides=case["overrides"])
    config = CoreModelConfig(market_key=market_key, params=params)
    expected = case["expected"]

    result = await run(config, _TruncatedReader(INPUTS_DIR), INPUTS_DIR)

    assert result.market_key == market_key
    assert result.n_mc == case["overrides"]["N_MC"]
    assert result.forecast_step == case["overrides"]["FORECAST_STEP"]
    assert result.computed_at is not None

    for metric in ("crr_el_pct", "crr_es_pct", "crr_var_pct", "hhi"):
        actual = getattr(result, metric)
        assert float(actual) == pytest.approx(float(expected[metric]), rel=_REL_TOL), (
            f"{metric}: expected {expected[metric]}, got {actual}"
        )

    diagnostics = result.mc_diagnostics
    assert diagnostics.n_scenarios == expected["n_scenarios"]
    assert diagnostics.n_loss_scenarios == expected["n_loss_scenarios"]
    assert diagnostics.n_catastrophic_scenarios == expected["n_catastrophic_scenarios"]
    assert diagnostics.crr_el_se_pct == pytest.approx(expected["crr_el_se_pct"], rel=_REL_TOL)
    assert diagnostics.crr_el_rel_se == pytest.approx(expected["crr_el_rel_se"], rel=_REL_TOL)
