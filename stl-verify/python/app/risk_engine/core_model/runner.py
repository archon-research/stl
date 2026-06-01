"""CORE pipeline runner.

Pure orchestration function: accepts a config + data reader, runs
calibration + simulation + liquidation, returns per-market CRR metrics.

The runner temporarily changes the working directory to ``inputs_dir``
before instantiating ``Liquidator``. This is required because Liquidator
calls ``importer.load_orderbook_data()`` which uses CWD-relative paths
(a known constraint of the original codebase -- see README.md).
"""

from __future__ import annotations

import json
import os
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from app.risk_engine.core_model import importer
from app.risk_engine.core_model.calibrator import Calibrator
from app.risk_engine.core_model.forecaster import Simulator
from app.risk_engine.core_model.liquidator import Liquidator

if TYPE_CHECKING:
    from app.ports.core_model_data_reader import CoreModelDataReader

warnings.filterwarnings("ignore", category=FutureWarning)


@dataclass(frozen=True)
class CoreModelConfig:
    """Configuration for a single CORE pipeline run.

    ``market_key`` is the identifier written to ``core_model_results``.
    ``params`` is a flat dict from ``config.load_params(overrides=...)``.
    """

    market_key: str
    params: dict


@dataclass(frozen=True)
class CoreModelPipelineResult:
    market_key: str
    crr_el_pct: Decimal
    crr_es_pct: Decimal
    crr_var_pct: Decimal
    hhi: Decimal | None
    protocol: str
    forecast_step: int
    n_mc: int
    copula_type: str
    computed_at: datetime


@contextmanager
def _chdir(path: Path):
    """Temporarily change the working directory."""
    orig = Path.cwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(orig)


def _load_protection_usd(protocol: str, inputs_dir: Path) -> float:
    """Read inputs/protocol_defense.json and return total USD protection for the protocol."""
    defense_path = inputs_dir / "protocol_defense.json"
    try:
        with open(defense_path) as f:
            data = json.load(f)
        return float(data.get(protocol.upper(), {}).get("total_protection_usd", 0))
    except Exception:
        return 0.0


async def run(
    config: CoreModelConfig,
    data_reader: CoreModelDataReader,
    inputs_dir: Path,
) -> CoreModelPipelineResult:
    """Run the full CORE pipeline and return per-market CRR metrics.

    Mirrors the logic of ``main.py`` in the original CORE repository with
    data loading delegated to ``data_reader`` and the result returned as a
    typed dataclass instead of being printed to stdout.
    """
    p = config.params

    users_df, market_df = await data_reader.get_protocol_data(
        protocol=p["PROTOCOL"],
        network=p["NETWORK"],
        morpho_market=p["MORPHO_MARKET"],
        loan_token=p["LOAN_TOKEN"],
        galaxy_type=p["GALAXY_TYPE"],
    )

    if p["WORST_CASE"]:
        users_df = importer.change_user_ltvs(users_df, market_df)

    collateral_list = market_df["token_symbol"].unique()
    prices_df = await data_reader.get_prices(collateral_list)

    results = {}
    # TODO(bug#5): JUMP_PARAMS is calibrated from one token and reused for all.
    # Per-token path exists in forecaster.py but is never populated here.
    JUMP_PARAMS = None

    for collateral in collateral_list:
        TICKER = collateral.upper()

        prices = prices_df[collateral].dropna()
        prices.name = TICKER

        calibrator = Calibrator(price_series=prices, seed=p["SEED"])
        best_arima_fitted, best_garch_fitted, arima_spec, garch_spec = calibrator.total_fitter(
            use_log_returns=p["USE_LOG_RETURNS"],
            use_arma_model=False,
            use_vol_model=True,
            train_size=p["TRAIN_SIZE"],
            forecast_step=p["FORECAST_STEP"],
        )
        if best_garch_fitted is None:
            best_arima_fitted, best_garch_fitted, arima_spec, garch_spec = calibrator.total_fitter(
                use_log_returns=p["USE_LOG_RETURNS"],
                use_arma_model=True,
                use_vol_model=True,
                train_size=p["TRAIN_SIZE"],
                forecast_step=p["FORECAST_STEP"],
            )

        if p["JUMPS"]:
            if p["HOURLY_CONV"]:
                # TODO: importer.load_data_yahoo is not implemented (yfinance not
                # a service dependency). JUMPS + HOURLY_CONV requires a data source.
                prices_df_hourly, _ = importer.load_data_yahoo(
                    ticker=TICKER,
                    period="max",
                    time_interval="1h",
                )
                prices_jumps = prices_df_hourly["Close"]
                prices_jumps.name = TICKER
            else:
                prices_jumps = prices.copy()
            returns, log_returns = Calibrator.calculate_returns(prices_jumps)
            all_returns = log_returns if p["USE_LOG_RETURNS"] else returns
            JUMP_PARAMS = Calibrator.fit_poisson_intensity(
                hist_series=all_returns,
                lower_q=0.025,
                upper_q=0.975,
                focus_on_negative=p["FOCUS_ON_NEGATIVE"],
            )
            JUMP_PARAMS["focus_on_negative"] = p["FOCUS_ON_NEGATIVE"]

        simulator = Simulator(prices, arima_spec, garch_spec, p["SEED"])
        arima_model, garch_model, residuals = simulator.arma_garch_refitter(
            p["TRAIN_SIZE"],
            p["USE_LOG_RETURNS"],
        )

        results[TICKER] = {
            "token": TICKER,
            "prices": prices,
            "arima_model": arima_model,
            "garch_model": garch_model,
            "residuals": residuals,
        }

    all_simulated_prices = Simulator.simulate_prices(
        result_per_token=results,
        copula_type=p["COPULA_TYPE"],
        forecasted_step=p["FORECAST_STEP"],
        use_log_returns=p["USE_LOG_RETURNS"],
        use_brownian_bridge=p["HOURLY_CONV"],
        jump_parameters=JUMP_PARAMS,
        n_sims=p["N_MC"],
        seed=p["SEED"],
        market_df=market_df,
        vol_floor_pct=p["VOL_FLOOR_PCT"],
    )

    protection_usd = _load_protection_usd(p["PROTOCOL"], inputs_dir)

    # Liquidator.__init__ calls importer.load_orderbook_data() which uses
    # CWD-relative paths -- temporarily chdir to inputs_dir to satisfy this.
    with _chdir(inputs_dir):
        init_positions = Liquidator(borrowers_df=users_df, market_df=market_df)

    liq_results = init_positions.simulate_liquidations(
        all_prices=all_simulated_prices,
        product=p["PROTOCOL"],
        swap_fee=p["SWAP_FEE_USD"],
        gas_fee_usd=p["GAS_FEE_USD"],
        perc=p["PERC"],
        protection_usd=protection_usd,
        margin_call_trigger=p["MC_TRIGGER"],
        margin_call_target_ltv=p["MC_TARGET_LTV"],
        margin_call_cure_prob=p["MC_CURE_PROB"],
    )

    return CoreModelPipelineResult(
        market_key=config.market_key,
        crr_el_pct=Decimal(str(liq_results["crr_el"])),
        crr_es_pct=Decimal(str(liq_results["crr_es"])),
        crr_var_pct=Decimal(str(liq_results["crr_var"])),
        hhi=Decimal(str(liq_results["hhi"])) if liq_results["hhi"] is not None else None,
        protocol=p["PROTOCOL"],
        forecast_step=int(p["FORECAST_STEP"]),
        n_mc=int(p["N_MC"]),
        copula_type=p["COPULA_TYPE"],
        computed_at=datetime.now(UTC),
    )
