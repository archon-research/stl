# ============================================================
# Forecaster: ARMA-GARCH-based price and volatility forecasting framework.

# This class provides functionality to forecast financial time series using
# a combination of mean models (ARMA/ARIMA) and volatility models (GARCH/EGARCH),
# optionally including jumps. Forecasts can be generated for log or simple returns
# over a specified horizon, and can be used to simulate future price paths.
# ============================================================

import warnings
from typing import Optional, Union

import numpy as np
import pandas as pd
from arch import arch_model
from arch.univariate.base import ARCHModelResult
from joblib import Parallel, delayed
from scipy.stats import norm, t
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults
from tqdm import tqdm

from app.risk_engine.core_model.aggregator import Aggregator
from app.risk_engine.core_model.backtester import Backtester
from app.risk_engine.core_model.calibrator import Calibrator

warnings.simplefilter(action="ignore", category=FutureWarning)


def compute_lindy_factor(
    n_obs: int,
    alpha: float = 0.0,
    ref_days: int = 1825,
    max_factor: float = 2.0,
) -> float:
    """
    Lindy volatility scaling factor — uncertainty premium for assets with short price histories.

    factor = min(max_factor,  max(1.0,  (ref_days / n_obs) ^ alpha))

    Rationale
    ---------
    GARCH parameters estimated on a short history carry wide confidence intervals,
    and the observed sample may cover only a single market regime (e.g. a bull run).
    The Lindy factor compensates by scaling up the conditional vol forecast, making
    capital requirements more conservative when data is sparse.

    Parameters
    ----------
    n_obs       : number of daily price observations available for calibration
    alpha       : decay exponent; 0.0 = disabled (returns 1.0 always)
                  0.5 = square-root — recommended starting point
    ref_days    : history length at which factor = 1.0 (default: 1825 = ~5 years)
    max_factor  : hard cap; prevents extreme inflation for very new assets

    Examples (alpha=0.5, ref_days=1825, max_factor=2.0)
    -------------------------------------------------------
    BTC / ETH   (≥5 y, ≥1825 obs) → 1.00
    SOL         (~3 y, ~1095 obs) → 1.29
    WIF / PENDLE (~2 y, ~730 obs) → 1.58
    HYPE        (~6 m, ~180 obs)  → 2.00  (capped)
    brand-new   (<3 m, <90 obs)   → 2.00  (capped)
    """
    if alpha == 0.0 or n_obs >= ref_days:
        return 1.0
    raw = (ref_days / max(1, n_obs)) ** alpha
    return float(min(max_factor, max(1.0, raw)))


class Forecaster:
    def __init__(
        self,
        arma_model: ARIMAResults,
        garch_model: ARCHModelResult,
        seed: int,
        use_brownian_bridge: Optional[bool] = False,
        vol_floor: Optional[float] = None,
        lindy_factor: float = 1.0,
    ):
        self.arma_model = arma_model
        self.garch_model = garch_model
        self.use_brownian_bridge = use_brownian_bridge
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        # Volatility floor: prevents the model from producing unrealistically
        # narrow price distributions when GARCH adapts to a low-vol regime.
        # Computed externally from the full historical series so it is stable
        # and independent of the training window choice.
        self.vol_floor = vol_floor

        # Lindy factor: uncertainty premium applied to the conditional vol
        # forecast for assets with short price histories (see compute_lindy_factor).
        # 1.0 = no adjustment (default; corresponds to LINDY_ALPHA=0.0).
        self.lindy_factor = lindy_factor

    def _sample_norm(self, size: int) -> np.ndarray:
        return self.rng.standard_normal(size)

    def _sample_t(self, df: float, size: int) -> np.ndarray:
        return self.rng.standard_t(df, size)

    def _sample_poisson(self, lam: float, size: int) -> np.ndarray:
        return self.rng.poisson(lam, size)

    def _inverse_cdf_transform(self, u: pd.DataFrame) -> np.ndarray:
        """
        u must be in (0,1), shape = (N_MC, step)
        """
        dist_name, df = self._get_garch_distribution()

        if dist_name == "t":
            return t.ppf(u, df=df)
        else:
            return norm.ppf(u)

    def _get_garch_distribution(self) -> tuple[str, any]:
        """
        Safely detects the GARCH innovation distribution and its parameters.
        Returns: (dist_name, df)
        """

        try:
            dist = self.garch_model.model.distribution
            name = dist.name.lower()

            if "student" in name or "t" in name:
                # Read nu from the *fitted* parameter vector, not the unfitted
                # distribution object (which never has the estimated value).
                params = self.garch_model.params
                df = params.get("nu", params.get("df", None))
                if df is None:
                    df = 10.0  # hard fallback
                return "t", float(df)

            else:
                return "normal", None

        except Exception:
            # Absolute fallback if anything breaks
            return "normal", None

    def returns_forecasting(
        self,
        step: int,
        prices: pd.Series,
        correlated_uniform: pd.Series,
        jump_params: Optional[Union[dict, int]],
        use_log_return: bool,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """
        Combines ARMA mean forecasts, GARCH volatility forecasts,
        and a Poisson jump component for the next `step` periods.

        jump_params: dict with keys
            - "lambda": Poisson intensity per step
            - "jump_mean": mean of jump size distribution
            - "jump_std": std of jump size distribution
        """
        pct_returns = prices.pct_change().dropna()
        log_returns = np.log(prices / prices.shift(1)).dropna()
        returns = log_returns if use_log_return else pct_returns

        if self.arma_model is None and self.garch_model is None:
            raise ValueError("No ARMA-GARCH model fitted. No forecast possible!")
        # 1. ARMA mean forecast
        if self.arma_model is not None:
            arma_forecast = self.arma_model.get_forecast(steps=step)
            mean_forecast = arma_forecast.predicted_mean
        else:
            mean_forecast = pd.Series(0, index=pd.RangeIndex(step))

        # 2. GARCH conditional volatility forecast
        if self.garch_model is not None:
            try:
                garch_forecast = self.garch_model.forecast(horizon=step)
            except Exception:
                garch_forecast = self.garch_model.forecast(horizon=step, method="simulation", reindex=False)

            vol_forecast = np.sqrt(garch_forecast.variance.iloc[-1].values) / 100
            vol_forecast = pd.Series(vol_forecast, index=mean_forecast.index)

            if self.vol_floor is not None:
                vol_forecast = vol_forecast.clip(lower=self.vol_floor)

            # Lindy factor: scale up vol for assets with short price history.
            # factor = 1.0 when LINDY_ALPHA = 0.0 (disabled) → no-op.
            if self.lindy_factor != 1.0:
                vol_forecast = vol_forecast * self.lindy_factor

            if hasattr(self.garch_model.model, "distribution"):
                dist = self.garch_model.model.distribution
                if dist.name.lower().startswith("student"):
                    # Read nu from the *fitted* parameter vector (same fix as _get_garch_distribution)
                    _params = self.garch_model.params
                    df = _params.get("nu", _params.get("df", 10.0))
                    random_innovations = pd.Series(t.ppf(correlated_uniform, df=float(df)), index=mean_forecast.index)
                else:
                    random_innovations = pd.Series(norm.ppf(correlated_uniform), index=mean_forecast.index)

            else:
                random_innovations = pd.Series(norm.ppf(correlated_uniform), index=mean_forecast.index)

        else:
            hist_vol = returns.squeeze().std()
            vol_forecast = pd.Series(hist_vol, index=mean_forecast.index)
            random_innovations = pd.Series(norm.ppf(correlated_uniform), index=mean_forecast.index)

        innovations = random_innovations.copy()

        # 4. Poisson jump process
        if jump_params is not None:
            hours = 24
            step_jumps = step if not self.use_brownian_bridge else step * hours
            jump_occur = self._sample_poisson(jump_params["lambda"], step_jumps)
            jump_sizes = np.where(
                jump_occur > 0,
                self._sample_t(jump_params["df"], step_jumps) * jump_params["jump_std"] + jump_params["jump_mean"],
                0.0,
            )

            if self.use_brownian_bridge:
                jump_sizes = jump_sizes.reshape(step, hours)
                jump_series = pd.DataFrame(jump_sizes, index=mean_forecast.index, columns=range(hours))
            else:
                jump_series = pd.Series(jump_sizes, index=mean_forecast.index)

            crisis_cap = jump_params["crisis_cap"]
            # minimum_cap = 0.50 if not self.use_brownian_bridge else 0.15
            # crisis_cap = min(crisis_cap, minimum_cap)
            if jump_params.get("focus_on_negative", False):
                jump_series = np.clip(jump_series, -crisis_cap, 0.0)
            else:
                jump_series = np.clip(jump_series, -crisis_cap, crisis_cap)
        else:
            jump_series = pd.Series(0.0, index=mean_forecast.index)

        combined = mean_forecast + vol_forecast * innovations

        return (pd.Series(combined, index=mean_forecast.index), vol_forecast, jump_series)

    @staticmethod
    def brownian_bridge_hourly(
        daily_returns: pd.Series, daily_vol: pd.Series, jump_series: pd.Series, hours: int = 24, seed: int | None = 0
    ) -> pd.Series:

        hourly_returns = []

        for step_idx, t in enumerate(daily_returns.index):  # noqa: F402
            # Use the integer step position, not hash(t): hash() is non-deterministic
            # across Python processes (PYTHONHASHSEED), so the same seed would give
            # different Brownian bridge paths in different runs.
            rng = np.random.default_rng(None if seed is None else seed + step_idx)

            R_d = daily_returns.loc[t]
            sigma_d = daily_vol.loc[t]

            dt = 1 / hours

            # Brownian motion component
            Z = rng.standard_normal(hours)
            Z -= Z.mean()
            Z /= np.std(Z, ddof=0)

            r_cont_hourly = R_d / hours + sigma_d * np.sqrt(dt) * Z

            if np.isnan(r_cont_hourly).any() or np.isinf(r_cont_hourly).any():
                print(f"[BB] Bad hourly returns at {t}: R_d={R_d}, sigma_d={sigma_d}")
                r_cont_hourly = np.zeros(hours)  # fallback

            if jump_series is not None:
                hourly_jumps = jump_series.loc[t]  # shape (24,)
                r_cont_hourly = r_cont_hourly + hourly_jumps

            hourly_returns.extend(r_cont_hourly)

        return pd.Series(hourly_returns)

    def price_forecasting(
        self,
        prices_series: pd.Series,
        correlated_eps: pd.Series,
        jump_params: pd.DataFrame,
        use_log_returns: Optional[bool] = True,
        forecasted_step: Optional[int] = None,
        token_name: Optional[str] = None,
        *,
        market_df: Optional[pd.DataFrame] = pd.DataFrame(),
    ) -> pd.Series:

        if not forecasted_step:
            forecasted_step = 1

        # Step 1: Forecast
        forecasted_series, vol_forecast, jump_series = self.returns_forecasting(
            step=forecasted_step,
            prices=prices_series,
            correlated_uniform=correlated_eps,
            use_log_return=use_log_returns,
            jump_params=jump_params,
        )

        if self.use_brownian_bridge:
            forecasted_returns = self.brownian_bridge_hourly(
                daily_returns=forecasted_series,
                daily_vol=vol_forecast,
                jump_series=jump_series,
                hours=24,
                seed=self.seed,
            )
        else:
            forecasted_returns = forecasted_series.values + jump_series.values
            forecasted_returns = pd.Series(forecasted_returns, index=forecasted_series.index)

        # Step 2: Reconstruct price

        if (not market_df.empty) and (token_name in market_df["token_symbol"].values):
            last_price = market_df.loc[market_df["token_symbol"] == token_name, "oracle_price"].iloc[0]
        else:
            last_price = prices_series.iloc[-1].item()

        if use_log_returns:
            cumsum = forecasted_returns.cumsum()
            prices = last_price * np.exp(cumsum)

        else:
            prices = last_price * (1 + forecasted_returns).cumprod()

        prices_forecast = prices.reset_index(drop=True)
        prices_forecast.name = "Forecast"

        return prices_forecast


class Simulator:
    def __init__(
        self,
        price_series: pd.Series,
        arima_spec: dict,
        garch_spec: dict,
        seed: int,
        *,
        market_input: pd.DataFrame = pd.DataFrame(),
    ) -> None:

        self.price_series = price_series
        self.arima_spec = arima_spec
        self.garch_spec = garch_spec
        self.seed = seed

        self.market_input = market_input

    @staticmethod
    def calculate_pct_change_parameters(series: pd.Series) -> tuple[float, float]:

        pct_change = series.pct_change().dropna()
        negative_pct_change = pct_change[pct_change < 0.0].dropna()
        positive_pct_change = pct_change[pct_change > 0.0].dropna()

        negative_parameter = np.median(negative_pct_change)
        positive_parameter = np.median(positive_pct_change)

        return negative_parameter, positive_parameter

    def arma_garch_refitter(self, train_size: int, use_log_returns: bool) -> pd.Series:

        filtered_prices = self.price_series.iloc[-train_size:]

        f_returns, f_log_returns = Calibrator.calculate_returns(filtered_prices)
        filtered_returns = f_log_returns if use_log_returns else f_returns

        if self.arima_spec is not None:
            if all(k in self.arima_spec for k in ("p", "d", "q")):
                order = (self.arima_spec["p"], self.arima_spec["d"], self.arima_spec["q"])
                arima_refit_model = ARIMA(filtered_returns.squeeze(), order=order).fit()
                arima_residuals = arima_refit_model.resid
            else:
                raise ValueError("ARIMA spec is missing one of p,d,q")
        else:
            arima_refit_model = None
            arima_residuals = None

        if self.garch_spec is not None:
            if arima_residuals is not None:
                series = arima_residuals.copy()
            else:
                series = filtered_returns.copy()
            dist_name = Backtester.identify_dist(self.garch_spec["dist"])
            garch_refit_model = arch_model(
                series.squeeze() * 100,
                vol=self.garch_spec.get("vol", "GARCH"),
                p=self.garch_spec.get("p", 0),
                o=self.garch_spec.get("o", 0),
                q=self.garch_spec.get("q", 0),
                dist=dist_name,
                rescale=False,
            ).fit(update_freq=0, disp="off", options={"maxiter": 1000})
            garch_residuals = garch_refit_model.resid
        else:
            garch_refit_model = None
            garch_residuals = None

        residuals = garch_residuals
        if garch_residuals is None:
            residuals = arima_residuals

        return arima_refit_model, garch_refit_model, residuals

    @staticmethod
    def simulate_prices(
        result_per_token: dict,
        copula_type: str,
        forecasted_step: int,
        use_log_returns: bool,
        use_brownian_bridge: bool,
        jump_parameters: pd.DataFrame,
        n_sims: int,
        seed: int,
        market_df: pd.DataFrame,
        vol_floor_pct: Optional[float] = None,
        lindy_alpha: float = 0.0,
        lindy_ref_days: int = 1825,
        lindy_max_factor: float = 2.0,
    ) -> pd.DataFrame:

        if len(result_per_token) > 1:
            residuals_dict = {token: result_per_token[token]["residuals"] for token in result_per_token}
            residuals_df = pd.DataFrame(residuals_dict)

            aggregator = Aggregator(residuals_df, seed=seed)

            U_gaussian = aggregator.copula_aggregator(copula_type, n_sims, forecasted_step)

        else:
            # Single token → generate simple uniform(0,1) for each scenario and step.
            # Use a seeded generator so results are reproducible (legacy np.random.uniform
            # ignores the seed parameter entirely).
            token_name = list(result_per_token.keys())[0]
            _rng = np.random.default_rng(seed)
            U_single = _rng.uniform(0, 1, size=(n_sims, forecasted_step))
            U_gaussian = {token_name: pd.DataFrame(U_single)}

        print("\nRunning Monte-Carlo Simulations for Prices...\n")
        sim_prices = {}

        for token in result_per_token:
            arima_model = result_per_token[token]["arima_model"]
            garch_model = result_per_token[token]["garch_model"]
            price_series = result_per_token[token]["prices"]
            corr_residuals = U_gaussian[token]

            # Compute vol floor from the FULL historical series (not just the training window)
            # so the floor is independent of the window size choice.
            token_vol_floor = None
            if vol_floor_pct is not None:
                full_log_returns = np.log(price_series / price_series.shift(1)).dropna()
                rolling_vol = full_log_returns.rolling(21).std()
                token_vol_floor = float(np.percentile(rolling_vol.dropna(), vol_floor_pct * 100))

            # Per-token jump params take priority; fall back to the shared
            # jump_parameters argument for backwards compatibility with main.py.
            token_jump_params = result_per_token[token].get("jump_params", jump_parameters)

            # Lindy factor: uncertainty premium for assets with short price history.
            # Returns 1.0 when lindy_alpha=0.0 (disabled) → no change to behaviour.
            token_lindy_factor = compute_lindy_factor(
                n_obs=len(price_series),
                alpha=lindy_alpha,
                ref_days=lindy_ref_days,
                max_factor=lindy_max_factor,
            )

            def run_simulation(
                i, _vol_floor=token_vol_floor, _jump_params=token_jump_params, _lindy=token_lindy_factor
            ):
                local_seed = seed + i
                corr_eps_row = corr_residuals.iloc[i % len(corr_residuals), :]
                forecaster = Forecaster(arima_model, garch_model, local_seed, use_brownian_bridge, _vol_floor, _lindy)
                forecasted_prices = forecaster.price_forecasting(
                    jump_params=_jump_params,
                    correlated_eps=corr_eps_row,
                    prices_series=price_series,
                    use_log_returns=use_log_returns,
                    forecasted_step=forecasted_step,
                    token_name=token,
                    market_df=market_df,
                )
                return {"prices": forecasted_prices.values}

            sim_results = Parallel(jobs=4)(delayed(run_simulation)(i) for i in tqdm(range(n_sims)))

            all_prices = [res["prices"] if res is not None else np.full(forecasted_step, np.nan) for res in sim_results]

            forecasted_length = forecasted_step * 24 if use_brownian_bridge else forecasted_step
            sim_df_prices = pd.DataFrame(
                np.array(all_prices),
                index=[f"Scen_{i + 1}" for i in range(n_sims)],
                columns=[f"Step_{i + 1}" for i in range(forecasted_length)],
            )

            sim_prices[token] = sim_df_prices

        return sim_prices
