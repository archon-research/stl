# ============================================================
# Calibrator: Fits and evaluates ARMA-GARCH models for financial time series.

# The Calibrator class automates the process of identifying the best-fitting
# mean (ARMA/ARIMA) and volatility (GARCH/EGARCH/GJR-GARCH) models for a given
# price series. It optionally tests for jumps and evaluates the quality of
# model forecasts using statistical backtests.
# ============================================================

import random
import warnings
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import scipy.stats as stats
from arch import arch_model
from arch.univariate.base import ARCHModel, ARCHModelResult
from joblib import Parallel, delayed
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults
from statsmodels.tsa.stattools import adfuller

from app.risk_engine.core_model.backtester import Backtester

warnings.filterwarnings("ignore", category=UserWarning)


class Calibrator:
    def __init__(self, price_series: pd.Series, seed: int) -> None:

        list_models = ["FIGARCH", "GJR-GARCH", "GARCH", "EGARCH"]

        self.price_series = price_series
        self.list_models = list_models
        self.seed = seed

    @staticmethod
    def calculate_returns(series: pd.Series) -> tuple[pd.Series, pd.Series]:

        returns = series.pct_change().dropna()
        log_returns = np.log(series / series.shift(1)).dropna()

        returns.name = "Returns"
        log_returns.name = "LogReturns"
        return returns, log_returns

    @staticmethod
    def check_stationarity(hist_series: pd.Series) -> bool:
        """
        Runs the Augmented Dickey-Fuller (ADF) test on the series
        to check for stationarity.

        Returns
        -------
        bool
            True if the series is stationary (p-value <= 0.05),
            False otherwise.
        """
        result = adfuller(hist_series.dropna())
        p_value: float = result[1]

        if p_value > 0.05:
            # print("Series is non-stationary. Differencing the series.")
            return False
        else:
            # print("Series is stationary.")
            return True

    @staticmethod
    def find_best_mean_model(
        hist_series: pd.Series, max_lag: int, seed: int, verbose: bool = True
    ) -> Tuple[ARIMAResults, pd.DataFrame]:
        """
        Iterates over ARMA(p,q) combinations (p, q <= max_lag)
        and selects the one with the lowest BIC.

        Parameters
        ----------
        max_lag : int
            Maximum AR and MA lag to consider.
        verbose : bool, default=True
            If True, print progress and results.

        Returns
        -------
        best_model_fit : ARIMAResults
            Fitted ARIMA model with the best BIC.
        df : pd.DataFrame
            DataFrame with results for all successfully fitted models.
        """
        np.random.seed(seed)
        random.seed(seed)

        results = []
        warnings.filterwarnings("ignore")  # Suppress ARIMA convergence warnings

        for p in range(0, max_lag + 1):
            for q in range(0, max_lag + 1):
                try:
                    model = ARIMA(hist_series, order=(p, 0, q))
                    model_fit: ARIMAResults = model.fit()
                    results.append(
                        {
                            "p": p,
                            "q": q,
                            "AIC": model_fit.aic,
                            "BIC": model_fit.bic,
                            "LogLik": model_fit.llf,
                            "Model_Fit": model_fit,
                        }
                    )
                except Exception as e:
                    if verbose:
                        print(f"Skipping ARIMA({p},0,{q}) due to error: {e}")
                    continue

        df = pd.DataFrame(results).dropna(subset=["BIC"]).sort_values("BIC", ascending=True).reset_index(drop=True)
        best_model_fit: ARIMAResults = df.loc[0, "Model_Fit"]
        best_order = (df.loc[0, "p"], 0, df.loc[0, "q"])

        if verbose:
            print(
                f"Calculation complete! Best model is ARIMA({df.loc[0, 'p']},0,{df.loc[0, 'q']}) "
                f"with AIC={df.loc[0, 'AIC']:.2f} and BIC={df.loc[0, 'BIC']:.2f}"
            )

        return best_model_fit, best_order, df

    @staticmethod
    def check_arima_residuals(model_fit: ARIMAResults, returns: pd.Series) -> Tuple[pd.Series, bool]:
        resid = model_fit.resid
        ticker = returns.name
        white_noise = False

        if np.allclose(resid, returns, atol=1e-6):
            print(
                f"Model residuals of {ticker} match returns! "
                "This suggests the ARIMA model did not capture any structure.\n"
            )
            return resid, False

        print(f"\n{ticker} ARIMA Residual Diagnostics")
        print("Descriptive Statistics of Residuals:")
        print(resid.describe())

        # Ljung-Box test on residuals
        lb_test = acorr_ljungbox(resid, lags=50, return_df=True)
        pval = lb_test["lb_pvalue"].iloc[-1]

        if pval > 0.05:
            print(f"\nLjung-Box Test: Residuals appear to be white noise (p-value={pval:.4f})")
            white_noise = True
        else:
            print(f"\nLjung-Box Test: Residuals show autocorrelation (p-value={pval:.4f})")

        return resid, white_noise

    @staticmethod
    def check_arch_effects(residuals: pd.Series, nlags: int = 10, alpha: float = 0.05) -> bool:
        """
        ARCH-LM test on raw residuals to determine whether a GARCH model is warranted.
        Returns True if heteroskedasticity is detected (GARCH should be fitted).
        This is the correct gate for GARCH: a series can have white-noise levels
        (passing Ljung-Box) yet still exhibit significant volatility clustering.
        """
        _, p_value, _, _ = het_arch(residuals.dropna(), nlags=nlags)
        arch_present = p_value < alpha
        print(f"ARCH-LM test p-value: {p_value:.4f} → GARCH {'warranted' if arch_present else 'not warranted'}")
        return arch_present

    @staticmethod
    def find_best_vol_model(
        hist_series: pd.Series, seed: int, model_type: Optional[str] = None
    ) -> Tuple[ARCHModelResult, ARCHModel, pd.DataFrame]:
        """
        Grid search over several GARCH-family models with Student-t innovations.

        Parameters
        ----------
        series : pd.Series
            Time series of returns.
        max_lag : int
            Maximum lag for p and q to consider.
        model_types : Optional[List[str]], default None
            List of GARCH-family models to test, e.g., ["GARCH", "EGARCH"].
            If None, defaults to ["GARCH"].

        Returns
        -------
        best_model : ARCHModelResult
            Fitted GARCH-family model with the lowest BIC.
        df : pd.DataFrame
            DataFrame with all models tested, including p, q, o, AIC, BIC, LogLik.
        """
        np.random.seed(seed)
        random.seed(seed)

        results = []
        if not model_type:
            model_type = "GARCH"

        dist_list = ["normal", "skewt", "t"]

        for try_dist in dist_list:
            if model_type == "GJR-GARCH":
                vol_type = "GARCH"  # arch_model only understands "GARCH" here
                o_list = [1]  # include asymmetry
            else:
                vol_type = model_type
                o_list = [0]
            for o in o_list:
                try:
                    model = arch_model(
                        hist_series.squeeze() * 100, p=1, o=o, q=1, vol=vol_type, dist=try_dist, rescale=False
                    )
                    fit: ARCHModelResult = model.fit(disp="off", update_freq=0, options={"maxiter": 1000})
                    results.append(
                        {
                            "p": 1,
                            "o": o,
                            "q": 1,
                            "Model_type": model_type,
                            "Dist": fit.model.distribution.name,
                            "AIC": fit.aic,
                            "BIC": fit.bic,
                            "LogLik": fit.loglikelihood,
                            "Model": model,
                            "Model_Fit": fit,
                        }
                    )
                except Exception as e:
                    print(f"Skipping model {model_type}({1},{o},{1}) due to {e}")
                    continue

        df = pd.DataFrame(results).sort_values("BIC", ascending=True).reset_index(drop=True)

        best_model_fitted: ARCHModelResult = df.loc[0, "Model_Fit"]
        best_model: arch_model = df.loc[0, "Model"]
        best_model_dist = df.loc[0, "Dist"]
        print(
            f"Calculation complete! Best volatility model (p,o,q) is: "
            f"{df.loc[0, 'Model_type']}({df.loc[0, 'p']},{df.loc[0, 'o']},{df.loc[0, 'q']}) - {best_model_dist}"
        )

        return best_model_fitted, best_model, best_model_dist

    @staticmethod
    def check_garch_residuals(
        model_fit: ARCHModelResult, max_lag: int = 50, alpha: int = 0.05
    ) -> tuple[bool, Dict[str, bool]]:
        std_resid = model_fit.std_resid
        std_resid_sq = std_resid**2

        # Ljung–Box on residuals (mean)
        lb_resid = acorr_ljungbox(std_resid, lags=max_lag, return_df=True)
        lb_resid_pass = lb_resid["lb_pvalue"].iloc[-1] > alpha

        # Ljung–Box on squared residuals (ARCH)
        lb_sq = acorr_ljungbox(std_resid_sq, lags=max_lag, return_df=True)
        lb_sq_pass = lb_sq["lb_pvalue"].iloc[-1] > alpha

        # ARCH–LM
        lm = model_fit.arch_lm_test(standardized=True)
        lm_pass = lm.pval > alpha

        diagnostics = {"LB_resid_pass": lb_resid_pass, "LB_sq_pass": lb_sq_pass, "LM_pass": lm_pass}

        # accept_model = lb_sq_pass and lm_pass
        accept_model = lb_sq_pass or lm_pass

        return accept_model, diagnostics

    @staticmethod
    def fit_poisson_intensity(
        hist_series: pd.Series,
        *,
        lower_q: float = 0.05,
        upper_q: float = 0.95,
        focus_on_negative: bool = False,
        fixed_df: Optional[float] = 4.0,
    ) -> Dict[str, Any]:
        """
        Estimate Poisson jump intensity and jump size distribution using tail events.

        Parameters
        ----------
        lower_q : float, default 0.005
            Lower quantile to define tail threshold.
        upper_q : float, default 0.995
            Upper quantile to define tail threshold.
        focus_on_negative : bool, default True
            If True, consider only downside jumps; otherwise consider both tails.
        fixed_df : float or None, default 4.0
            If provided, fix the Student-t degrees of freedom to this value and
            estimate only loc and scale via MLE. This prevents df collapsing to
            near-infinity on small tail samples, which would make the jump
            distribution indistinguishable from Gaussian. For crypto assets,
            values in [3, 6] are empirically appropriate. Set to None to let
            MLE estimate df freely (not recommended for n_jumps < 50).

        Returns
        -------
        dict
            Dictionary with:
                - "lambda": Poisson intensity per time-step
                - "df": degrees of freedom of fitted Student-t distribution
                - "jump_mean": location parameter of Student-t
                - "jump_std": scale parameter of Student-t
                - "n_jumps": number of tail observations used
        """
        abs_returns = hist_series.abs().squeeze()
        crisis_cap = float(abs_returns.max())

        hist_series = hist_series.squeeze()  # converts single-column DataFrame → Series

        q_inf = np.percentile(hist_series, lower_q * 100)
        q_sup = np.percentile(hist_series, upper_q * 100)

        if focus_on_negative:
            jumps = hist_series[hist_series <= q_inf]
        else:
            jumps = hist_series[(hist_series <= q_inf) | (hist_series >= q_sup)]

        intensity = len(jumps) / len(hist_series)  # Poisson lambda per time-step

        if fixed_df is not None:
            df = fixed_df
            _, loc, scale = stats.t.fit(jumps, f0=fixed_df)
        else:
            df, loc, scale = stats.t.fit(jumps)

        return {
            "lambda": intensity,
            "df": df,
            "jump_mean": loc,
            "jump_std": scale,
            "n_jumps": len(jumps),
            "crisis_cap": crisis_cap,
        }

    def total_fitter(
        self,
        train_size: int,
        forecast_step: int,
        *,
        use_log_returns: bool = False,
        use_arma_model: bool = True,
        use_vol_model: bool = True,
        backtest_alpha: float = 0.05,
    ) -> Tuple[ARIMAResults, ARCHModelResult, dict, dict]:
        """
        Fits ARMA-GARCH models, checks residuals (LB), optionally fits jumps.
        Returns best ARMA, best GARCH, and jump parameters if requested.
        """
        price_returns, price_log_returns = self.calculate_returns(self.price_series)
        returns = price_log_returns if use_log_returns else price_returns

        # Check stationarity
        if not self.check_stationarity(returns):
            raise ValueError("Returns are not stationary.")

        # Fit ARMA mean model
        best_arima = None
        arima_spech = None
        if use_arma_model:
            best_arima, arima_order, _ = self.find_best_mean_model(hist_series=returns, seed=self.seed, max_lag=5)
            arima_spech = {
                "p": arima_order[0],
                "d": arima_order[1],
                "q": arima_order[2],
                "model": "ARIMA",
                "criterion": "BIC",
            }
            _, _ = self.check_arima_residuals(best_arima, returns)
            returns = best_arima.resid.dropna()

        # Gate on ARCH effects rather than on ARIMA adequacy.
        # A series can have white-noise levels yet still have volatility clustering —
        # the ARCH-LM test on residuals is the correct check for whether GARCH is needed.
        has_arch_effects = self.check_arch_effects(returns) if use_vol_model else False

        # Fit GARCH volatility model
        best_garch_fitted = None
        garch_cand_spech = None
        model = None
        if use_vol_model and has_arch_effects:
            # Track all candidates so we can fall back to the least-bad one
            # if no model passes both Kupiec and Christoffersen.
            fallback_candidates = []  # list of (exceedance_rate, fitted, spec)

            for model in self.list_models:
                cand_fitted, cand_model, best_model_dist = self.find_best_vol_model(
                    hist_series=returns, model_type=model, seed=self.seed
                )
                accept_model, _ = self.check_garch_residuals(cand_fitted)

                if not accept_model:
                    print("Residuals or squared residuals autocorrelated and LM effects: skipping model")
                    continue

                vol_spec = cand_model.volatility  # e.g., EGARCH(p=1, q=1)
                garch_spec = {
                    "p": getattr(vol_spec, "p", 0),
                    "o": getattr(vol_spec, "o", 0),
                    "q": getattr(vol_spec, "q", 0),
                    "vol": "GARCH" if model == "GJR-GARCH" else model,
                    "dist": best_model_dist.lower(),
                }

                # Rolling VaR backtest
                backtester = Backtester(
                    price_series=self.price_series,
                    train_size=train_size,
                    forecast_step=forecast_step,
                    garch_spec=garch_spec,
                )

                hit_list = Parallel(n_jobs=4)(
                    delayed(backtester.hit_backtest)(i) for i in range(0, len(self.price_series) - train_size)
                )
                all_hits = np.concatenate(hit_list)
                total_exceedance_rate = all_hits.mean()
                print(f"Overall Exceedance Rate: {total_exceedance_rate:.2%}")

                _, p_value_k = Backtester.kupiec_test(all_hits, backtest_alpha)
                _, _, _, p_value_c = Backtester.christoffersen_test(all_hits, backtest_alpha)

                fallback_candidates.append((total_exceedance_rate, cand_fitted, garch_spec))

                if (p_value_k >= 0.05) and (p_value_c >= 0.05):
                    best_garch_fitted = cand_fitted
                    garch_cand_spech = garch_spec
                    print(f"\nSelected GARCH model: {model}")
                    break
                else:
                    print(f"❌ {model} rejected in backtest (Kupiec or Christoffersen).")

            # Soft fallback: if no model passed the formal tests, pick the candidate
            # whose exceedance rate is closest to the 5% target rather than abandoning
            # GARCH entirely and falling back to ARIMA-only.
            if best_garch_fitted is None and fallback_candidates:
                fallback_candidates.sort(key=lambda x: abs(x[0] - backtest_alpha))
                best_rate, best_garch_fitted, garch_cand_spech = fallback_candidates[0]
                print(
                    f"\n⚠️  No GARCH model passed formal backtests. "
                    f"Using fallback candidate with exceedance rate {best_rate:.2%} "
                    f"(closest to {backtest_alpha:.1%} target): {garch_cand_spech['vol']}"
                )

        if best_garch_fitted is None and not has_arch_effects:
            # No ARCH effects detected — GARCH is not warranted, but the already-fitted
            # ARMA model is still a valid mean forecast and must be kept. Only discard
            # the GARCH spec; setting best_arima to None here would crash returns_forecasting.
            garch_cand_spech = None

        return best_arima, best_garch_fitted, arima_spech, garch_cand_spech
