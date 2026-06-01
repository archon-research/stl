# ============================================================
# Class Backtester: Rolling-window risk analysis for financial time series
# 
# This class provides tools to backtest volatility models (GARCH/EGARCH variants)
# on historical price data, compute Value-at-Risk (VaR) forecasts, and perform
# statistical hit tests such as Kupiec and Christoffersen tests.
# ============================================================

import pandas as pd
import numpy as np
from scipy.stats import chi2
from arch import arch_model


class Backtester:


    def __init__(
        self, 
        price_series: pd.Series, 
        train_size: int, 
        forecast_step: int, 
        garch_spec: dict
    ) -> None:
        
        self.price_series = price_series
        self.train_size = train_size
        self.forecast_step = forecast_step
        self.garch_spec = garch_spec


    @staticmethod
    def calculate_returns(
        series: pd.Series
    ) -> tuple[pd.Series, pd.Series]:
        
        returns = series.pct_change().dropna()
        log_returns = np.log(series / series.shift(1)).dropna()

        returns.name = "Returns"
        log_returns.name = "LogReturns"
        return returns, log_returns


    @staticmethod
    def kupiec_test(
        hits, 
        alpha
    ):
        n = len(hits)
        x = sum(hits)
        pi_hat = x / n
        if pi_hat == 0 or pi_hat == 1:
            LR_pof = 0
            p_value = 1.0
        else:
            LR_pof = -2 * ( (n-x)*np.log(1-alpha) + x*np.log(alpha) - (n-x)*np.log(1-pi_hat) - x*np.log(pi_hat) )
            p_value = 1 - chi2.cdf(LR_pof, df=1)
        return LR_pof, p_value


    @staticmethod
    def christoffersen_test(
        hits, 
        alpha
    ):
        n = len(hits)
        hits = np.array(hits)
        n00 = n01 = n10 = n11 = 0
        for t in range(1, n):
            if hits[t-1]==0 and hits[t]==0: n00 += 1
            if hits[t-1]==0 and hits[t]==1: n01 += 1
            if hits[t-1]==1 and hits[t]==0: n10 += 1
            if hits[t-1]==1 and hits[t]==1: n11 += 1
        pi0 = n01 / (n00 + n01) if (n00+n01)>0 else 0
        pi1 = n11 / (n10 + n11) if (n10+n11)>0 else 0
        pi = (n01 + n11) / (n-1)
        
        # Avoid log(0)
        pi0 = max(min(pi0, 1-1e-10), 1e-10)
        pi1 = max(min(pi1, 1-1e-10), 1e-10)
        pi = max(min(pi, 1-1e-10), 1e-10)
        
        # Likelihood ratio for independence
        L0 = ((1-pi)**(n00+n10)) * (pi**(n01+n11))
        L1 = ((1-pi0)**n00) * (pi0**n01) * ((1-pi1)**n10) * (pi1**n11)
        epsilon = 1e-10  # small number to avoid log(0)
        LR_ind = -2 * np.log(np.clip(L0, epsilon, None) / np.clip(L1, epsilon, None))
        p_value_ind = 1 - chi2.cdf(LR_ind, df=1)
        
        # Conditional coverage = LR POF + LR_ind
        # LR_cc = LR_ind + Backtester.kupiec_test(hits, pi)[0]
        LR_pof, _ = Backtester.kupiec_test(hits, alpha)
        LR_cc = LR_ind + LR_pof
        p_value_cc = 1 - chi2.cdf(LR_cc, df=2)
        return LR_ind, p_value_ind, LR_cc, p_value_cc


    @staticmethod
    def identify_dist(
        name: str
    ) -> str:
        if name == "standardized student's t":
            return 't'
        elif name == "standardized skew student's t":
            return 'skewt'
        else:
            return 'normal'

    # TODO(bug#3): hit_backtest defaults use_log_returns=False but production runs
    # USE_LOG_RETURNS=True, causing Kupiec/Christoffersen model selection to evaluate
    # the wrong return type. Fix: default should match the caller's USE_LOG_RETURNS.
    def hit_backtest(
        self, 
        i: int,
        use_log_returns: bool = True,
        alpha: float = 0.95
    ) -> np.ndarray:
        """
        Computes hits for rolling window i.
        Returns an array of 0/1 hits for each forecasted step.
        """

        # 1. Select rolling window & 1-step-ahead test day.
        # The backtest always evaluates 1-step-ahead VaR regardless of FORECAST_STEP.
        # This gives non-overlapping daily hits (~1280 over 4 years) vs. sparse
        # multi-step windows (~90), giving the Kupiec and Christoffersen tests
        # much more statistical power for model selection.
        prices_df_rolling = self.price_series.iloc[i:(i + self.train_size)].copy()
        prices_actual_df = self.price_series.iloc[(i + self.train_size - 1):(i + self.train_size + 1)].copy()

        actual_price_returns, actual_price_log_returns = self.calculate_returns(prices_actual_df)
        actual_losses = actual_price_log_returns if use_log_returns else actual_price_returns
        actual_losses = actual_losses.to_numpy().ravel()

        forecasted_step = 1

        # 2. Calculate returns
        price_returns, price_log_returns = Backtester.calculate_returns(prices_df_rolling)
        returns = price_log_returns if use_log_returns else price_returns

        # 3. Fit GARCH
        if self.garch_spec:
            dist_name = Backtester.identify_dist(self.garch_spec['dist'])
            garch_model = arch_model(
                returns.squeeze() * 100,
                p=self.garch_spec.get("p", 0),
                o=self.garch_spec.get("o", 0),
                q=self.garch_spec.get("q", 0),
                vol=self.garch_spec.get("vol", "GARCH"),
                dist=dist_name,
                rescale=False
            )
            garch_fitted = garch_model.fit(
                update_freq=0,
                disp="off", 
                options={'maxiter': 1000}
            )
        else:
            garch_fitted = None

        # 4. Forecast volatility
        if garch_fitted:
            try:
                forecast_var = garch_fitted.forecast(horizon=forecasted_step, reindex=False).variance
            except:
                forecast_var = garch_fitted.forecast(horizon=forecasted_step, method='simulation', reindex=False).variance
            sigma_forecast = np.sqrt(forecast_var.iloc[-1, :]) / 100
        else:
            sigma_forecast = np.std(returns)

        params = garch_fitted.params

        try:
            if dist_name == "t":
                z_alpha = garch_fitted.model.distribution.ppf(1 - alpha, params[-1:])
            elif dist_name == "skewt":
                z_alpha = garch_fitted.model.distribution.ppf(1 - alpha, params[-2:])
            else:
                z_alpha = garch_fitted.model.distribution.ppf(1 - alpha)
        except ValueError:
            # Fallback: normal quantile
            from scipy.stats import norm
            z_alpha = norm.ppf(1 - alpha)

        # VaR Forecast
        var_forecast = z_alpha * sigma_forecast
    
        # 7. Compute hits
        hits = (actual_losses < var_forecast).astype(int)
        exceedances = hits.sum()
        exceedance_rate = exceedances / len(hits)

        print(f"Exceedances: {exceedances}/{len(hits)} ({exceedance_rate:.2%})")

        return np.array(hits)

