# CORE — Collateralized Onchain Risk Engine

---

## TL;DR

The Collateralized Onchain Risk Engine (CORE) is a modular Monte Carlo stress-testing framework designed to quantify downside risk in over-collateralised lending protocols. It simulates many possible future price paths for collateral assets, checks every borrower position step by step, executes liquidations subject to realistic market frictions, and measures how much bad debt accumulates on average in the distribution given stressed conditions.

Note that CRR is an expected loss, not a tail loss by construction. However, the inputs that generate bad debt in the model are deliberately conservative: volatility is floored at its 75th historical percentile, liquidity is consumed cumulatively across sequential liquidations without replenishment, and joint tail events across collateral assets are modelled using a t-Copula that assigns materially higher probability to simultaneous crashes than standard correlation assumptions: a bad debt event in this model already presupposes a severe stress scenario.

Tail metrics such as VaR and Expected Shortfall are computed internally but are not reported as primary outputs. In over-collateralised lending, the loss distribution has a specific shape: the vast majority of scenarios produce zero bad debt, while a small fraction of tail scenarios produce very large losses. In this setting, VaR is largely uninformative: it will read zero for most reasonable confidence levels, since the quantile sits in the zero-mass region of the distribution. ES, on the other hand, is computed only over the loss tail and is therefore highly sensitive to the exact composition of the portfolio: a single new borrower entering at a high LTV, or a new collateral token with thin liquidity, can cause a step-change in ES that is disproportionate to their actual weight in the book. This instability makes tail metrics unreliable as a day-to-day monitoring tool in a portfolio that is repriced daily. Expected loss integrates across the entire scenario distribution, including the large zero-loss mass, and produces a figure that moves smoothly and meaningfully as the portfolio evolves, while still reflecting the conservative stress assumptions embedded in the simulation.

The model answers three practical questions:
- **How often do positions get liquidated?** (Probability of Liquidation, PL)
- **How often do liquidations fail to cover outstanding debt?** (Probability of Default, PD)
- **How much capital is needed to absorb tail losses?** (Capital Requirement Ratio, CRR)

---

## 0. Glossary

| Term | Definition |
|---|---|
| **CRR** | Capital Requirement Ratio — bad debt as a fraction of total exposure |
| **EL** | Expected Loss — mean CRR across all Monte Carlo scenarios; the primary headline metric; equals PD × LGD × EAD in Basel notation (here CRR = EL) |
| **HHI** | Herfindahl-Hirschman Index — `Σ (borrow_i / total_borrow)²`; measures borrower concentration; 0 = perfectly granular, 1 = single borrower |
| **PL** | Probability of Liquidation — fraction of positions liquidated in a given scenario |
| **PD** | Probability of Default — fraction of positions generating unrecovered bad debt |
| **VaR** | Value at Risk — α-quantile of the loss distribution (diagnostic) |
| **ES** | Expected Shortfall — expected loss conditional on exceeding VaR (diagnostic) |
| **LTV** | Loan-to-Value ratio — outstanding debt / collateral value |
| **LT** | Liquidation Threshold — LTV level at which a position becomes eligible for liquidation |
| **HF** | Health Factor — (collateral value × LT) / debt; position is unsafe when HF < 1 |
| **EAD** | Exposure at Default — outstanding debt at the moment bad debt is first recorded |

---

## 1. Overview

The model integrates four components into a single pipeline:

1. **Calibrator** — fits and validates ARMA-GARCH models on historical return data
2. **Forecaster / Simulator** — generates correlated Monte Carlo price paths
3. **Aggregator** — constructs cross-asset copula dependence
4. **Liquidator** — simulates protocol-specific liquidation mechanics and computes bad debt

All components share a common data flow: calibrated model parameters drive the simulation, simulated prices drive the liquidation engine, and liquidation outcomes are aggregated into risk metrics.

---

## 2. Data Inputs

The model draws from three independent data layers, each fetched from a different source.

---

### 2.1 Protocol position data

Borrower-level positions (collateral amounts, debt, LTV, liquidation threshold, liquidation bonus) are fetched from parquet files. Position-level inputs per borrower include:

- Collateral amounts and token identities
- Borrowed amounts and loan token identities
- Current LTV and Health Factor
- Liquidation threshold and liquidation bonus

| Protocol | Source | Granularity |
|---|---|---|
| Morpho, Aave, SparkLend, Maple (Syrup), Galaxy, Anchorage | Local parquet files (`inputs/*.parquet`) | Per-position static file |

Protocol parameters used by the liquidation engine (close-factor rules, partial liquidation formulas, gas and swap fee assumptions) are hard-coded per protocol in `liquidator.py` and configurable via `GAS_FEE_USD` and `SWAP_FEE_USD`.

---

### 2.2 Price data

All collateral price histories are downloaded from **Yahoo Finance** (`yfinance`, `period="max"`) at calibration time. For the purposes of this script, however, a precomputed snapshot of these prices is loaded from a parquet file.

---

### 2.3 Order book / liquidity data

Order book depth is uploaded from a parquet files. Routing depends on the collateral token:

| Collateral token | Venue type | Source | Detail |
|---|---|---|---|
| **CBBTC** | DEX | Uniswap V3 | Pool `0xfBB...43ef` (cbBTC/USDC, Base) — live on-chain pool state |
| **HYPE** (and variants) | DEX | HyperLiquid | Native HyperLiquid order book |
| **ETH and LSTs** (WETH, WEETH, STETH, WSTETH, RETH) | CEX | Aggregated | Proxied via ETH spot book |
| **BTC and wrappers** (WBTC, LBTC, TBTC) | CEX | Aggregated | Proxied via BTC spot book |
| **SOL** | CEX | Aggregated | Direct SOL spot book |
| **All other tokens** | CEX | Aggregated | Direct spot book |

CEX aggregation covers **11 venues**: Binance, Bybit, OKX, Kraken, Coinbase, Gate.io, KuCoin, Huobi, Bitget, Bitfinex, Crypto.com.

Liquidity is consumed **cumulatively** within each scenario: each successive liquidation starts from the point in the order book where the previous one left off. This is conservative - it reflects a stress scenario where sequential liquidations face progressively depleted depth, rather than assuming the book replenishes between events.

---

## 3. Return Dynamics

### 3.1 Log Returns

All models operate on daily log returns:

```
r_t = log(P_t / P_{t-1})
```

Log returns are preferred over simple returns for their additive property over time and their approximate symmetry for moderate price changes.

### 3.2 Mean Model — ARMA(p, q)

Where autocorrelation structure is present in the return series, a mean model of the form

```
r_t = c + φ_1 r_{t-1} + ... + φ_p r_{t-p} + ε_t + θ_1 ε_{t-1} + ... + θ_q ε_{t-q}
```

is fitted via maximum likelihood. The order (p, q) is selected by **BIC** over a grid search with p, q ≤ 5. If ARMA residuals already exhibit white noise (Ljung-Box test), the mean model is retained only for its residuals, which are then passed to the volatility model.

### 3.3 Volatility Model — GARCH Family

Whether a GARCH model is warranted is determined by an **ARCH-LM test** on the return series (or ARMA residuals). If heteroskedasticity is detected at p < 0.05, the following specifications are considered in order:

| Model | Variance Equation |
|---|---|
| **FIGARCH(1,1)** | Long-memory: fractional integration in the lag polynomial |
| **GJR-GARCH(1,1)** | `σ²_t = ω + (α + γ·𝟙[ε_{t-1}<0])·ε²_{t-1} + β·σ²_{t-1}` |
| **GARCH(1,1)** | `σ²_t = ω + α·ε²_{t-1} + β·σ²_{t-1}` |
| **EGARCH(1,1)** | `log σ²_t = ω + α·(|z_{t-1}| − E|z|) + γ·z_{t-1} + β·log σ²_{t-1}` |

Each specification is tested with three innovation distributions: Normal, Student-t, and Skewed Student-t. The winning combination minimises BIC. When a Student-t distribution is selected, the degrees-of-freedom parameter `ν` is estimated jointly with the other model parameters via maximum likelihood; this MLE-estimated value is used in all subsequent simulation steps (copula inverse-CDF transform and innovation sampling). A fixed or assumed `ν` is never used.

**Fallback when no ARCH effects are detected.** If the ARCH-LM test does not detect heteroskedasticity (p ≥ 0.05), a GARCH model is not warranted. In this case the GARCH step is skipped entirely, but the fitted ARMA mean model is retained and used for forecasting with historical-volatility innovations. This preserves the mean dynamics without imposing unnecessary variance structure.

### 3.4 Model Validation

A candidate model is accepted only if it passes all three residual diagnostics:
- **Ljung-Box** on standardised residuals (no remaining autocorrelation in mean)
- **Ljung-Box** on squared standardised residuals (no remaining ARCH effects)
- **ARCH-LM test** on standardised residuals (no remaining heteroskedasticity)

Accepted candidates are then subject to a **rolling 1-step-ahead VaR backtest**. The model is trained on a window of `TRAIN_SIZE` days and the 1-day-ahead VaR is computed at level `backtest_alpha = 1 - PERC`. The window then rolls forward by 1 day, producing approximately (`N_history` − `TRAIN_SIZE`) non-overlapping hit observations — roughly 1 280 over a 4-year history with a 180-day training window. Two statistical tests are applied to the resulting hit sequence:

- **Kupiec POF test** — tests unconditional coverage: does the observed exceedance rate match `backtest_alpha`?
- **Christoffersen test** — tests conditional coverage: are exceedances independent over time?

Note: the backtest evaluates 1-step-ahead VaR only, while the simulation uses a `FORECAST_STEP`-day horizon. These are separate concerns — 1-step-ahead backtesting is the industry standard for model validation and provides far more statistical power than rolling by `FORECAST_STEP` days (which would yield only ~90 non-overlapping windows).

Both tests must have p-values ≥ 0.05 for a model to be accepted. If no model passes both tests, a **soft fallback** selects the candidate whose rolling exceedance rate is closest to `backtest_alpha`, ensuring a GARCH model is always used for assets with detected heteroskedasticity.

### 3.5 Volatility Floor

GARCH models are conditionally adaptive: they produce low volatility forecasts during calm market regimes, which can cause capital requirements to collapse precisely when the risk environment is benign but not necessarily safe. To prevent this procyclicality, the GARCH conditional volatility forecast is bounded below by:

```
vol_floor = Percentile(rolling_21d_std(r_t, full history), VOL_FLOOR_PCT)
```

The floor is computed from the **full historical price series** (not the training window), ensuring it remains stable regardless of the window size used for model estimation. `VOL_FLOOR_PCT = 0.75` corresponds to the 75th percentile of historical realised volatility, keeping the model in the upper half of observed conditions.

### 3.6 Lindy Volatility Scaling (Optional)

When `LINDY_ALPHA > 0`, a per-token uncertainty premium is applied multiplicatively to the GARCH conditional volatility forecast before innovations are sampled:

```
lindy_factor = min(LINDY_MAX_FACTOR,  max(1.0,  (LINDY_REF_DAYS / n_obs)^LINDY_ALPHA))
σ̂_t  ←  σ̂_t × lindy_factor
```

**Rationale.** GARCH parameter estimates carry wide confidence intervals when the training sample is short. A token with only 6 months of price history may have its GARCH fit dominated by a single bull market, giving conditional volatility forecasts that understate tail risk relative to a token with 5+ years of diverse regime coverage. The Lindy factor compensates by scaling up the vol forecast in proportion to the shortfall in history, making capital requirements more conservative when data is sparse.

| Parameter | Role |
|---|---|
| `LINDY_ALPHA` | Decay exponent; `0.0` = disabled (factor = 1.0 always); `0.5` = square-root decay (recommended) |
| `LINDY_REF_DAYS` | History length (days) at which factor = 1.0; default 1825 (≈ 5 years of daily data) |
| `LINDY_MAX_FACTOR` | Hard cap on the multiplier; default 2.0 prevents extreme inflation for brand-new assets |

Example factors at α = 0.5, ref = 1825 d, cap = 2.0:

| Token | History | factor |
|---|---|---|
| BTC / ETH | ≥ 5 y | 1.00 |
| SOL | ≈ 3 y | 1.29 |
| WIF / PENDLE | ≈ 2 y | 1.58 |
| HYPE (6 m) | ≈ 180 d | 2.00 (capped) |

When `LINDY_ALPHA = 0.0` (the default), the factor is always 1.0 and the feature is a strict no-op — it has zero effect on any model output.

### 3.7 Jump Component (Optional)

When `JUMPS = True`, a compound Poisson jump process is added to the return:

```
J_t = N_t × j_t

N_t ~ Poisson(λ)               jump occurrence
j_t ~ Student-t(df, μ_j, σ_j)  jump size
```

Jump parameters (λ, μ_j, σ_j, df) are **calibrated independently for each collateral token** from the tail of that token's historical return distribution. By default, bilateral tails are used (returns below the 2.5th or above the 97.5th percentile). Setting `FOCUS_ON_NEGATIVE = True` restricts calibration to the left tail only, and clips simulated jumps to be non-positive, which is more conservative for liquidation risk.

In multi-token portfolios, each token's price path is generated using its own calibrated jump parameters, correctly reflecting heterogeneous tail behaviour across assets.

---

## 4. Cross-Asset Correlation

When multiple collateral tokens are present in a market, the model captures their joint tail behaviour via a **copula**. The key steps are:

**Step 1 — Spearman rank correlation.** Pairwise Spearman correlations are computed on the standardised GARCH residuals. Spearman correlation is preferred over Pearson because it is robust to outliers and captures monotonic dependence without requiring linearity.

**Step 2 — Conversion to copula correlation.** Spearman correlations are converted to the linear correlations used inside the copula via:

```
ρ_copula = 2 · sin(π · ρ_spearman / 6)
```

**Step 3 — PSD enforcement.** If the resulting correlation matrix is not positive semi-definite (due to rounding or incomplete pairwise observations), it is regularised using the Rebonato-Jäckel eigenvalue flooring method.

**Step 4 — Copula sampling.** Two copula types are supported:

- **Gaussian copula** — joint Normal dependence; correctly captures linear correlation but underestimates tail co-movement.
- **t-Copula** — joint Student-t dependence; adds tail dependence, meaning assets are more likely to crash together in extreme scenarios. This is the recommended setting for crypto collateral.

For single-token markets, uniform samples are drawn independently.

---

## 5. Price Simulation

For each of the `N_MC` Monte Carlo scenarios:

1. Sample a row of `FORECAST_STEP` correlated uniform variates from the copula.
2. Transform each uniform to an innovation via the fitted distribution's inverse CDF (t or Normal). When the fitted distribution is Student-t, the MLE-estimated degrees-of-freedom `ν` is used — not a fixed assumption — ensuring the simulated tail thickness matches what was observed in the data.
3. Combine with GARCH conditional volatility forecast and ARMA mean forecast:

```
r̂_t = μ̂_t + σ̂_t · z_t + J_t
```

4. Reconstruct prices from cumulative log returns:

```
P_t = P_0 · exp(Σ r̂_s for s=1 to t)
```

Cumulative log returns are clipped to [−log(5), log(5)] to prevent numerical overflow in extreme scenarios.

**Brownian Bridge (optional).** When `HOURLY_CONV = True`, each daily return is decomposed into 24 hourly sub-returns using a Brownian bridge conditioned on the daily endpoint. This allows the liquidation engine to check for threshold crossings at hourly frequency, better capturing intraday liquidation dynamics. This is the more realistic and less conservative configuration; daily-only simulation should be preferred when continuous liquidation cannot be assumed.

---

## 6. Liquidation Engine

The liquidation engine processes each borrower position step by step along each simulated price path.

### 6.1 State Update

At each time step, collateral values are recomputed using simulated prices, and LTVs and Health Factors are updated accordingly.

### 6.2 Margin Call Logic (Maple / Galaxy)

For products with a margin call mechanism, when a position breaches the margin call threshold, the borrower is given a probabilistic opportunity to self-cure. The cure probability is calibrated from historical data provided by the asset manager. If cure occurs, the borrower posts additional collateral to restore the LTV to the margin call threshold. If no cure occurs, the position proceeds to liquidation.

### 6.3 Liquidation Trigger

A position is eligible for liquidation when:

```
LTV ≥ LT   ↔   HF < 1
```

### 6.4 Repayment Amount

**Morpho** uses partial liquidation, restoring the position exactly to the liquidation threshold:

```
R_req = (LT × CV − D) / (LT × (1 + bonus) − 1)
```

where CV is collateral value and D is outstanding debt.

**Aave / SparkLend** use a close-factor approach:
- HF > 0.95 → repay 50 % of outstanding debt
- HF ≤ 0.95 → repay 100 % of outstanding debt

In all cases, `R_req` is bounded by available collateral.

### 6.5 Profitability Constraint

A liquidation is only executed if the liquidator earns a non-negative profit:

```
proceeds = (1 − swap_fee − slippage) × (1 + bonus) × R_req
profit   = proceeds − R_req − gas_fee_usd
```

If `profit < 0`, no liquidation occurs and the full outstanding debt is recorded as bad debt at that step.

### 6.6 Slippage Modelling

Slippage is computed from a synthetic order book aggregated across 12+ CEX venues (Binance, Bybit, OKX, Kraken, Coinbase, Gate.io, KuCoin, Huobi, Bitget, Bitfinex, Crypto.com) and Uniswap V3. For a required liquidation of size `R_req`, the model:

1. Identifies available sell-side liquidity at prices ≤ the simulated price
2. Computes the average execution price by consuming order book depth sequentially
3. Derives slippage as the deviation of average execution price from the mid price

The order book is fetched once at simulation start and held static over the forecast horizon. Crucially, liquidity is consumed cumulatively across liquidation events: each successive liquidation starts from the point in the order book where the previous one left off, rather than assuming a fully replenished book. This is a conservative assumption — in a real stress event, market depth is likely to thin further as prices decline, and sequential liquidations would face progressively worse execution prices.

The amount consumed from the order book at each liquidation event is the **collateral seized**, not the debt repaid: seized collateral = R_req × (1 + bonus). This is correct because the order book is denominated in collateral-asset USD — the liquidator sells collateral to repay debt, so it is the sell-side collateral quantity that depletes the book.

### 6.7 Bad Debt Accounting

Bad debt is recorded using "count once" semantics:
- When a position first becomes unsafe and cannot be liquidated profitably, the full EAD is recorded as bad debt
- In subsequent steps, if the liquidation becomes profitable (e.g., volatility subsides), any recovered amount reduces the outstanding bad debt
- Net bad debt = EAD − cumulative recoveries

This correctly distinguishes between exposure at default and net economic loss.

---

## 7. Risk Metrics

Scenario-level net bad debt figures are aggregated into the following metrics:

| Metric | Definition | Role |
|---|---|---|
| **CRR (EL)** | Mean (Net Bad Debt / Total Exposure) across all N scenarios | **Headline metric** |
| **HHI** | `Σ (borrow_i / total_borrow)²` computed from the live borrower DataFrame | Concentration diagnostic |
| **PL** | α-quantile of the fraction of positions liquidated per scenario | Liquidation activity |
| **PD** | α-quantile of the fraction of positions with net bad debt > 0 | Default frequency |
| **Delta LTV** | α-quantile of the maximum LTV overshoot above LT across all positions | Severity indicator |

### Why EL is the headline metric

CRR (EL) equals `PD × LGD × EAD` in Basel notation — the expected cost of lending expressed as a fraction of total exposure. It is:

- **Stable under concentration**: each scenario contributes equally to the mean regardless of whether one large borrower or many small borrowers drive the loss. ES, by contrast, averages only the worst tail and is dominated by whichever large concentrated position happens to default in those scenarios, producing numbers that can be an order of magnitude larger than EL for the same portfolio.
- **Directly comparable across protocols**: different markets, loan tokens, and horizon assumptions all produce EL figures on the same scale, making cross-protocol ranking meaningful.
- **Interpretable as a cost**: EL quantifies the average bad-debt expense per dollar lent, which maps directly to the spread required to break even on risk.

### Concentration and EL

A high HHI signals that EL is driven by a small number of large positions rather than a diversified pool. This does not make EL wrong, but it does change its interpretation: for a concentrated portfolio, EL reflects the expected loss in a world where that large borrower defaults with some frequency, and the confidence interval around EL is wide. The HHI is therefore shown alongside EL in the dashboard to provide context.

VaR and ES are computed internally and remain available as diagnostics in `Liquidator.compute_run_stats()`, but they are not the primary reported output.

---

## 8. Model Calibration Choices and Limitations

### Calibration choices

| Choice | Rationale |
|---|---|
| BIC for model selection | Penalises complexity more heavily than AIC; prevents overfitting on short training windows |
| ARCH-LM gate for GARCH | Ljung-Box on levels detects mean autocorrelation, not variance clustering; ARCH-LM is the correct pre-test for GARCH |
| 1-step-ahead backtest rolling by 1 day | Rolling by `FORECAST_STEP` days produces only ~90 non-overlapping windows over 4 years — too sparse for reliable Kupiec / Christoffersen tests. Rolling by 1 day gives ~1280 non-overlapping 1-day hits, providing proper statistical power. The 1-step-ahead horizon is the standard for VaR model validation; the multi-step simulation horizon is a separate concern. |
| Soft backtest fallback | Hard rejection of all GARCH models when none passes formal tests causes a regression to constant-volatility forecasting; the least-bad GARCH candidate is always preferable |
| ARMA retained when no ARCH effects | When no heteroskedasticity is detected, skipping GARCH does not mean discarding the mean model — the fitted ARMA is preserved and residuals are scaled by historical volatility, which is strictly better than reverting to a constant-mean random walk |
| t-Copula over Gaussian | Crypto assets exhibit strong tail co-dependence; a Gaussian copula underestimates the probability of simultaneous crashes |
| Volatility floor from full history | A floor computed from the training window is self-referential and moves with window size; anchoring to the full series gives a stable long-run stress reference |
| Lindy vol scaling disabled by default | The feature is a conservative add-on, not a baseline assumption. It is off (`LINDY_ALPHA = 0.0`) by default so that standard outputs remain directly comparable with prior model runs. Analysts may activate it (recommended α = 0.5) when assessing portfolios that include newly listed tokens with limited price history |
| Spearman over Pearson for correlation | Robust to outliers; captures monotonic relationships without requiring linearity |

### Known limitations

| Limitation | Impact |
|---|---|
| No cascading liquidations | Large-scale simultaneous selling depresses prices further; second-order market impact is not modelled |
| No interest rate dynamics | Borrow rates spike during high-utilisation stress events; fixed-rate assumption is optimistic |
| Oracle risk not modelled | Stale oracles or oracle manipulation are not captured |
| Stablecoin collateral filtered | Peg risk for USDC, USDT, and crypto-backed stablecoins is not modelled; positions using stablecoin collateral are excluded from price simulation |

---

## 9. Expected Model Behaviour and Sensitivity

This section documents the qualitative relationships between model inputs and outputs that a correctly functioning model should exhibit. These relationships serve as sanity checks when interpreting results and as a guide for parameter sensitivity analysis.

### 9.1 Borrower LTV Level

**Higher initial LTV → higher CRR.**

A borrower at LTV = 0.80 with a liquidation threshold of LT = 0.85 requires only a 6% collateral price decline to be liquidated. A borrower at LTV = 0.60 requires a 29% decline. For a given volatility and horizon, a higher-LTV portfolio means a larger fraction of the simulated price distribution reaches the liquidation boundary, increasing both PL and the frequency of scenarios where liquidations fail to recover the full debt.

In practice, CRR is highly non-linear in LTV: portfolios where most borrowers sit comfortably below LT (LTV < 0.70) will show near-zero CRR under typical volatility assumptions, while portfolios with LTV tightly clustered just below LT (LTV ≥ 0.80) can exhibit CRR that increases sharply with even modest changes in volatility or horizon.

### 9.2 Collateral Volatility

**Higher volatility → higher CRR. Longer forecast horizon → higher CRR.**

The simulated price distribution widens as volatility increases, moving more scenarios into the tail where LTV breaches the liquidation threshold. This effect is compounded over the forecast horizon: for a random walk, the variance of cumulative log returns scales with the number of steps, so a 14-day horizon produces a distribution roughly √14 ≈ 3.7× wider than the 1-day distribution (all else equal).

Consequently:
- Tokens with high historical volatility (e.g. newer or less liquid assets) will always produce higher CRR than BTC or ETH under the same portfolio structure.
- Extending the forecast horizon increases CRR monotonically, with diminishing marginal effect as the distribution becomes so wide that incremental steps move little additional probability mass into the loss region.
- The volatility floor (`VOL_FLOOR_PCT`) prevents CRR from collapsing to near-zero during sustained low-volatility regimes by anchoring the minimum forecast volatility to the 75th percentile of historical realised vol.

### 9.3 Portfolio Concentration

**Few large borrowers → higher CRR than many small borrowers of equal total size.**

Concentration risk operates through two distinct channels:

**Channel 1 — Order book depth.** When a single large borrower is liquidated, the required sale size is proportionally large relative to the available market depth. Slippage is a convex function of trade size — doubling the sale amount more than doubles the price impact. A portfolio dominated by one large position therefore faces disproportionately poor liquidation execution, making it more likely that liquidations become unprofitable and the position converts to bad debt.

**Channel 2 — Statistical diversification.** A portfolio of many small, partially independent borrowers benefits from diversification across idiosyncratic risk: not all positions will be at high LTV simultaneously, and the distribution of aggregate bad debt converges (by a law-of-large-numbers argument) around its mean. A single large borrower provides no such averaging — the outcome is effectively binary (liquidated or not), which is reflected in a heavier tail of the bad-debt distribution and a higher CRR at any given confidence level.

As a rule of thumb, if the top-3 borrowers represent more than 50% of total exposure, concentration risk is a material driver of CRR. The model captures this correctly because it operates at individual-position level, not at portfolio-aggregate level.

The HHI (Herfindahl-Hirschman Index) is computed from the live borrower DataFrame and reported alongside CRR (EL) in the dashboard. An HHI above 0.25 (equivalent to fewer than 4 equal-sized borrowers) indicates that the portfolio is concentrated and the EL should be read in that context. An HHI below 0.10 (more than 10 equivalent borrowers) indicates a sufficiently granular portfolio where EL is a reliable portfolio-wide measure.

### 9.4 Order Book Depth and Collateral Liquidity

**Thinner order books → higher CRR.**

For a given liquidation size, thinner market depth implies higher slippage and a higher probability that the profitability constraint (`profit < 0`) prevents liquidation, converting the full position to bad debt. This effect is token-specific: BTC and ETH are supported by deep, aggregated CEX order books across 12+ venues, while smaller-cap tokens have substantially thinner depth.

In multi-collateral portfolios, CRR is dominated by the most illiquid collateral token, even if that token represents a minority of total exposure, because it is in exactly those positions that liquidators are least likely to execute profitably under stress.

### 9.5 Cross-Asset Correlation

**Higher cross-asset correlation → higher CRR.**

When multiple collateral tokens are highly correlated, simultaneous adverse price moves are more likely. This concentrates losses in a smaller number of severe scenarios rather than spreading them across independent events.

The model uses a t-Copula (recommended) which, relative to a Gaussian copula, assigns additional probability mass to joint tail events. This means assets are more likely to decline sharply at the same time, consistent with observed crypto market behaviour during stress episodes. Switching from t-Copula to Gaussian copula will mechanically reduce CRR estimates for multi-token portfolios and should be considered a less conservative assumption.

### 9.6 Liquidation Threshold and Bonus

**Lower LT → lower CRR. Higher liquidation bonus → lower CRR.**

A lower liquidation threshold gives the protocol more room to liquidate positions before they become undercollateralised, reducing the probability that price paths reach a point where debt cannot be recovered. A higher liquidation bonus makes each liquidation event more profitable for the liquidator, reducing the probability that the profitability constraint (`profit < 0`) causes a failed liquidation.

These two parameters are not independent in protocol design: a higher bonus increases the liquidator's incentive but also increases the collateral seized per liquidation, which itself increases the liquidation sale size and thus slippage. The model captures this trade-off explicitly, so reducing the bonus to test sensitivity will both reduce slippage and reduce liquidator profitability — the net effect on CRR depends on which channel dominates for the specific portfolio and order book depth.

### 9.7 Defense Mechanisms

**Higher protection → lower net CRR. Gross CRR is unaffected.**

Protection layers (FLC, subsidies, senior tranches) reduce net bad debt by absorbing losses before they fall on the modelled exposure. The model computes both gross bad debt (before any protection) and net bad debt (after protection is applied at the scenario level). CRR is reported on net bad debt by default.

An important implication: if the protection amount is large relative to the typical tail loss, net CRR may be close to zero even when gross CRR is material. In this case, both metrics should be reported to avoid conveying a misleading picture of the underlying risk — the gross figure reflects the structural fragility of the portfolio, while the net figure reflects the effective capital requirement after structural mitigants are taken into account.

### 9.8 Summary Table

| Input change | Direction | Primary channel |
|---|---|---|
| Initial LTV ↑ | CRR ↑ | Smaller price drop needed to breach LT |
| Collateral volatility ↑ | CRR ↑ | Wider simulated price distribution |
| Forecast horizon ↑ | CRR ↑ | Variance scales with time |
| Vol floor percentile ↑ | CRR ↑ | Minimum forecast vol increases |
| Lindy α ↑ (for short-history tokens) | CRR ↑ | Uncertainty premium scales up conditional vol |
| Portfolio concentration ↑ | CRR ↑ | Higher slippage + loss of diversification |
| Order book depth ↑ | CRR ↓ | Lower slippage → more profitable liquidations |
| Cross-asset correlation ↑ | CRR ↑ | More simultaneous tail events |
| Copula: Gaussian → t | CRR ↑ | Heavier joint tails |
| Liquidation threshold ↑ | CRR ↑ | Less buffer before undercollateralisation |
| Liquidation bonus ↑ | CRR ↓ | Higher profitability, but also higher sale size |
| Protection (FLC/subsidies) ↑ | Net CRR ↓ | Absorbs losses before they reach the modelled tranche |

---

## 10. Further Developments

The following extensions are under consideration for future model iterations:

- Idle capital risk — stablecoins: extension of the CRR framework to cover idle capital held in stablecoins, which is currently excluded from the model perimeter.
- Idle capital risk and collateral — RWA tokens: integration of tokenised real-world assets both as a form of idle capital and as accepted collateral, accounting for the distinct risk structure of these instruments relative to native crypto assets.
- PT tokens: extension of the liquidation and price simulation framework to cover fixed-rate DeFi instruments whose price dynamics depend on both interest rate movements and protocol credit risk.
- DEX liquidity integration: broader coverage of decentralised exchange venues for order book depth aggregation, relevant for collateral tokens whose liquidity resides primarily or exclusively on-chain.

---

**Key observations:**
- Hourly liquidation granularity materially reduces CRR relative to daily, reflecting realistic continuous liquidation dynamics. Daily-only simulation is the more conservative, prudential configuration.
- In addition to CRR, the model provides PL, PD, and Delta LTV, giving a richer view of the risk drivers beyond a single capital number.

---

*End of Documentation*
