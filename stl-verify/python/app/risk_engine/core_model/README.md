# CORE - Collateralized Onchain Risk Engine

A quantitative framework for computing the **Capital Requirement Ratio (CRR)** across over-collateralised DeFi lending protocols. The model combines ARMA-GARCH price simulation, copula-based cross-asset correlation, an optional compound Poisson jump component, and full liquidation mechanics to estimate the **Expected Loss (EL)** of bad-debt exposure (the primary risk metric) together with concentration diagnostics based on the Herfindahl-Hirschman Index (HHI) of borrower exposures.

Note that CRR is an expected loss, not a tail loss by construction. However, the inputs that generate bad debt in the model are deliberately conservative: volatility is floored at its 75th historical percentile, liquidity is consumed cumulatively across sequential liquidations without replenishment, and joint tail events across collateral assets are modelled using a t-Copula that assigns materially higher probability to simultaneous crashes than standard correlation assumptions: a bad debt event in this model already presupposes a severe stress scenario.

---

## Note

This directory contains the CORE model as integrated into the STL service. The original standalone version lives in `core_model_copy/`. The integration wires CORE as a first-class `RiskModel` backed by a pre-compute cronjob and a thin API service that reads the results.

---

## Supported Protocols

| Protocol | Data Source |
|---|---|
| **Morpho** | Parquet snapshots (long-term: on-chain via block RPC workers) |
| **SparkLend** | Parquet snapshots (long-term: on-chain via block RPC workers) |
| **Maple** | Parquet snapshots |
| **Galaxy** | Parquet snapshots (off-chain, requires maintainer approval per CONTRIBUTING.md §5) |
| **Anchorage** | Parquet snapshots (off-chain, requires maintainer approval per CONTRIBUTING.md §5) |

---

## Data Sources

The model draws from three distinct data layers. Each is fetched independently and at a different cadence.

### 1 — Protocol position data

Borrower-level positions (collateral amounts, debt, LTV, liquidation threshold, liquidation bonus) are currently loaded from static parquet snapshots in `inputs/`. The long-term target is on-chain via block RPC workers.

### 2 — Price data

All collateral price histories are loaded from a parquet snapshot in `inputs/prices_df.parquet`. The long-term target is the existing `offchain-price-indexer` extended to 180-day retention.

### 3 — Order book / liquidity data

Order book depth is loaded from per-token parquet snapshots in `inputs/`. The long-term target is a new `orderbook-indexer` cronjob. Routing depends on the collateral token:

| Collateral token | Venue type | Source | Notes |
|---|---|---|---|
| **CBBTC** | DEX | Uniswap V3 | Pool `0xfB...43ef` (cbBTC/USDC, Base) — on-chain pool state |
| **HYPE** (and variants) | DEX | HyperLiquid | Native HyperLiquid order book |
| **ETH and LSTs** (WETH, WEETH, STETH, WSTETH, RETH) | CEX | Aggregated | Proxied via ETH spot book across 11 venues |
| **BTC and wrappers** (WBTC, LBTC, TBTC) | CEX | Aggregated | Proxied via BTC spot book across 11 venues |
| **SOL** | CEX | Aggregated | Direct SOL spot book across 11 venues |
| **All other tokens** | CEX | Aggregated | Direct spot book across 11 venues |

CEX aggregation covers: **Binance, Bybit, OKX, Kraken, Coinbase, Gate.io, KuCoin, Huobi, Bitget, Bitfinex, Crypto.com**.

Liquidity is consumed **cumulatively** across liquidation events within a scenario: each successive liquidation starts from the point in the book where the previous one left off, rather than assuming a fully replenished book.

---

## Architecture

```
runner.py             Service entry point — orchestrates the full pipeline
│
├── importer.py       Protocol-specific data loaders (users + market data) plus prices and orderbook data
│
├── calibrator.py     ARMA / GARCH-family model selection, diagnostics, backtesting
│   └── backtester.py Rolling VaR backtests (Kupiec + Christoffersen)
│
├── forecaster.py     Monte Carlo price simulation (Forecaster + Simulator)
│   └── aggregator.py Cross-asset copula construction (Gaussian / t-Copula)
│
└── liquidator.py     Liquidation mechanics + bad debt / CRR calculation
```

### Pipeline

| Step | Module | Description |
|---|---|---|
| 1 | `importer.py` / `CoreModelDataReader` | Fetch borrower positions and market parameters plus price and orderbook data for each modelled token |
| 2 | `calibrator.py` | Fit ARMA(p,q)-GARCH-family models on daily log returns; select best specification by BIC; validate with ARCH-LM diagnostics and rolling Kupiec / Christoffersen backtests |
| 3 | `calibrator.py` | Optionally fit a compound Poisson jump process with Student-t jump sizes to tail return observations |
| 4 | `forecaster.py` / `aggregator.py` | Generate `N_MC` correlated price scenarios via a Gaussian or t-Copula; optionally decompose to hourly resolution using a Brownian bridge |
| 5 | `liquidator.py` | For each scenario, apply protocol-specific liquidation rules, compute liquidator profit (after gas and slippage), and accumulate bad debt. Compute finally risk metrics |

---

## Key Parameters

| Parameter | Default | Description |
|---|---|---|
| `PROTOCOL` | `MORPHO` | Target protocol |
| `NETWORK` | `ETHEREUM` | Target network |
| `FORECAST_STEP` | `14` | Forecast horizon (days) |
| `TRAIN_SIZE` | `180` | Rolling training window (days) |
| `N_MC` | `10 000` | Monte Carlo scenarios |
| `PERC` | `0.975` | VaR / ES confidence level |
| `COPULA_TYPE` | `T-COPULA` | Cross-asset dependence structure (`GAUSSIAN` or `T-COPULA`) |
| `HOURLY_CONV` | `False` | Decompose daily returns to hourly via Brownian bridge |
| `USE_LOG_RETURNS` | `True` | Use log returns instead of simple returns |
| `JUMPS` | `False` | Include compound Poisson jump component |
| `FOCUS_ON_NEGATIVE` | `False` | Restrict jump simulation to downside only |
| `VOL_FLOOR_PCT` | `0.75` | Floor GARCH forecast vol at this percentile of the full historical rolling vol |
| `WORST_CASE` | `False` | Use worst-case LTVs instead of observed LTVs |
| `LOAN_TOKEN` | `USDC` | Filter positions by loan token (`ALL` = no filter) |
| `SEED` | `0` | Global random seed |

All parameters can be overridden via environment variables when running the cronjob — see `cli/cronjobs/core_model_runner/config.py` for the full mapping.

---

## Volatility Models

The calibrator performs a grid search over GARCH-family specifications, each tested with Normal, Student-t, and Skewed-t innovations. The winning model is selected by **BIC** and must:

1. Pass residual diagnostics: Ljung-Box on standardised residuals and squared residuals, plus ARCH-LM test
2. Pass rolling VaR backtests: **Kupiec** (unconditional coverage) and **Christoffersen** (conditional coverage / independence) at `backtest_alpha = 1 - PERC`

Models tested (in order of preference):

| Model | Characteristic |
|---|---|
| FIGARCH(1,1) | Long-memory volatility |
| GJR-GARCH(1,1) | Asymmetric response to negative shocks |
| GARCH(1,1) | Standard volatility clustering |
| EGARCH(1,1) | Leverage effects, log-variance formulation |

If no model passes both backtests, a **soft fallback** selects the candidate whose rolling exceedance rate is closest to `backtest_alpha`, rather than discarding GARCH entirely.

### Volatility Floor

To prevent capital requirements from collapsing during low-volatility regimes, the GARCH conditional volatility forecast is floored at the `VOL_FLOOR_PCT` percentile of the 21-day rolling realised volatility computed over the **full historical series** (not just the training window).

---

## Liquidation Mechanics

### Morpho
Partial liquidation up to the repayment amount `R_req` that restores the position exactly to the liquidation threshold:

```
R_req = (LT × CV - D) / (LT × (1 + bonus) - 1)
```

### Aave / SparkLend
Close-factor liquidation based on Health Factor:
- **HF > 0.95** → 50 % of outstanding debt repaid
- **HF ≤ 0.95** → 100 % of outstanding debt repaid

### Liquidator Profitability Constraint
Liquidation is only executed if the liquidator makes a non-negative profit:

```
proceeds = (1 - swap_fee - slippage) × (1 + bonus) × R_req
profit   = proceeds - R_req - gas_fee_usd  ≥ 0
```

---

## Risk Metrics

| Metric | Definition |
|---|---|
| **CRR (EL)** | Mean (Net Bad Debt / Total Exposure) across all `N_MC` scenarios — the Basel Expected Loss analog; the primary headline metric |
| **HHI** | Herfindahl-Hirschman Index of borrower exposures: `Σ (borrow_i / total_borrow)²`; ranges from 0 (perfectly granular) to 1 (single borrower) |
| **PL** | `PERC`-quantile of the fraction of positions liquidated |
| **PD** | `PERC`-quantile of the fraction of positions generating bad debt |
| **Delta LTV** | `PERC`-quantile of the maximum LTV overshoot above the liquidation threshold |

CRR (EL) is the headline metric. It equals `PD × LGD` in Basel notation.

---

## Usage in the STL Service

CORE runs as a two-step process: a cronjob pre-computes the CRR and writes results to the `core_model_results` DB table; the API service reads the latest result at request time.

### Step 1 — Run the pre-compute cronjob

From `stl-verify/python/`:

```bash
DATABASE_URL=postgresql://... \
CORE_MODEL_MARKET_KEY=sparklend_usdc \
CORE_MODEL_PROTOCOL=SPARKLEND \
CORE_MODEL_LOAN_TOKEN=USDC \
python -m cli.cronjobs.core_model_runner.main
```

Override any model parameter via env var (e.g. `CORE_MODEL_N_MC=5000`). See `cli/cronjobs/core_model_runner/config.py` for the full list of overridable parameters and their defaults.

### Step 2 — Query via the risk API

Once a result has been written, the `CoreModelRiskService` serves it through the standard risk endpoint:

```
GET /v1/risk/rrc?asset_id=<id>&prime_id=<address>
```

The service multiplies `crr_el_pct` from the latest pre-computed result by the prime's USD exposure.

### asset_id → market_key mapping

To enable a market, add an entry to `mappings/asset_to_market_key.json`:

```json
{
  "1:0xYourReceiptTokenAddress": "sparklend_usdc"
}
```

The key is `chain_id:0xAddress` (same format as the SURAF mapping). The value is the `market_key` used when running the cronjob.

---

## Module Structure

```
app/risk_engine/core_model/
├── runner.py                     Orchestration entry point (replaces standalone main.py)
├── calibrator.py                 ARMA / GARCH model selection and backtesting
├── backtester.py                 Rolling VaR backtests (Kupiec + Christoffersen)
├── forecaster.py                 Monte Carlo price simulation
├── aggregator.py                 Cross-asset copula construction
├── liquidator.py                 Liquidation mechanics and CRR calculation
├── importer.py                   Data loading utilities (change_user_ltvs etc.)
├── config.py                     Parameter defaults (inputs/default_params.json)
├── core_model_mapping.py         asset_id -> market_key mapping loader
├── mappings/
│   └── asset_to_market_key.json  Chain/address -> market_key mapping
├── inputs/                       Static parquet snapshots (positions, prices, orderbooks)
└── README.md                     This file

app/ports/
├── core_model_data_reader.py     Port: get_protocol_data, get_prices
└── core_model_results_reader.py  Port: get_latest(market_key)

app/adapters/
├── parquet/core_model_data_reader.py    Reads static parquet snapshots
└── postgres/core_model_results_reader.py  Reads core_model_results table

app/services/core_model_risk_service.py  RiskModel implementation

cli/cronjobs/core_model_runner/
├── config.py    Env-var settings
└── main.py      Entry point: run pipeline, write to DB
```

---

## Known Issues

These bugs exist in the original model code and have not been fixed during integration. They are tracked as `# TODO` comments in the source files.

| ID | File | Line | Severity | Description |
|---|---|---|---|---|
| #2 | `liquidator.py` | ~510 | Low | `final_collat_totals` is never populated — always zero. `summary_df['final_total_collateral']` is silent wrong data in every run. |
| #3 | `backtester.py` | ~111 | High | `hit_backtest` defaults `use_log_returns=False` but production uses `USE_LOG_RETURNS=True`. Kupiec/Christoffersen model selection runs on the wrong return type — the "winning" GARCH model may not be the best for simulation. |
| #4 | `aggregator.py` | ~203 | High | t-Copula `nu` is hardcoded to 3. MLE estimation exists but is disabled. `nu=3` produces very fat tails and is a material assumption that ignores the data. |
| #5 | `runner.py` | | Medium | Jump parameters are calibrated from one token and applied uniformly to all tokens. Per-token override path exists in `forecaster.py` but is never populated. |
