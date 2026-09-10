# CORE - Collateralized Onchain Risk Engine

A quantitative framework for computing the **Capital Requirement Ratio (CRR)** across over-collateralised DeFi lending protocols. The model combines ARMA-GARCH price simulation, copula-based cross-asset correlation, an optional compound Poisson jump component, and full liquidation mechanics to estimate the **Expected Loss (EL)** of bad-debt exposure (the primary risk metric) together with concentration diagnostics based on the Herfindahl-Hirschman Index (HHI) of borrower exposures.

Note that CRR is an expected loss, not a tail loss by construction. However, the inputs that generate bad debt in the model are deliberately conservative: volatility is floored at its 75th historical percentile, liquidity is consumed cumulatively across sequential liquidations without replenishment, and joint tail events across collateral assets are modelled using a t-Copula that assigns materially higher probability to simultaneous crashes than standard correlation assumptions: a bad debt event in this model already presupposes a severe stress scenario.

---

## Note

This directory contains the CORE model as integrated into the STL service. The original standalone version lives in [`core_model_copy/`](https://github.com/TWave-code/core_model_copy). The integration wires CORE as a first-class `RiskModel` backed by a pre-compute cronjob and a thin API service that reads the results.

Each market picks its data sources per input via the `*_SOURCE` flags in `inputs/market_configs.json` — `postgres` reads the live tables, `parquet` the static snapshots (that file is the list of which markets run live). Env vars override in two shapes, most specific wins: `CORE_MODEL_PRICE_SOURCE=parquet` forces every market (the dev overlay pins local kind back to parquet this way), and `CORE_MODEL_<MARKET>_<KEY>=parquet` (e.g. `CORE_MODEL_SYRUP_USDC_PRICE_SOURCE`, `-` in a market key becomes `_`) pins one market in one environment whose data lags the shared config. [`DATA_GAPS.md`](DATA_GAPS.md) tracks what keeps the rest on parquet, and what brings each one back.

---

## Changes from the original standalone version

The financial model logic (ARMA-GARCH calibration, copula simulation, liquidation mechanics) is **mathematically unchanged** with two exceptions — the `slippage_calculator_cum` partial-tick fix and the innovation-dispatch fix (each has its own row below) — both of which correct defects rather than change the model's design. The other modifications were made for service integration:

| Change | Reason |
|---|---|
| `main.py` replaced by `runner.py` | Original `main.py` printed results to stdout. `runner.py` is a pure function that accepts typed inputs and returns a typed `CoreModelPipelineResult` dataclass, making it testable and composable. |
| Import paths updated (`from app.risk_engine.core_model.X import Y`) | Required for Python package structure; original used bare module imports only valid when run from the same directory. |
| `Parallel(n_jobs=-1)` changed: `n_jobs=4` in the calibrator backtest, `n_jobs=1` in the Monte Carlo | `-1` consumed all available CPUs and caused OOM in constrained environments. Note the MC has **always run sequentially**: the earlier `Parallel(jobs=4)` was a typo joblib silently swallowed (`Parallel.__init__` forwards unknown kwargs to the backend), leaving `n_jobs` at its default of 1. The explicit `n_jobs=1` changes nothing at runtime — it turns the accident into a decision. Do not "restore" parallelism: per-scenario tensors peak at ~7.5 GiB for the heaviest live market before the `MIN_BORROW_USD` filter (~1.4 GiB after; measured at N_MC=10000), and loky workers would multiply that past any pod memory limit. The backtest parallelises fine — its per-window state is small. |
| `orderbook_data` lookup lowercased (`symbol.lower()`) | Original assumed the working directory was case-insensitive (macOS). Lowercase normalisation is required for Linux where the service runs. |
| Bare `except:` changed to `except Exception:` | Required by the project linter (ruff). |
| `importer.py` reduced to `change_user_ltvs` (later joined by `drop_small_borrowers`, the `MIN_BORROW_USD` filter) | `load_protocol_data` and `load_price_data` were dead code replaced by the `CoreModelDataReader` port. `load_orderbook_data` moved to `ParquetCoreModelDataReader.get_orderbooks()` (also added to the port), so all I/O goes through the same abstraction. `Liquidator` now accepts pre-loaded orderbooks instead of loading them internally
| Dead variable assignments removed (`slippage`, `P`, `new_supply_qty_df`) | Three variables were initialized then immediately overwritten before first use, producing no-op assignments. Removed to reduce noise. |
| `JUMPS + HOURLY_CONV` raises `NotImplementedError` | The original code called `importer.load_data_yahoo()` which never existed in this codebase (yfinance is not a service dependency). The dead call is replaced with an explicit error so the combination is rejected at runtime rather than crashing with `AttributeError`. |
| `# TODO` comments added | Document known bugs in the original code that were not fixed during integration (see **Known Issues** section). |
| `default_params.json` `_comment` extended | States that the schema's min/max/choices are advisory and enforced nowhere — upstream's convention (a human editing overrides in `main.py` with the schema open), kept deliberately. |
| `Forecaster` draws Monte Carlo innovations from the fitted GARCH distribution's own ppf (`_innovations_from_uniform`) | Upstream dispatched on `dist.name.lower().startswith("student")`, which arch's names ("Standardized Student's t", "Standardized Skew Student's t") never match, so **every** fit simulated Gaussian innovations and the persisted CRRs underestimated tail risk (audit C-05). Two further defects hid behind it: upstream's own t-branch used scipy's unstandardized `t.ppf` (variance nu/(nu−2), not the unit variance GARCH innovations require), and a skew-t fit would have been collapsed to a symmetric t (fitted `lambda` ignored). The fix calls `model.distribution.ppf(u, shape)` with the fitted shape parameters (the last `num_params` entries of the parameter vector), so Normal/t/skew-t all simulate exactly what the calibrator fitted; a `garch_model` without a distribution now raises instead of silently falling back to Gaussian, and the dead duplicate dispatcher (`_inverse_cdf_transform` / `_get_garch_distribution`) is removed. This moves every CRR; see [VEC-765](https://linear.app/archontech/issue/VEC-765/core-model-fitted-student-t-innovations-are-silently-replaced-by) and its PR for the before/after runs. |
| `Liquidator.slippage_calculator_cum` prices the partial tick at both ends of the consumed slice | Upstream measured the slice a liquidation consumes as `cum[idx_end] - cum[idx_base]` between whole-tick boundaries. Whenever `already_consumed` and `already_consumed + amount` fell inside the same tick the slice was empty, the average price came out as 0 and the slippage as 0.9999, so the liquidation was never profitable and the position defaulted. On the parquet BTC book that covered every amount below the first tick ($664) from a fresh book and, once about $1M had been consumed, every amount up to $10,000. The fix walks the book exactly: the unfilled part of the start tick, the full ticks in between, and the filled part of the end tick, with a fill inside one tick priced directly as `fill × price`. Everything else (the `price <= sim_price` mask, stored book order, USD-weighted average price, the unfilled-share term, the 0.9999 cap) is unchanged; a book with a non-finite or negative level now raises instead of being priced. This moves CRRs down on markets with many small positions; see [VEC-739](https://linear.app/archontech/issue/VEC-739) and its PR for the before/after runs. |

---

## Supported Protocols

| Protocol | Markets | Data source today |
|---|---|---|
| **SparkLend** | 4 | Live tables (17 Aug 2026): `borrower*` positions, `onchain_token_price`, `cex_orderbook_snapshots` |
| **Morpho** | 2 | Live tables (18 Aug 2026): `morpho_market_position` + the same price/book sources |
| **Maple (Syrup)** | 2 | Live tables (9 Sep 2026): `maple_*` loans, `onchain_token_price` + `asset_price`, `cex_orderbook_snapshots` |
| **Anchorage** | 1 | Parquet snapshots (live reader written; blocked on the frozen upstream feed, ARCT-229 — see DATA_GAPS §3) |
| **Galaxy** | disabled | Parquet market frame only; no position ingestion exists (DATA_GAPS §4) |

---

## Data Sources

The model draws from three distinct data layers, each independently switchable per market between `parquet` (the static snapshots BA shipped, under `inputs/`) and `postgres` (the live adapters under `app/adapters/postgres/core_model_*`).

### 1 — Protocol position data

Borrower-level positions (collateral amounts, debt, LTV, liquidation threshold, liquidation bonus). Live: per-protocol readers in `core_model_positions_reader.py` — SparkLend from the `borrower*` current caches, Morpho from `morpho_market_position`, Syrup one row per external Active Maple loan, Anchorage one row per active custody package (runs parquet until the frozen upstream feed resumes, ARCT-229). Galaxy stays parquet.

### 2 — Price data

Daily closes per modeled collateral, 180+ contiguous days (validated up front). Live: `core_model_price_reader.py` reads `onchain_token_price` pinned to one oracle (SparkLend's, the only feed with a year of gap-free history); token-less assets (XRP, HYPE) read the CoinGecko series in `asset_price`; native BTC/ETH ride the WBTC/WETH oracle series as proxies. Parquet fallback: `inputs/prices_df.parquet` (BA's Yahoo history).

### 3 — Order book / liquidity data

Sell-side depth per book. Live: `core_model_orderbook_reader.py` merges the freshest snapshot per venue from `cex_orderbook_snapshots` (Coinbase, OKX, Kraken; books: BTC, ETH, XRP, HYPE — ETH LSTs proxy the ETH book, BTC wrappers the BTC book). Note the live books hold the top 100 levels per venue side — Known Issue #12. The parquet books are BA's originals, aggregated across 11 venues (Binance, Bybit, OKX, Kraken, Coinbase, Gate.io, KuCoin, Huobi, Bitget, Bitfinex, Crypto.com) with DEX routing for some tokens (cbBTC via a Uniswap V3 pool, HYPE via HyperLiquid) — deeper than the live books, which is why live and parquet CRRs are not directly comparable.

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
| `MIN_BORROW_USD` | `100` | Drop borrowers with less total debt before the liquidation simulation (see **Borrower filter** below; `0` keeps every row) |
| `LOAN_TOKEN` | `USDC` | Filter positions by loan token (`ALL` = no filter) |
| `SEED` | `0` | Global random seed |

All parameters can be overridden via environment variables when running the cronjob — see `app/services/core_model_runner/config.py` for the full mapping.

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

Both backtest gates are currently defective — see Known Issues #7 and #8. Boundary exceedance rates pass Kupiec unconditionally, and long samples collapse the Christoffersen statistic to `p=1`. Until those are fixed, treat selection as BIC plus residual diagnostics, with the VaR backtests contributing little.

### Volatility Floor

To prevent capital requirements from collapsing during low-volatility regimes, the GARCH conditional volatility forecast is floored at the `VOL_FLOOR_PCT` percentile of the 21-day rolling realised volatility computed over the **full historical series** (not just the training window).

---

### Borrower filter

`MIN_BORROW_USD` drops borrowers below that total debt before the liquidation simulation. The
liquidator holds two `(borrowers, N_MC, 15)` float64 tensors, so memory and loop time scale with the
borrower count, and the live positions feed carries many sub-dollar interest-dust rows the reference
parquet snapshots mostly never had (the 5th percentile of debt is about 110 USD on three of the four
SparkLend snapshots; `sparklend_usds` has 10 of 106 rows under 100 USD, holding 0.00004 % of its debt).
The trade-off is explicit: dropped debt can no longer become bad debt, so the CRR is quoted against a
slightly smaller exposure. Measured on live staging data (Sep 2026) at the 100 USD default:

| Market | Rows kept | Dropped debt (share of exposure) | Peak RSS at N_MC=10,000 |
|---|---|---|---|
| sparklend_dai | 378 / 2,204 | 0.0014 % | 7.5 GiB → 1.3 GiB |
| sparklend_usdc | 208 / 343 | 0.0004 % | 1.4 → 0.9 GiB |
| sparklend_usds | 336 / 578 | 0.0001 % | 2.2 → 1.4 GiB |
| sparklend_usdt | 331 / 389 | 0.0001 % | 1.6 → 1.3 GiB |
| morpho_cbbtc-usdc | 322 / 360 | 0.0001 % | 1.5 GiB unfiltered (not re-measured) |
| morpho_weth-usdc | 65 / 114 | 0.014 % | 0.7 GiB unfiltered (not re-measured) |

On the parquet snapshot of sparklend_dai (seed 0, N_MC=1000) the filter drops 0.0001 % of debt at the
100 USD default and moves the EL from 0.013477 % (no filter) to 0.013472 %; a 1,000 USD threshold gives
0.013468 %. The runner logs the dropped count and share on every run. A one-cent threshold does not help
memory (sparklend_dai still peaks at 6.6 GiB) because the extra live rows are dust, not zero-debt.
Before the `slippage_calculator_cum` fix (see **Changes from the original standalone version**) every
dust row defaulted whenever it was unsafe, so the same three runs read 0.017458 %, 0.017452 % and
0.018946 %: dropping debt moved the EL *up*, which was the symptom that exposed the defect.

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

### Step 1 — Seed the local database

Nothing to run: migrations seed every receipt token the mapping references —
the four SparkLend ones (`20260604_…_seed_sparklend_spusdt_receipt_token.sql`,
`20260814_…_seed_sparklend_core_model_receipt_tokens.sql`) and the syrup ones
(`20260702_…_maple_syrup_allocation_exposure.sql`) — so any database that has
migrations applied — `make dev-up`, integration test containers, a plain
migrate run — resolves the mapping at startup.

### Step 2 — Run the pre-compute cronjob

The runner has two modes. Market-specific params (`PROTOCOL`, `LOAN_TOKEN`, etc.) are loaded automatically from `inputs/market_configs.json` — only `CORE_MODEL_MARKET_KEY` is required in both.

**One-shot** — computes, writes, exits. No Temporal. This is what `make dev-up` users want for a quick check, and what a hand-triggered run looks like:

```bash
# From stl-verify/ — defaults to every market against the dev-up database
make run-core-model
make run-core-model MARKET=sparklend_usdt N_MC=200

# Or directly, from stl-verify/python/
DATABASE_URL=postgresql://... \
CORE_MODEL_MARKET_KEY=sparklend_usdt \
uv run python -m cli.cronjobs.core_model_runner.main --once
```

**Scheduled worker** — the default mode, and what the `core-model-runner` Deployment runs. It registers a Temporal schedule on startup and then serves its task queue:

```bash
TEMPORAL_HOST_PORT=localhost:7233 \
DATABASE_URL=postgresql://... \
CORE_MODEL_MARKET_KEY=all \
uv run python -m cli.cronjobs.core_model_runner.main
```

Scheduling follows the repo convention (`CONTRIBUTING.md` §9): Temporal owns the schedule, not Kubernetes. The interval defaults to 24h and is set by `CORE_MODEL_RUN_INTERVAL_HOURS`, but **changing that variable does not move an existing schedule** — delete the schedule in the Temporal UI or CLI and restart the worker.

A tick runs as a single activity with a 4-hour timeout and no retries: the inputs do not change until the next window, and `core_model_results` is append-only, so retrying a partly-finished pass would duplicate rows for the markets that already succeeded. Overlapping runs are skipped for the same reason.

Params are resolved in three layers (lowest wins):
1. `inputs/default_params.json` — canonical defaults
2. `inputs/market_configs.json[market_key]` — per-market overrides
3. `CORE_MODEL_*` env vars — runtime overrides

The full params dict is stored as JSONB in `core_model_results.params` for auditability. The same
JSONB carries one extra lower-case key, `mc_diagnostics` (`convergence.py`): the Monte Carlo standard
error of the EL (`crr_el_se_pct`, and `crr_el_rel_se` = SE / EL) plus the scenario counts behind it
(`n_scenarios`, `n_loss_scenarios`, `n_catastrophic_scenarios` = scenarios losing more than
`catastrophic_loss_pct` of total debt). The service logs a `crr_el not converged` warning with the
reason when `crr_el_rel_se` exceeds `MAX_REL_SE` or fewer than `MIN_CATASTROPHIC_SCENARIOS`
catastrophic scenarios were drawn. At the overlays' `N_MC=10000` eight of nine markets pass; a smoke-test
run at `N_MC=100` warns on most markets (too few catastrophic draws), which is expected.

### Step 3 — Query via the risk API

There is no standalone core-model endpoint. `/v1/risk/rrc` includes the core model's result
in `results[]` (scaled to the given prime's exposure) whenever the asset is mapped:

```
GET /v1/risk/rrc?chain_id=1&token_address={receipt_token_address}&prime_id={address}
```

### asset_id → market_key mapping

To enable a market, add an entry to `mappings/asset_to_market_key.json`:

```json
{
  "1:0xReceiptTokenAddress": "market_key"
}
```

The key is `chain_id:0xAddress` (same format as the SURAF mapping). The value must match a key in `inputs/market_configs.json` and a `market_key` value in `core_model_results`.

**Currently mapped markets** (1:1 receipt token → market):

| Receipt token | Address | Market key |
|---|---|---|
| spDAI | `0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b` | `sparklend_dai` |
| spUSDC | `0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815` | `sparklend_usdc` |
| spUSDS | `0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359` | `sparklend_usds` |
| spUSDT | `0xe7df13b8e3d6740fe17cbe928c7334243d86c92f` | `sparklend_usdt` |
| syrupUSDC | `0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b` | `syrup_usdc` |
| syrupUSDT | `0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d` | `syrup_usdt` |

syrupUSDG is deliberately unmapped — it has no CORE market — and with no other
model applicable `/v1/risk/rrc` answers 404 for it rather than a wrong number.

**Morpho vault shares** are served without a mapping entry: a MetaMorpho vault
spreads one deposit across many Blue markets (n:m), so `CoreModelRiskService`
resolves the vault's live per-market allocations and weights the per-market
results into one figure, reporting `coverage_pct` and the per-market slices in
`details.markets`. Below the configured minimum coverage
(`core_model_min_coverage_pct`, default 50%) the aggregate is withheld and the
caller's model chain falls back.

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
├── convergence.py                Monte Carlo standard error of the EL and the convergence verdict
├── importer.py                   Position preprocessing (MIN_BORROW_USD filter, worst-case LTVs)
├── config.py                     Parameter defaults (inputs/default_params.json)
├── mappings/
│   └── asset_to_market_key.json  Chain/address -> market_key mapping
├── inputs/                       Static parquet snapshots (positions, prices, orderbooks)
└── README.md                     This file

app/ports/
├── core_model_data_reader.py     Port: get_protocol_data, get_prices
├── core_model_results_reader.py  Port: get_latest(market_key)
└── core_model_results_writer.py  Port: insert(result) — the cronjob's write side

app/adapters/
├── composite.py                             Per-input parquet/postgres switch (the *_SOURCE flags)
├── parquet/core_model_data_reader.py        Reads static parquet snapshots
├── postgres/core_model_positions_reader.py  Live positions: SparkLend, Morpho, Syrup, Anchorage
├── postgres/core_model_price_reader.py      Live daily closes: oracle series + asset_price + BTC/ETH proxies
├── postgres/core_model_orderbook_reader.py  Live venue books from cex_orderbook_snapshots
├── postgres/core_model_results_reader.py    Reads core_model_results table
└── postgres/core_model_results_writer.py    Appends to core_model_results (no ON CONFLICT)

app/services/core_model_risk_service.py  RiskModel implementation

app/services/core_model_runner/
├── config.py    Param resolution (defaults -> market config -> env)
├── service.py   The body of one tick: run each market, append via the writer port
└── workflow.py  Temporal workflow; sandboxed, imports nothing from the model

app/adapters/temporal/
└── cronjob.py   Shared harness: connect, ensure/reconcile schedule, run worker

cli/cronjobs/core_model_runner/
├── main.py      Entry point: Temporal worker, or --once. No business logic.
└── activity.py  The activity + tick wiring (engine, writer, reader); the only
                 side of the workflow/activity pair that may import the model
```

---

## Known Issues

Bugs and structural defects in the model code that have not been fixed during integration. Some carry a
matching `# TODO` comment in the source. Parenthesised IDs are the corresponding finding in the
September 2026 Python risk-model audit.

| ID | File | Line | Severity | Description |
|---|---|---|---|---|
| #2 | `liquidator.py` | ~510 | Cosmetic | `final_collat_totals` is never populated — always zero. However `final_total_collateral` is excluded from the `summary_df` subset before any CRR computation and is never read downstream. No metric stored in `core_model_results` is affected. |
| #3 | `backtester.py` | ~111 | High | `hit_backtest` defaults `use_log_returns=False` but production uses `USE_LOG_RETURNS=True`. Kupiec/Christoffersen model selection runs on the wrong return type — the "winning" GARCH model may not be the best for simulation. |
| #4 | `aggregator.py` | ~203 | High | t-Copula `nu` is hardcoded to 3. MLE estimation exists but is disabled. `nu=3` produces very fat tails and is a material assumption that ignores the data. |
| #5 (C-08) | `runner.py` | ~151 | Medium | Jump parameters are calibrated from one token and applied uniformly to all tokens. `JUMP_PARAMS` is reassigned on each collateral iteration and the per-token result entry omits `jump_params`, so `simulate_prices` receives only the last token's calibrated jumps. The per-token override path in `forecaster.py` reads `result_per_token[token].get("jump_params", …)` and is therefore never populated. Triggers whenever `JUMPS=True` with two or more collateral tokens. |
| #7 (C-06) | `backtester.py` | ~38 | High | Kupiec returns `LR=0, p=1` whenever the observed exceedance rate is exactly 0 or 1, so an all-hit or no-hit window passes unconditional coverage unconditionally. For `n=100` all-hit at `alpha=0.05` the likelihood ratio should be `-2 × 100 × log(0.05)`, not zero. `Calibrator.total_fitter` accepts candidates on `p_value_k >= 0.05` and conditional coverage reuses the same LR, so a severely miscalibrated model can win selection. |
| #8 (C-07) | `backtester.py` | ~75 | High | Christoffersen independence multiplies hundreds of sub-unit probabilities into `L0` and `L1` (~72-73), then clips each to the same `1e-10` epsilon. Once both underflow past that floor the ratio is 1, so `LR_ind=0, p=1` regardless of the true likelihood ratio and clustered violations pass. Calibration evaluates once per rolling observation, so a long series hits this routinely. Fix: accumulate the log-likelihoods in log space, handling zero-count terms explicitly. |
| #9 (C-09) | `forecaster.py` | ~270 | High | `brownian_bridge_hourly` detects non-finite `r_cont_hourly`, prints, and substitutes zeros. Degenerate or non-finite daily returns/volatility therefore produce flat price paths instead of aborting, and the pipeline persists the result as an apparently valid CRR. Fix: raise a contextual model-input error before the result reaches the writer. |
| #10 (T-01) | `config.py` | ~102 | Medium | CORE parameters stay an unvalidated `dict[str, Any]` from the loader through `RunnerConfig` and `CoreModelConfig`, so a JSON override of the wrong type reaches simulation unchecked — `"WORST_CASE": "false"` is a non-empty string and activates the truthy branch at `runner.py:107`. Env-var coercion only guards env values, not `market_configs.json` or hand-passed params. Fix: validate parameter types, leaving the deliberately advisory min/max/choices alone. |
| #12 | `core_model_orderbook_reader.py` / `cex_orderbook_snapshots` | | High | The live books hold the top 100 levels per side per venue: measured 7 Sep 2026, the merged ETH bid book is ~$6.2M reaching 0.18 % below mid and BTC ~$10.5M reaching 0.12 %, against parquet sell books of $20.7M / $65.8M within 0.5 % of the top that run down to $0.01. `slippage_calculator_cum` treats the book as the whole market (`add_slippage = (amount - available) / amount` once depth runs out), so on live data any liquidation above a few million USD is unprofitable and defaults, and live CRRs are set by the indexer's level count rather than market liquidity. Live and parquet CRRs are not comparable until the indexer stores deeper books. Tracked in [VEC-740](https://linear.app/archontech/issue/VEC-740). |

### Structural debt

Not wrong numbers — these violate conventions the rest of the tree follows and make the numerics hard to
review. IDs are the audit's.

| ID | File | Description |
|---|---|---|
| D-05 | `runner.py` (~97) | `_run_pipeline` mixes I/O with numerics: three `data_reader` calls, `_load_protection_usd` opening `protocol_defense.json`, then roughly 160 lines of inlined collateral fitting, jump fitting, simulation, time-grid mutation and liquidation. `python/AGENTS.md` requires `risk_engine/` to be pure math with no I/O — inputs belong in the service or adapter, with the numerical stages composed explicitly. `suraf/scoring.py` loads CSVs inside the scoring class the same way. |
| M-01 | `liquidator.py` (~300) | `simulate_liquidations` is one ~448-line function spanning input shaping, margin-call configuration and application, liquidation/default state mutation, recovery accounting and diagnostic printing. This is the comment-delimited, deeply nested orchestration the repo's function-composition rule prohibits. Extract named numerical stages with explicit scenario state so accounting changes can be reviewed on their own. Distinct from D-05, which is about the pipeline entry point. |

---

## API serving shapes (historical notes resolved)

**Morpho — the n:m mismatch is solved by vault aggregation (VEC-654).** The
`receipt_token` table stores MetaMorpho vault addresses, and one vault lends
across many Blue markets while one market takes deposits from many vaults, so
no 1:1 mapping entry can exist. Instead of mapping, `CoreModelRiskService`
resolves a Morpho vault share at request time: the vault's live per-market
supply allocations weight the per-market CORE results into one figure (idle
liquidity at zero risk), with `coverage_pct` and per-market slices reported in
`details.markets`, and the aggregate withheld below the minimum coverage.

**Syrup — served 1:1 since 9 Sep 2026.** The syrup receipt tokens have been in
`receipt_token` since VEC-372 (migration-seeded), and a syrup share maps to
exactly one pool, so syrupUSDC/syrupUSDT are plain mapping entries (table
above), the SparkLend shape.

**Anchorage, Galaxy — still unmapped.** No receipt tokens exist for them; the
cronjob can compute their markets (Anchorage from parquet today), but nothing
serves them through `/v1/risk/rrc`.

## Next Steps

### Galaxy — disabled until it has inputs

`market_configs.json` disables Galaxy (`_galaxy_disabled`). Its market frame
(`market_galaxy.parquet`) uses `ETH`, `SOL`, `JITOSOL`, `XRP`, `BTC` as
collaterals; BA never shipped ETH/SOL/JITOSOL parquet books, so there is no
parquet fallback for those. The **live** venue books for all five collaterals
flow in staging and prod since Sep 2026 (the SOL/JITOSOL books have no
consumer yet), but SOL/JITOSOL price series and — above all — a position
ingestion pipeline do not exist. See DATA_GAPS §4.

### Remaining parquet inputs

Anchorage is the one enabled market still on snapshots (`users_anchorage.parquet`
/ `market_anchorage.parquet`). Its live positions reader is written and tested
(one row per active custody package from `anchorage_package_snapshot`), but the
upstream Anchorage API has returned zero packages since 2026-06-16 (ARCT-229),
so the sources stay parquet until the feed resumes (DATA_GAPS §3 has the flip
step). The
parquet files under `inputs/` are **not updated automatically**: for the live
markets they remain useful only as the dev-cluster fallback and for
before/after comparisons, and their CRRs reflect BA's June 2026 snapshot, not
current protocol state.
