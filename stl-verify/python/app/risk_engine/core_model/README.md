# CORE - Collateralized Onchain Risk Engine

A quantitative framework for computing the **Capital Requirement Ratio (CRR)** across over-collateralised DeFi lending protocols. The model combines ARMA-GARCH price simulation, copula-based cross-asset correlation, an optional compound Poisson jump component, and full liquidation mechanics to estimate the **Expected Loss (EL)** of bad-debt exposure (the primary risk metric) together with concentration diagnostics based on the Herfindahl-Hirschman Index (HHI) of borrower exposures.

Note that CRR is an expected loss, not a tail loss by construction. However, the inputs that generate bad debt in the model are deliberately conservative: volatility is floored at its 75th historical percentile, liquidity is consumed cumulatively across sequential liquidations without replenishment, and joint tail events across collateral assets are modelled using a t-Copula that assigns materially higher probability to simultaneous crashes than standard correlation assumptions: a bad debt event in this model already presupposes a severe stress scenario.

---

## Note

This directory contains the CORE model as integrated into the STL service. The original standalone version lives in [`core_model_copy/`](https://github.com/TWave-code/core_model_copy). The integration wires CORE as a first-class `RiskModel` backed by a pre-compute cronjob and a thin API service that reads the results.

Each market picks its data sources per input via the `*_SOURCE` flags in `inputs/market_configs.json` — `postgres` reads the live tables, `parquet` the static snapshots (that file is the list of which markets run live; `CORE_MODEL_*_SOURCE` env vars override globally, e.g. the dev overlay pins local kind back to parquet). [`DATA_GAPS.md`](DATA_GAPS.md) tracks what keeps the rest on parquet, and what brings each one back.

---

## Changes from the original standalone version

The financial model logic (ARMA-GARCH calibration, copula simulation, liquidation mechanics) is **mathematically unchanged**. The following modifications were made for service integration:

| Change | Reason |
|---|---|
| `main.py` replaced by `runner.py` | Original `main.py` printed results to stdout. `runner.py` is a pure function that accepts typed inputs and returns a typed `CoreModelPipelineResult` dataclass, making it testable and composable. |
| Import paths updated (`from app.risk_engine.core_model.X import Y`) | Required for Python package structure; original used bare module imports only valid when run from the same directory. |
| `Parallel(n_jobs=-1)` changed: `n_jobs=4` in the calibrator backtest, `n_jobs=1` in the Monte Carlo | `-1` consumed all available CPUs and caused OOM in constrained environments. Note the MC has **always run sequentially**: the earlier `Parallel(jobs=4)` was a typo joblib silently swallowed (`Parallel.__init__` forwards unknown kwargs to the backend), leaving `n_jobs` at its default of 1. The explicit `n_jobs=1` changes nothing at runtime — it turns the accident into a decision. Do not "restore" parallelism: per-scenario tensors peak at ~8.0 GiB for the heaviest market (measured at N_MC=10000), and loky workers would multiply that past any pod memory limit. The backtest parallelises fine — its per-window state is small. |
| `orderbook_data` lookup lowercased (`symbol.lower()`) | Original assumed the working directory was case-insensitive (macOS). Lowercase normalisation is required for Linux where the service runs. |
| Bare `except:` changed to `except Exception:` | Required by the project linter (ruff). |
| `importer.py` reduced to `change_user_ltvs` only | `load_protocol_data` and `load_price_data` were dead code replaced by the `CoreModelDataReader` port. `load_orderbook_data` moved to `ParquetCoreModelDataReader.get_orderbooks()` (also added to the port), so all I/O goes through the same abstraction. `Liquidator` now accepts pre-loaded orderbooks instead of loading them internally
| Dead variable assignments removed (`slippage`, `P`, `new_supply_qty_df`) | Three variables were initialized then immediately overwritten before first use, producing no-op assignments. Removed to reduce noise. |
| `JUMPS + HOURLY_CONV` raises `NotImplementedError` | The original code called `importer.load_data_yahoo()` which never existed in this codebase (yfinance is not a service dependency). The dead call is replaced with an explicit error so the combination is rejected at runtime rather than crashing with `AttributeError`. |
| Three `# TODO` comments added | Document known bugs in the original code that were not fixed during integration (see **Known Issues** section). |
| `default_params.json` `_comment` extended | States that the schema's min/max/choices are advisory and enforced nowhere — upstream's convention (a human editing overrides in `main.py` with the schema open), kept deliberately. |

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
| `MIN_BORROW_USD` | `100` | Drop borrowers with less total debt before the liquidation simulation (memory and time scale with borrower count; `0` keeps every row) |
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

Nothing to run: migrations seed the four SparkLend receipt tokens the mapping
references (`20260604_…_seed_sparklend_spusdt_receipt_token.sql` and
`20260814_…_seed_sparklend_core_model_receipt_tokens.sql`), so any database
that has migrations applied — `make dev-up`, integration test containers, a
plain migrate run — resolves the mapping at startup.

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
catastrophic scenarios were drawn. At the overlays' `N_MC=10000` most markets pass; a smoke-test run
at `N_MC=100` warns on every market, which is expected.

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

**Currently mapped markets (SparkLend):**

| Receipt token | Address | Market key |
|---|---|---|
| spDAI | `0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b` | `sparklend_dai` |
| spUSDC | `0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815` | `sparklend_usdc` |
| spUSDS | `0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359` | `sparklend_usds` |
| spUSDT | `0xe7df13b8e3d6740fe17cbe928c7334243d86c92f` | `sparklend_usdt` |

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
├── core_model_results_reader.py  Port: get_latest(market_key)
└── core_model_results_writer.py  Port: insert(result) — the cronjob's write side

app/adapters/
├── parquet/core_model_data_reader.py    Reads static parquet snapshots
├── postgres/core_model_results_reader.py  Reads core_model_results table
└── postgres/core_model_results_writer.py  Appends to core_model_results (no ON CONFLICT)

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
| #6 (C-05) | `forecaster.py` | ~192 | Critical | Student-t distribution check uses `.startswith("student")` but the arch library names the distribution `"Standardized Student's t"`, which starts with `"Standardized"`. The check silently falls through to the Normal branch (`norm.ppf` at ~202), so calibrated heavy tails are replaced by Gaussian innovations and tail risk is **underestimated** in the CRR that `core_model_results` persists. `Backtester.identify_dist` (~86-90) recognises the same names correctly, which is why calibration and simulation disagree. Fix: draw from the *fitted* distribution's `ppf` with its fitted shape parameters — broadening the string match to `"student" in name` is **not sufficient**, because it still ignores the standardized variance and the skew parameter. |
| #7 (C-06) | `backtester.py` | ~38 | High | Kupiec returns `LR=0, p=1` whenever the observed exceedance rate is exactly 0 or 1, so an all-hit or no-hit window passes unconditional coverage unconditionally. For `n=100` all-hit at `alpha=0.05` the likelihood ratio should be `-2 × 100 × log(0.05)`, not zero. `Calibrator.total_fitter` accepts candidates on `p_value_k >= 0.05` and conditional coverage reuses the same LR, so a severely miscalibrated model can win selection. |
| #8 (C-07) | `backtester.py` | ~75 | High | Christoffersen independence multiplies hundreds of sub-unit probabilities into `L0` and `L1` (~72-73), then clips each to the same `1e-10` epsilon. Once both underflow past that floor the ratio is 1, so `LR_ind=0, p=1` regardless of the true likelihood ratio and clustered violations pass. Calibration evaluates once per rolling observation, so a long series hits this routinely. Fix: accumulate the log-likelihoods in log space, handling zero-count terms explicitly. |
| #9 (C-09) | `forecaster.py` | ~270 | High | `brownian_bridge_hourly` detects non-finite `r_cont_hourly`, prints, and substitutes zeros. Degenerate or non-finite daily returns/volatility therefore produce flat price paths instead of aborting, and the pipeline persists the result as an apparently valid CRR. Fix: raise a contextual model-input error before the result reaches the writer. |
| #10 (T-01) | `config.py` | ~102 | Medium | CORE parameters stay an unvalidated `dict[str, Any]` from the loader through `RunnerConfig` and `CoreModelConfig`, so a JSON override of the wrong type reaches simulation unchecked — `"WORST_CASE": "false"` is a non-empty string and activates the truthy branch at `runner.py:107`. Env-var coercion only guards env values, not `market_configs.json` or hand-passed params. Fix: validate parameter types, leaving the deliberately advisory min/max/choices alone. |

### Structural debt

Not wrong numbers — these violate conventions the rest of the tree follows and make the numerics hard to
review. IDs are the audit's.

| ID | File | Description |
|---|---|---|
| D-05 | `runner.py` (~97) | `_run_pipeline` mixes I/O with numerics: three `data_reader` calls, `_load_protection_usd` opening `protocol_defense.json`, then roughly 160 lines of inlined collateral fitting, jump fitting, simulation, time-grid mutation and liquidation. `python/AGENTS.md` requires `risk_engine/` to be pure math with no I/O — inputs belong in the service or adapter, with the numerical stages composed explicitly. `suraf/scoring.py` loads CSVs inside the scoring class the same way. |
| M-01 | `liquidator.py` (~300) | `simulate_liquidations` is one ~448-line function spanning input shaping, margin-call configuration and application, liquidation/default state mutation, recovery accounting and diagnostic printing. This is the comment-delimited, deeply nested orchestration the repo's function-composition rule prohibits. Extract named numerical stages with explicit scenario state so accounting changes can be reviewed on their own. Distinct from D-05, which is about the pipeline entry point. |

---

## Next Steps

### Morpho — receipt token mapping is not straightforward

The current `asset_to_market_key.json` mapping assumes a 1:1 relationship between an on-chain receipt token and a core model market key. This works cleanly for SparkLend (one spToken per loan token), but **does not work for Morpho Blue** for the following reason:

- The STL `receipt_token` table stores **MetaMorpho vault** addresses (e.g. steakUSDC, bbqUSDC). A single MetaMorpho vault lends USDC across many Morpho Blue markets simultaneously — it may be exposed to cbBTC, WETH, and other collaterals at the same time.
- The core model market keys `morpho_cbbtc-usdc` and `morpho_weth-usdc` represent **all Morpho Blue borrowers** using a given collateral/loan pair across the entire protocol, regardless of which vault is lending to them.
- There is an **n:m mismatch**: one vault → many collateral markets, one market → many vaults. No single receipt token maps 1:1 to a core model Morpho market key.

**Options to resolve:**

1. **Pick a representative vault per market key** (pragmatic, approximate): choose the largest MetaMorpho vault that primarily exposes to the target collateral and accept it as a proxy. This is imprecise but unblocks the API.
2. **Virtual receipt tokens**: introduce a synthetic receipt token in the DB (not backed by a real on-chain address) to represent the aggregate Morpho cbBTC/USDC or WETH/USDC market. Requires a schema decision.
3. **Separate query path**: add a market-key-based endpoint that bypasses receipt token resolution entirely — useful if the Morpho core model result is consumed without a specific prime's exposure context.

Until this is resolved, `morpho_cbbtc-usdc` and `morpho_weth-usdc` remain configured in `market_configs.json` and can be run by the cronjob, but cannot be served through the `asset_to_market_key.json` mapping or `/v1/risk/rrc`.

### Syrup, Anchorage, Galaxy — no receipt tokens in the DB

These three protocols do not appear in the STL `receipt_token` table. They are off-chain or institutional clients without on-chain receipt tokens tracked by the watcher. Before these markets can be wired into the API, they require:

- Protocol entries in the `protocol` table
- A mechanism to track user positions (off-chain feed or watcher extension)
- Receipt token rows for their position tokens (if any)

The cronjob can still run these markets against the parquet snapshots -- only the API mapping is blocked.

### Galaxy -- missing ETH, SOL, and JITOSOL order books

The Galaxy market data (`market_galaxy.parquet`) uses `ETH`, `SOL`, `JITOSOL`, `XRP`, `BTC` as collateral token symbols. `XRP` and `BTC` already have matching order book files. The three remaining collaterals are blocked:

- `eth_sell_orderbook.parquet` -- missing. Galaxy uses the unwrapped `ETH` symbol; the existing file is `weth_sell_orderbook.parquet` (used by SparkLend/Morpho markets which report `WETH`). These need to be treated as the same asset or a separate `eth_sell_orderbook.parquet` file needs to be provided.
- `sol_sell_orderbook.parquet` -- missing, needed for SOL-collateralised positions
- `jitosol_sell_orderbook.parquet` -- missing, needed for JitoSOL-collateralised positions

Until these are provided, the Galaxy cronjob will fail at the liquidity loading step.

Note: `importer.load_orderbook_data` now lowercases all symbol names before constructing filenames, fixing a latent case-sensitivity bug that would have caused other markets to fail on Linux (e.g. `WETH` would have looked for `WETH_sell_orderbook.parquet` on a case-sensitive filesystem).

### Parquet data is temporary

All position, price, and order book data is currently loaded from static snapshots in `inputs/`. These files are a temporary scaffold to enable early development and testing -- they are **not updated automatically** and will become stale. CRR results computed from them reflect a historical snapshot, not current protocol state.

The long-term target for each data layer:
- **Position data** -- live on-chain via the existing block RPC watcher workers (same pipeline used by SparkLend today)
- **Price data** -- the existing `offchain-price-indexer`, extended to 180-day retention
- **Order book data** -- a new `orderbook-indexer` cronjob

Until that pipeline is complete, the parquet files in `inputs/` must be manually refreshed to keep results meaningful.
