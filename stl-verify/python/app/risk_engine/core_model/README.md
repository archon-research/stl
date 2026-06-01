# CORE - Collateralized Onchain Risk Engine

CORE computes a Collateral Risk Ratio (CRR) for a lending protocol market using
ARMA-GARCH calibration, copula-based Monte Carlo price simulation, and
liquidation mechanics.

## Known Issues

These bugs exist in the original model code and have not been fixed during
integration. They are tracked as TODO comments in the source.

| ID | File | Line | Severity | Description |
|---|---|---|---|---|
| #2 | `liquidator.py` | ~510 | Low | `final_collat_totals` is never populated - always zero. `summary_df['final_total_collateral']` is silent wrong data in every run. |
| #3 | `backtester.py` | ~111 | High | `hit_backtest` defaults `use_log_returns=False` but production uses `USE_LOG_RETURNS=True`. Kupiec/Christoffersen model selection runs on the wrong return type - the "winning" GARCH model may not be the best for simulation. |
| #4 | `aggregator.py` | ~203 | High | t-Copula `nu` is hardcoded to 3. MLE estimation exists but is disabled. `nu=3` produces very fat tails and is a material assumption that ignores the data. |
| #5 | `main.py` / `runner.py` | | Medium | Jump parameters are calibrated from one token and applied uniformly to all tokens. Per-token override path exists in `forecaster.py` but is never populated. |

## Data Sources

Input data is currently loaded from static parquet snapshots in `inputs/`.
The long-term target (per `CORE_MODEL_INTEGRATION_v2.md`) is:
- **Positions / market params**: on-chain via block RPC workers
- **Prices**: existing `offchain-price-indexer` extended to 180-day retention
- **Orderbook depth**: new `orderbook-indexer` cronjob (currently a stub)
