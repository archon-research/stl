# CORE Model Integration Plan

Steps to wire CORE (Collateralized Onchain Risk Engine) into STL as a first-class `RiskModel`. The model is solid; the work is architectural plumbing.

---
## Recommended sequence

| Step | Notes |
|---|---|
| 1 — Fix bugs #3 and #4 | Isolated PR; no integration risk |
| 3 — Refactor imports | Mechanical; verifiable by running existing tests |
| 4 — Add dependencies | Validate ARM64 build early |
| 5 — Domain types | Small; enables parallel work downstream |
| 2a/2b — Position + price pipelines | Biggest work; determine protocol coverage first |
| 6 — DB data reader + results reader ports | Unblocks both the cronjob and the service |
| 7 — `asset_id → market_key` mapping file | Small; needed before service can dispatch |
| 8 — Temporal compute cronjob | Separate PR from the API service |
| 9 — `CoreModelRiskService` | Thin once the pre-computed table exists |
| 10 — Config cleanup | Small, any time |
| 11 — Registry wiring + startup (lifespan) | Final integration |
| 12 — Tests | Alongside each step, not at the end |
| 2c — Orderbook ingestion | Parallel initiative; stub in adapter until ready |

---

## Blockers summary

| Blocker | Severity | Notes |
|---|---|---|
| Galaxy + Anchorage data sourcing | High | Off-chain data; needs explicit maintainer approval and justification per CONTRIBUTING.md §5 |
| Orderbook ingestion infrastructure | High | CEX API keys, venue coverage, DB schema, ingestion cadence all undefined; no existing cronjob |
| Bug #3 (backtest/simulation return type mismatch) | High | Produces wrong GARCH model selection in every run |
| Bug #4 (hardcoded t-Copula nu=3) | High | Material risk model assumption; MLE code exists but is disabled |
| ARM64 Docker build with BLAS/LAPACK | Medium | arch + statsmodels add native binaries; validate build time and image size on Graviton before merging |
| Price history completeness in DB | Medium | Need 180-day daily OHLCV for every collateral token; verify coverage before building the reader |
| Async / blocking compute | Medium | Resolved by the pre-compute architecture; do not run inline in the API |


## Constraints from CONTRIBUTING.md

These are non-negotiable and shape most of the decisions below:

- **Data pipelines and model pipelines must stay separate.** Ingest writes to the DB; the model reads from the DB. CORE's current pattern — loading parquet snapshots inline — merges both. That must be split before anything else.
- **On-chain data must come from chain RPC or our cached block payload,** not from third-party indexers. Off-chain feeds (prices, orderbook) need an explicit justification and maintainer approval.
- **Every timeseries table needs hypertable + compression + S3 tiering** in the same migration it is created.
- **Python deps are managed with `uv`,** not pip. `uv add <pkg>` from `stl-verify/python/`.
- **Temporal is the scheduler** for periodic jobs, not k8s CronJobs.
- **Model code:** `app/risk_engine/<model>/`. Service: `app/services/<model>_service.py`. API unchanged (`app/api/v1/risk.py`).

---

## Current state

CORE lives in `core_model_copy/` repo:
- Reads user positions, market params, prices, and orderbook depth from local parquet snapshots.
- Runs GARCH calibration + Monte Carlo + liquidation mechanics inline.
- Prints results to stdout.

The service uses a hexagonal `RiskModel` protocol (`applies_to` + `compute` → `RrcResult`), a `ModelRegistry`, and a PostgreSQL-backed data layer. SURAF and gap-sweep already follow this pattern.

---

## Detailed steps

### 1. Fix known bugs before integrating (from ANALYSIS.md)

These will produce wrong results in production and should be addressed in a standalone PR before any integration work begins:

| File | Issue | Severity |
|---|---|---|
| `backtester.py:111` | Backtest uses simple returns, simulation uses log returns — corrupts GARCH model selection | High |
| `aggregator.py:203` | t-Copula `nu` hardcoded to 3; MLE estimation is commented out — material modeling assumption | High |
| `main.py:150` | Jump params calibrated from one token applied to all tokens; per-token path exists but unused | Medium |
| `liquidator.py:509` | `final_collat_totals` never populated — always zero, silent wrong data | Low |

---

### 2. Split ingest from model — build data pipelines first (BLOCKER)

**This is the mandatory first step per CONTRIBUTING.md §5.** CORE currently mixes data loading with model computation. The correct pattern:

> _Data pipelines write "what happened" into Postgres. Model pipelines read from Postgres and write "what it means"._

Four data sources need DB-backed pipelines before the model can run in the service:

#### 2a. User borrower positions + market parameters

On-chain data — should come from the chain RPC or cached block payload. The morpho-indexer and sparklend-position-tracker workers already index some of this. Determine coverage per protocol:

- Does the indexed data already contain per-user collateral balances, LTVs, and liquidation thresholds for all five protocols (Morpho, SparkLend, Maple, Galaxy, Anchorage)?
- For gaps: add new per-block workers in `cmd/workers/<protocol>-position-tracker/` (Go) or `cli/workers/<protocol>-position-tracker/` (Python).

**Blocker for Galaxy and Anchorage**: these are not DeFi protocols with on-chain state. Their data is off-chain. This requires explicit approval from maintainers (CONTRIBUTING.md §5) and a documented justification. Off-chain adapters would go in `cmd/cronjobs/` (periodic pull) or `cli/cronjobs/` (Python equivalent).

#### 2b. Price history (180-day OHLCV)

Needed for GARCH calibration. A training window of 180 days of daily close prices must be available in the DB for every collateral token the model covers.

Determine whether the existing `offchain-price-indexer` cronjob covers all required tokens and whether the retention window is ≥ 180 days. If not: extend it, or add a backfiller for historical gaps.

Any off-chain price source (CoinGecko, etc.) needs an explicit justification — on-chain oracle prices from the block cache are preferred if available at daily granularity.

**Blocker**: Verify coverage and retention before starting the DB reader.

#### 2c. Sell-side orderbook depth

Used by `liquidator.py` for slippage modeling. Currently static parquet snapshots from 12+ CEX venues and Uniswap V3.

This needs a new periodic cronjob (`cli/cronjobs/orderbook-indexer/`) that fetches live orderbook depth and writes snapshots to a DB table keyed by `(token, captured_at)`. CEX venue API keys and DeFi AMM on-chain read infrastructure must be provisioned via the infra repo.

**Blocker**: Largest operational dependency. CEX API keys, venue coverage, table schema, and ingestion cadence are all undefined. Treat as a separate initiative. In the meantime, stub the adapter with the static parquet fallback so model development can proceed.

#### 2d. Migrations for new tables

For each new table (positions snapshot, price history if not already covered, orderbook snapshots): write a new SQL migration in `stl-verify/db/migrations/`. Every timeseries table must include `create_hypertable`, compression policy, and S3 tiering policy in the same migration. Never modify an existing migration file.

---

### 3. Refactor module imports

CORE uses bare imports (`from calibrator import ...`). The service uses the `app.` package namespace — these will break at import time.

Change all internal imports to absolute form:

```python
# Before
from calibrator import Calibrator
import importer

# After
from app.risk_engine.core_model.calibrator import Calibrator
from app.risk_engine.core_model import importer
```

Add `__init__.py` files as needed.

---

### 4. Add heavy dependencies via `uv`

Run from `stl-verify/python/`. Do not use pip directly.

```bash
uv add arch statsmodels scipy joblib
```

Packages that are **not** needed in the service (the `core_model/requirements.txt` is the standalone script's file and is not used by the service): `streamlit`, `plotly`, `psycopg2-binary` (service uses asyncpg), `yfinance` (replace with DB price reader).

**Blocker**: `arch` and `statsmodels` pull in BLAS/LAPACK native binaries. Validate ARM64 (Graviton) Docker build time and image size delta before merging. Build failure here blocks all deployment.

---

### 5. Add `core_model` to the domain type system

In `app/domain/entities/risk.py`:

1. Extend `ModelName`: `Literal["suraf", "gap_sweep", "core_model"]`
2. Add `CoreModelDetails(BaseModel)` carrying CORE's output fields: `crr_el_pct`, `hhi`, `pl_pct`, `pd_pct`, `delta_ltv_pct`, `n_mc`, `protocol`, `forecast_step`, and `copula_type`
3. Register in `_RISK_MODEL_TO_DETAILS`

The `_missing`/`_extra` guard at module load will catch drift here at startup time.

---

### 6. Build a DB data reader port + adapter

Create two ports:

**`app/ports/core_model_data_reader.py`** — input data for the compute cronjob:

```python
class CoreModelDataReader(Protocol):
    async def get_users_df(self, protocol: str, ...) -> pd.DataFrame: ...
    async def get_market_df(self, protocol: str, ...) -> pd.DataFrame: ...
    async def get_prices_df(self, collateral_list: list[str]) -> pd.DataFrame: ...
    async def get_orderbook(self, collateral: str) -> pd.DataFrame: ...
```

**`app/ports/core_model_results_reader.py`** — reads pre-computed CRR from the results table; used by `CoreModelRiskService`:

```python
class CoreModelResultsReader(Protocol):
    async def get_latest(self, market_key: str) -> CoreModelResult | None: ...
```

**Note on DataFrames in ports**: returning raw `pd.DataFrame` from a port is a deliberate exception to the usual domain-entity pattern, justified because CORE's entire compute stack operates on DataFrames and wrapping them into domain objects would require rebuilding the same structure one layer up. The boundary is still clean — adapters own the SQL; CORE's modules own the DataFrame schema.

Implement both as `app/adapters/postgres/core_model_data_reader.py` and `app/adapters/postgres/core_model_results_reader.py`. For the orderbook adapter, fall back to the static parquet files until the ingestion job is live (log a warning).

Replace all calls to `importer.load_*` in `main.py` with calls to `CoreModelDataReader`.




---

### 7. Create `asset_id → market_key` mapping

Add a JSON mapping file (same pattern as `app/risk_engine/mapping.py` / SURAF's asset mapping) that resolves each `asset_id` to a `market_key` string (e.g., `"morpho_cbbtc_usdc"`). This `market_key` is the key used in the `core_model_results` table.

Load and validate at startup in `app/main.py` (lifespan), fail fast on any unknown or malformed entry. Stash on `app.state` and expose via `app/api/deps.py`.

---

### 8. Pre-compute architecture for the model pipeline

**Key constraint**: GARCH calibration + 10,000-scenario Monte Carlo runs for several minutes per protocol market. Calling this synchronously inside an async FastAPI handler will block the event loop and starve all other requests. This must be pre-computed on a schedule.

**The correct pattern** (aligns with CONTRIBUTING.md §5 model pipeline pattern):

1. A Temporal cronjob (`cli/cronjobs/core-model-runner/`) runs on a schedule (e.g., daily or after each data refresh), executes the full CORE pipeline for each configured protocol market, and writes one CRR row per market to a `core_model_results` table.
2. `CoreModelRiskService.compute()` reads the latest market CRR and the prime's USD exposure — both sub-second DB queries — then multiplies.

This is the same split as "data pipeline writes, model pipeline reads" applied one level deeper: the compute cronjob is the "model writer"; the API service is the "model reader."

#### Temporal cronjob skeleton (`cli/cronjobs/core-model-runner/main.py`)

Follow the Python cronjob pattern from CONTRIBUTING.md §9. The cronjob worker:
- Connects to Temporal on startup
- Registers a schedule (idempotent via `AlreadyExists` swallow)
- Each tick runs the full CORE pipeline for all configured protocol markets and writes rows to `core_model_results`

---

### 9. Implement `CoreModelRiskService`

Create `app/services/core_model_risk_service.py`:

- `risk_model: ModelName = "core_model"`
- `applies_to(asset_id, prime_id)` — returns `True` if `asset_id` maps to a known protocol market in the `asset_id → market_key` config. CORE and gap-sweep may both apply to the same asset — the registry returns all applicable models.
- `async compute(asset_id, prime_id, overrides)`:
  1. Resolve `asset_id → market_key` from the mapping
  2. Load the latest pre-computed result via `CoreModelResultsReader.get_latest(market_key)` (port defined in step 6)
  3. Load `prime_usd_exposure` via `AllocationRepository.get_usd_exposure(asset_id, prime_id)`
  4. `rrc_usd = prime_usd_exposure × crr_el_pct / 100`
  5. Return `RrcResult` with `CoreModelDetails`
  - If `get_latest` returns `None`, raise a clear error rather than running inline

---

### 10. `protocol_defense.json` → Settings or DB

`_load_protection_usd()` in `main.py` reads a static JSON file. In the service, this should come from either:
- An env-variable-backed field in `Settings` (for per-deployment configuration), or
- A DB table if values change frequently or need audit trail.

The static file is fine for local dev; it must not be the source in prod.

---

### 11. Wire into `ModelRegistry` and app startup

In `app/main.py`:

1. In the `lifespan` context manager (before `yield`), load and validate the `asset_id → market_key` mapping — fail startup on any invalid entry.
2. Instantiate `PostgresCoreModelDataReader` (step 6) and `PostgresCoreModelResultsReader` (step 6).
3. Load CORE config from `Settings` (FORECAST_STEP, N_MC, COPULA_TYPE, etc. from env vars, defaulting to `config.DEFAULTS`).
4. Instantiate `CoreModelRiskService`.
5. Pass alongside `SurafRrcService` and `CryptoLendingRiskService` to `ModelRegistry`.

Stash anything needed by request handlers on `app.state.<name>` and expose via `app/api/deps.py`.

---

### 12. Tests

Per CLAUDE.md / CONTRIBUTING.md §12:

- **Unit tests for `CoreModelRiskService`**: mock the DB reader port; test `applies_to`, `compute` with fixture results, override validation, "no result yet" edge case.
- **Unit tests for the Temporal cronjob service**: mock the `CoreModelDataReader` port and DB writer; test the pipeline logic with small fixture DataFrames (N_MC=10, FORECAST_STEP=2).
- **Integration tests**: full pipeline against real Postgres (testcontainers); mock only CEX/external APIs.
- Services require 100% coverage.

---

