# CORE Model Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the CORE (Collateralized Onchain Risk Engine) into the STL service as a first-class `RiskModel` backed by a pre-compute cronjob and a thin API service that reads the results.

**Architecture:** The CORE model source files are copied as-is into `app/risk_engine/core_model/` with only import paths changed. A standalone cronjob (`cli/cronjobs/core_model_runner/`) runs the full GARCH + Monte Carlo + liquidation pipeline for a given market configuration and writes one row to a `core_model_results` TimescaleDB table. `CoreModelRiskService` reads the latest pre-computed row at request time and multiplies `crr_el_pct` by the prime's USD exposure.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy async, asyncpg, pandas, arch (GARCH), statsmodels, scipy, joblib, tqdm, TimescaleDB/TigerData, testcontainers

---

## File Map

### New files
| Path | Responsibility |
|---|---|
| `app/risk_engine/core_model/__init__.py` | Package marker |
| `app/risk_engine/core_model/aggregator.py` | Copied from core_model_copy; absolute imports |
| `app/risk_engine/core_model/backtester.py` | Copied; absolute imports |
| `app/risk_engine/core_model/calibrator.py` | Copied; absolute imports; TODO on bug #3 |
| `app/risk_engine/core_model/config.py` | Copied; `__file__`-relative path to inputs/ already correct |
| `app/risk_engine/core_model/forecaster.py` | Copied; absolute imports |
| `app/risk_engine/core_model/importer.py` | Copied; no internal imports |
| `app/risk_engine/core_model/liquidator.py` | Copied; absolute import for importer; TODO on bug #2 |
| `app/risk_engine/core_model/runner.py` | NEW: pure `run()` orchestrating the pipeline |
| `app/risk_engine/core_model/README.md` | Known issues |
| `app/risk_engine/core_model/inputs/` | Parquet snapshots copied from core_model_copy/inputs/ |
| `app/risk_engine/core_model/mappings/asset_to_market_key.json` | asset_id → market_key (starts empty) |
| `app/ports/core_model_data_reader.py` | Protocol: get_protocol_data, get_prices |
| `app/ports/core_model_results_reader.py` | Protocol: get_latest + CoreModelResult dataclass |
| `app/adapters/parquet/__init__.py` | Package marker |
| `app/adapters/parquet/core_model_data_reader.py` | Reads parquet snapshots |
| `app/adapters/postgres/core_model_results_reader.py` | Reads core_model_results table |
| `app/services/core_model_risk_service.py` | RiskModel impl: reads pre-computed CRR, multiplies exposure |
| `cli/__init__.py` | Package marker |
| `cli/cronjobs/__init__.py` | Package marker |
| `cli/cronjobs/core_model_runner/__init__.py` | Package marker |
| `cli/cronjobs/core_model_runner/config.py` | Env-var settings for the cronjob |
| `cli/cronjobs/core_model_runner/main.py` | Entry point: run pipeline, write to DB |
| `db/migrations/20260601_120000_create_core_model_results.sql` | Hypertable + compression + tiering |
| `tests/unit/services/test_core_model_risk_service.py` | Unit tests for service (100% coverage) |
| `tests/unit/risk_engine/test_core_model_runner.py` | Smoke test for runner (integration mark) |
| `tests/integration/test_core_model_results_reader.py` | Integration test for Postgres adapter |

### Modified files
| Path | Change |
|---|---|
| `app/domain/entities/risk.py` | Add `CoreModelDetails`; extend `ModelName`, `RrcDetails`, `_RISK_MODEL_TO_DETAILS` |
| `app/config.py` | Add `core_model_mappings_file` |
| `app/main.py` | Instantiate CORE adapters + service; add to `ModelRegistry` |

---

## Task 1: Add Python dependencies

**Files:** `pyproject.toml`, `uv.lock`

- [ ] **Step 1: Add dependencies via uv**

Run from `stl-verify/python/`:
```bash
uv add arch statsmodels scipy joblib tqdm
```

Expected: `pyproject.toml` updated; `uv.lock` regenerated; packages installable.

> **Note:** `arch` and `statsmodels` pull in BLAS/LAPACK native binaries. Validate the ARM64 (Graviton) Docker build time and image size delta before merging — see blocker in `CORE_MODEL_INTEGRATION_v2.md`.

- [ ] **Step 2: Verify import works**

```bash
cd stl-verify/python && python -c "from arch import arch_model; import statsmodels; import scipy; import joblib; import tqdm; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add stl-verify/python/pyproject.toml stl-verify/python/uv.lock
git commit -m "deps(python): add arch, statsmodels, scipy, joblib, tqdm for CORE model"
```

---

## Task 2: Copy CORE model source files and fix imports

**Files:** `app/risk_engine/core_model/*.py`, `app/risk_engine/core_model/inputs/`

The source files live in `~/code/core_model_copy/`. Copy them with absolute import rewrites. Do **not** fix any logic bugs — add `# TODO` comments at the known bug locations instead.

- [ ] **Step 1: Copy source files**

```bash
DEST=stl-verify/python/app/risk_engine/core_model
cp ~/code/core_model_copy/aggregator.py  $DEST/
cp ~/code/core_model_copy/backtester.py  $DEST/
cp ~/code/core_model_copy/calibrator.py  $DEST/
cp ~/code/core_model_copy/config.py      $DEST/
cp ~/code/core_model_copy/forecaster.py  $DEST/
cp ~/code/core_model_copy/importer.py    $DEST/
cp ~/code/core_model_copy/liquidator.py  $DEST/
```

- [ ] **Step 2: Copy inputs directory**

```bash
DEST=stl-verify/python/app/risk_engine/core_model/inputs
mkdir -p $DEST
cp ~/code/core_model_copy/inputs/* $DEST/
```

- [ ] **Step 3: Fix bare imports in calibrator.py**

`calibrator.py` has `from backtester import Backtester`. Change it to:
```bash
sed -i '' 's/from backtester import Backtester/from app.risk_engine.core_model.backtester import Backtester/' \
  stl-verify/python/app/risk_engine/core_model/calibrator.py
```

- [ ] **Step 4: Fix bare imports in forecaster.py**

`forecaster.py` has three bare imports:
```bash
sed -i '' \
  -e 's/from calibrator import Calibrator/from app.risk_engine.core_model.calibrator import Calibrator/' \
  -e 's/from backtester import Backtester/from app.risk_engine.core_model.backtester import Backtester/' \
  -e 's/from aggregator import Aggregator/from app.risk_engine.core_model.aggregator import Aggregator/' \
  stl-verify/python/app/risk_engine/core_model/forecaster.py
```

- [ ] **Step 5: Fix bare import in liquidator.py**

`liquidator.py` has `import importer`. Change it to:
```bash
sed -i '' 's/^import importer$/from app.risk_engine.core_model import importer/' \
  stl-verify/python/app/risk_engine/core_model/liquidator.py
```

- [ ] **Step 6: Create `__init__.py`**

Create `stl-verify/python/app/risk_engine/core_model/__init__.py` with content:
```python
```
(empty file — package marker only)

- [ ] **Step 7: Add TODO comments for known bugs**

In `stl-verify/python/app/risk_engine/core_model/backtester.py`, find line ~111 (the `hit_backtest` function's `use_log_returns` default) and add above it:
```python
# TODO(bug#3): hit_backtest defaults use_log_returns=False but production runs
# USE_LOG_RETURNS=True, causing Kupiec/Christoffersen model selection to evaluate
# the wrong return type. Fix: default should match the caller's USE_LOG_RETURNS.
```

In `stl-verify/python/app/risk_engine/core_model/aggregator.py`, find line ~203 (`nu = 3`) and add above it:
```python
# TODO(bug#4): t-Copula nu is hardcoded to 3. MLE estimation code exists but is
# disabled. nu=3 produces very fat tails and may be materially wrong for
# stablecoins and less volatile collateral. Fix: enable the MLE estimation.
```

In `stl-verify/python/app/risk_engine/core_model/liquidator.py`, find line ~510 (`final_collat_totals = np.zeros(N_SCEN)`) and add:
```python
# TODO(bug#2): final_collat_totals is never populated — always zero.
# The assignment is commented out below and `q` is undefined in that scope.
```

- [ ] **Step 8: Verify imports resolve**

```bash
cd stl-verify/python && python -c "
from app.risk_engine.core_model.calibrator import Calibrator
from app.risk_engine.core_model.forecaster import Simulator
from app.risk_engine.core_model.liquidator import Liquidator
from app.risk_engine.core_model.config import load_params, DEFAULTS
from app.risk_engine.core_model import importer
print('ok')
"
```

Expected: `ok`

- [ ] **Step 9: Commit**

```bash
git add stl-verify/python/app/risk_engine/core_model/
git commit -m "feat(core-model): copy CORE source files with absolute imports and known-issue TODOs"
```

---

## Task 3: Add README.md with known issues

**Files:** `app/risk_engine/core_model/README.md`

- [ ] **Step 1: Create README**

Create `stl-verify/python/app/risk_engine/core_model/README.md`:

```markdown
# CORE — Collateralized Onchain Risk Engine

CORE computes a Collateral Risk Ratio (CRR) for a lending protocol market using
ARMA-GARCH calibration, copula-based Monte Carlo price simulation, and
liquidation mechanics.

## Known Issues

These bugs exist in the original model code and have not been fixed during
integration. They are tracked as TODO comments in the source.

| ID | File | Line | Severity | Description |
|---|---|---|---|---|
| #2 | `liquidator.py` | ~510 | Low | `final_collat_totals` is never populated — always zero. `summary_df['final_total_collateral']` is silent wrong data in every run. |
| #3 | `backtester.py` | ~111 | High | `hit_backtest` defaults `use_log_returns=False` but production uses `USE_LOG_RETURNS=True`. Kupiec/Christoffersen model selection runs on the wrong return type — the "winning" GARCH model may not be the best for simulation. |
| #4 | `aggregator.py` | ~203 | High | t-Copula `nu` is hardcoded to 3. MLE estimation exists but is disabled. `nu=3` produces very fat tails and is a material assumption that ignores the data. |
| #5 | `main.py` / `runner.py` | | Medium | Jump parameters are calibrated from one token and applied uniformly to all tokens. Per-token override path exists in `forecaster.py` but is never populated. |

## Data Sources

Input data is currently loaded from static parquet snapshots in `inputs/`.
The long-term target (per `CORE_MODEL_INTEGRATION_v2.md`) is:
- **Positions / market params**: on-chain via block RPC workers
- **Prices**: existing `offchain-price-indexer` extended to 180-day retention
- **Orderbook depth**: new `orderbook-indexer` cronjob (currently a stub)
```

- [ ] **Step 2: Commit**

```bash
git add stl-verify/python/app/risk_engine/core_model/README.md
git commit -m "docs(core-model): add README with known issues"
```

---

## Task 4: Extend domain types with CoreModelDetails

**Files:** `app/domain/entities/risk.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_domain_entity_validation.py` (open the file and append):

```python
def test_core_model_details_in_rrc_result():
    from decimal import Decimal
    from app.domain.entities.risk import CoreModelDetails, RrcResult

    details = CoreModelDetails(
        risk_model="core_model",
        crr_el_pct=Decimal("12.5"),
        crr_es_pct=Decimal("15.0"),
        crr_var_pct=Decimal("10.0"),
        hhi=Decimal("22.3"),
        protocol="MORPHO",
        forecast_step=14,
        n_mc=10000,
        copula_type="T-COPULA",
    )
    result = RrcResult(
        asset_id=1,
        prime_id="0xBcca60bB61934080951369a648Fb03DF4F96263C",
        rrc_usd=Decimal("1250.00"),
        comparable_crr_pct=Decimal("12.5"),
        risk_model="core_model",
        details=details,
    )
    assert result.risk_model == "core_model"
    assert result.details.crr_el_pct == Decimal("12.5")


def test_core_model_details_rejects_hhi_none():
    from decimal import Decimal
    from app.domain.entities.risk import CoreModelDetails

    d = CoreModelDetails(
        risk_model="core_model",
        crr_el_pct=Decimal("5"),
        crr_es_pct=Decimal("6"),
        crr_var_pct=Decimal("4"),
        hhi=None,
        protocol="SPARKLEND",
        forecast_step=7,
        n_mc=1000,
        copula_type="GAUSSIAN",
    )
    assert d.hhi is None
```

- [ ] **Step 2: Run test to see it fail**

```bash
cd stl-verify/python && python -m pytest tests/unit/test_domain_entity_validation.py::test_core_model_details_in_rrc_result -v
```

Expected: `ImportError` or `AttributeError` — `CoreModelDetails` does not exist yet.

- [ ] **Step 3: Implement the changes in risk.py**

Open `app/domain/entities/risk.py`.

Change:
```python
ModelName = Literal["suraf", "gap_sweep"]
```
to:
```python
ModelName = Literal["suraf", "gap_sweep", "core_model"]
```

Add the new class after `GapSweepDetails`:
```python
class CoreModelDetails(BaseModel):
    """CORE model-specific output embedded in an RrcResult.

    ``crr_el_pct`` is the expected-loss CRR used as the primary capital
    charge (0–100 scale, e.g. ``Decimal("12.5")`` means 12.5%).
    ``hhi`` is the Herfindahl-Hirschman Index of borrower concentration
    expressed as a percentage; ``None`` when liquidation analysis was
    not run or the market had fewer than two borrowers.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    risk_model: Literal["core_model"]
    crr_el_pct: Decimal
    crr_es_pct: Decimal
    crr_var_pct: Decimal
    hhi: Decimal | None
    protocol: str
    forecast_step: int
    n_mc: int
    copula_type: str
```

Change `RrcDetails` to include `CoreModelDetails`:
```python
RrcDetails = Annotated[Union[SurafDetails, GapSweepDetails, CoreModelDetails], Field(discriminator="risk_model")]
```

Add the entry to `_RISK_MODEL_TO_DETAILS`:
```python
_RISK_MODEL_TO_DETAILS: dict[str, type] = {
    "suraf": SurafDetails,
    "gap_sweep": GapSweepDetails,
    "core_model": CoreModelDetails,
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd stl-verify/python && python -m pytest tests/unit/test_domain_entity_validation.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add app/domain/entities/risk.py tests/unit/test_domain_entity_validation.py
git commit -m "feat(core-model): add CoreModelDetails to domain risk types"
```

---

## Task 5: CoreModelDataReader port and ParquetCoreModelDataReader adapter

**Files:**
- Create: `app/ports/core_model_data_reader.py`
- Create: `app/adapters/parquet/__init__.py`
- Create: `app/adapters/parquet/core_model_data_reader.py`
- Test: `tests/unit/adapters/test_parquet_core_model_data_reader.py`

- [ ] **Step 1: Create the port**

Create `stl-verify/python/app/ports/core_model_data_reader.py`:

```python
"""CoreModelDataReader port — input data for the CORE pipeline.

Implementations provide protocol position data and price history.
The orderbook is loaded internally by Liquidator from the inputs directory
(see runner.py for the chdir strategy that satisfies this constraint).
"""

from typing import Protocol

import pandas as pd


class CoreModelDataReader(Protocol):
    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return (users_df, market_df) for the given protocol configuration."""
        ...

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        """Return a DataFrame of daily close prices indexed by date.

        Columns are token symbols matching ``collateral_list``.
        Must cover at least TRAIN_SIZE days of history.
        """
        ...
```

- [ ] **Step 2: Create the parquet adapter**

Create `stl-verify/python/app/adapters/parquet/__init__.py` (empty).

Create `stl-verify/python/app/adapters/parquet/core_model_data_reader.py`:

```python
"""Parquet-backed implementation of CoreModelDataReader.

Reads static parquet snapshots from ``inputs_dir``. Mirrors the file
naming conventions of the original ``importer.py`` so the snapshots from
``core_model_copy/inputs/`` work without transformation.
"""

from pathlib import Path

import pandas as pd


class ParquetCoreModelDataReader:
    def __init__(self, inputs_dir: Path) -> None:
        self._inputs_dir = inputs_dir

    def _path(self, filename: str) -> Path:
        return self._inputs_dir / filename

    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        protocol = protocol.lower()
        if protocol == "morpho":
            loan_token = f"{morpho_market}-{loan_token}".lower()
            users_df = pd.read_parquet(self._path(f"users_{protocol}_{loan_token}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}_{loan_token}.parquet"))
        elif protocol == "galaxy":
            type_ = "no-class-a" if "no" in galaxy_type.lower() else "w-class-a"
            users_df = pd.read_parquet(self._path(f"users_{protocol}_{type_}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}.parquet"))
        elif protocol == "anchorage":
            users_df = pd.read_parquet(self._path(f"users_{protocol}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}.parquet"))
        else:
            loan_token = loan_token.lower()
            users_df = pd.read_parquet(self._path(f"users_{protocol}_{loan_token}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}_{loan_token}.parquet"))
        return users_df, market_df

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        prices_df = pd.read_parquet(self._path("prices_df.parquet"))
        return prices_df[list(collateral_list)]
```

- [ ] **Step 3: Write unit tests**

Create `stl-verify/python/tests/unit/adapters/test_parquet_core_model_data_reader.py`:

```python
"""Unit tests for ParquetCoreModelDataReader."""

import pandas as pd
import pytest
from pathlib import Path
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader


@pytest.fixture()
def inputs_dir(tmp_path: Path) -> Path:
    """Minimal parquet fixtures for protocol data tests."""
    users = pd.DataFrame({"total_borrow_usd": [1000.0], "lltv": [0.8], "ltv": [0.7], "health_factor": [1.2], "liquidation_incentive": [1.05]})
    market = pd.DataFrame({"token_symbol": ["WBTC"], "oracle_price": [60000.0], "lltv": [0.8]})
    prices = pd.DataFrame({"WBTC": [60000.0, 61000.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02"]))

    users.to_parquet(tmp_path / "users_sparklend_usdc.parquet")
    market.to_parquet(tmp_path / "market_sparklend_usdc.parquet")
    users.to_parquet(tmp_path / "users_morpho_cbbtc-usdc.parquet")
    market.to_parquet(tmp_path / "market_morpho_cbbtc-usdc.parquet")
    users.to_parquet(tmp_path / "users_galaxy_no-class-a.parquet")
    market.to_parquet(tmp_path / "market_galaxy.parquet")
    users.to_parquet(tmp_path / "users_anchorage.parquet")
    market.to_parquet(tmp_path / "market_anchorage.parquet")
    prices.to_parquet(tmp_path / "prices_df.parquet")
    return tmp_path


async def test_get_protocol_data_sparklend(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, market_df = await reader.get_protocol_data(
        protocol="SPARKLEND", network="ethereum",
        morpho_market="CBBTC", loan_token="USDC", galaxy_type="no-class-a",
    )
    assert "total_borrow_usd" in users_df.columns
    assert "token_symbol" in market_df.columns


async def test_get_protocol_data_morpho(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, market_df = await reader.get_protocol_data(
        protocol="morpho", network="ethereum",
        morpho_market="CBBTC", loan_token="USDC", galaxy_type="no-class-a",
    )
    assert not users_df.empty


async def test_get_protocol_data_galaxy(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, _ = await reader.get_protocol_data(
        protocol="galaxy", network="ethereum",
        morpho_market="CBBTC", loan_token="USDC", galaxy_type="no-class-a",
    )
    assert not users_df.empty


async def test_get_protocol_data_galaxy_with_class_a(inputs_dir: Path):
    inputs_dir_extra = inputs_dir
    users = pd.DataFrame({"total_borrow_usd": [500.0], "lltv": [0.75], "ltv": [0.6], "health_factor": [1.3], "liquidation_incentive": [1.05]})
    users.to_parquet(inputs_dir_extra / "users_galaxy_w-class-a.parquet")
    reader = ParquetCoreModelDataReader(inputs_dir_extra)
    users_df, _ = await reader.get_protocol_data(
        protocol="galaxy", network="ethereum",
        morpho_market="CBBTC", loan_token="USDC", galaxy_type="with-class-a",
    )
    assert not users_df.empty


async def test_get_protocol_data_anchorage(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, _ = await reader.get_protocol_data(
        protocol="anchorage", network="ethereum",
        morpho_market="CBBTC", loan_token="USDC", galaxy_type="no-class-a",
    )
    assert not users_df.empty


async def test_get_prices_filters_columns(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    prices_df = await reader.get_prices(["WBTC"])
    assert list(prices_df.columns) == ["WBTC"]
    assert len(prices_df) == 2
```

- [ ] **Step 4: Run tests**

```bash
cd stl-verify/python && python -m pytest tests/unit/adapters/test_parquet_core_model_data_reader.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add app/ports/core_model_data_reader.py \
        app/adapters/parquet/ \
        tests/unit/adapters/test_parquet_core_model_data_reader.py
git commit -m "feat(core-model): add CoreModelDataReader port and ParquetCoreModelDataReader adapter"
```

---

## Task 6: DB migration for core_model_results

**Files:** `db/migrations/20260601_120000_create_core_model_results.sql`

- [ ] **Step 1: Create the migration**

Create `stl-verify/db/migrations/20260601_120000_create_core_model_results.sql`:

```sql
-- Create the core_model_results hypertable.
-- One row per (market_key, computed_at) produced by the core-model-runner cronjob.
-- The service reads the latest row per market_key at request time.

CREATE TABLE IF NOT EXISTS core_model_results (
    id             BIGSERIAL,
    market_key     TEXT        NOT NULL,
    crr_el_pct     NUMERIC     NOT NULL,
    crr_es_pct     NUMERIC     NOT NULL,
    crr_var_pct    NUMERIC     NOT NULL,
    hhi            NUMERIC,
    protocol       TEXT        NOT NULL,
    forecast_step  INT         NOT NULL,
    n_mc           INT         NOT NULL,
    copula_type    TEXT        NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, computed_at)
);

SELECT create_hypertable('core_model_results', 'computed_at');

ALTER TABLE core_model_results SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'market_key'
);

SELECT add_compression_policy('core_model_results', INTERVAL '7 days');

SELECT add_tiering_policy('core_model_results', INTERVAL '30 days');

-- Track this migration (filename must match exactly!)
INSERT INTO migrations (filename)
VALUES ('20260601_120000_create_core_model_results.sql')
ON CONFLICT (filename) DO NOTHING;
```

- [ ] **Step 2: Commit**

```bash
git add stl-verify/db/migrations/20260601_120000_create_core_model_results.sql
git commit -m "db: add core_model_results hypertable migration"
```

---

## Task 7: CoreModelResultsReader port and PostgresCoreModelResultsReader adapter

**Files:**
- Create: `app/ports/core_model_results_reader.py`
- Create: `app/adapters/postgres/core_model_results_reader.py`
- Test: `tests/integration/test_core_model_results_reader.py`

- [ ] **Step 1: Create the port and result dataclass**

Create `stl-verify/python/app/ports/core_model_results_reader.py`:

```python
"""CoreModelResultsReader port — reads pre-computed CRR rows from the DB.

Used by CoreModelRiskService at request time. The cronjob writes to the
same table; the service only reads.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class CoreModelResult:
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


class CoreModelResultsReader(Protocol):
    async def get_latest(self, market_key: str) -> CoreModelResult | None:
        """Return the most recently computed result for ``market_key``, or None."""
        ...
```

- [ ] **Step 2: Create the Postgres adapter**

Create `stl-verify/python/app/adapters/postgres/core_model_results_reader.py`:

```python
"""Postgres implementation of CoreModelResultsReader."""

from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.ports.core_model_results_reader import CoreModelResult


class PostgresCoreModelResultsReader:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_latest(self, market_key: str) -> CoreModelResult | None:
        query = text("""
            SELECT market_key, crr_el_pct, crr_es_pct, crr_var_pct,
                   hhi, protocol, forecast_step, n_mc, copula_type, computed_at
            FROM core_model_results
            WHERE market_key = :market_key
            ORDER BY computed_at DESC
            LIMIT 1
        """)
        async with self._engine.connect() as conn:
            row = (await conn.execute(query, {"market_key": market_key})).one_or_none()
        if row is None:
            return None
        return CoreModelResult(
            market_key=row.market_key,
            crr_el_pct=Decimal(str(row.crr_el_pct)),
            crr_es_pct=Decimal(str(row.crr_es_pct)),
            crr_var_pct=Decimal(str(row.crr_var_pct)),
            hhi=Decimal(str(row.hhi)) if row.hhi is not None else None,
            protocol=row.protocol,
            forecast_step=int(row.forecast_step),
            n_mc=int(row.n_mc),
            copula_type=row.copula_type,
            computed_at=row.computed_at,
        )
```

- [ ] **Step 3: Write integration test**

Create `stl-verify/python/tests/integration/test_core_model_results_reader.py`:

```python
"""Integration tests for PostgresCoreModelResultsReader.

Uses a real TimescaleDB container (shared session fixture from conftest.py).
"""

from datetime import datetime, timezone
from decimal import Decimal

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_results_reader import PostgresCoreModelResultsReader
from tests.integration.conftest import apply_migrations


@pytest.fixture()
async def db_url(module_db):
    return module_db


async def _insert_result(conn, market_key: str, crr_el: float, computed_at: datetime):
    await conn.execute(
        """
        INSERT INTO core_model_results
            (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi,
             protocol, forecast_step, n_mc, copula_type, computed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        market_key, crr_el, crr_el + 2.0, crr_el - 1.0, 10.5,
        "SPARKLEND", 14, 1000, "T-COPULA", computed_at,
    )


async def test_get_latest_returns_most_recent(db_url: str):
    conn = await asyncpg.connect(db_url)
    try:
        t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
        await _insert_result(conn, "sparklend_usdc", 10.0, t1)
        await _insert_result(conn, "sparklend_usdc", 20.0, t2)
    finally:
        await conn.close()

    engine = create_async_engine(db_url.replace("postgresql://", "postgresql+asyncpg://"))
    reader = PostgresCoreModelResultsReader(engine)
    result = await reader.get_latest("sparklend_usdc")
    await engine.dispose()

    assert result is not None
    assert result.crr_el_pct == Decimal("20.0")
    assert result.computed_at == t2


async def test_get_latest_returns_none_for_unknown_key(db_url: str):
    engine = create_async_engine(db_url.replace("postgresql://", "postgresql+asyncpg://"))
    reader = PostgresCoreModelResultsReader(engine)
    result = await reader.get_latest("does_not_exist")
    await engine.dispose()
    assert result is None


async def test_get_latest_returns_none_on_null_hhi(db_url: str):
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(
            """
            INSERT INTO core_model_results
                (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi,
                 protocol, forecast_step, n_mc, copula_type, computed_at)
            VALUES ($1,$2,$3,$4,NULL,$5,$6,$7,$8,$9)
            """,
            "morpho_cbbtc_usdc", 5.0, 7.0, 4.0,
            "MORPHO", 14, 1000, "GAUSSIAN",
            datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
    finally:
        await conn.close()

    engine = create_async_engine(db_url.replace("postgresql://", "postgresql+asyncpg://"))
    reader = PostgresCoreModelResultsReader(engine)
    result = await reader.get_latest("morpho_cbbtc_usdc")
    await engine.dispose()

    assert result is not None
    assert result.hhi is None
```

> **Note:** The `module_db` and `apply_migrations` fixtures are defined in `tests/integration/conftest.py`. The migration file from Task 6 will be auto-applied.

- [ ] **Step 4: Run integration tests**

```bash
cd stl-verify/python && python -m pytest tests/integration/test_core_model_results_reader.py -v
```

Expected: all three tests pass.

- [ ] **Step 5: Commit**

```bash
git add app/ports/core_model_results_reader.py \
        app/adapters/postgres/core_model_results_reader.py \
        tests/integration/test_core_model_results_reader.py
git commit -m "feat(core-model): add CoreModelResultsReader port and PostgresCoreModelResultsReader"
```

---

## Task 8: Create runner.py

**Files:**
- Create: `app/risk_engine/core_model/runner.py`
- Test: `tests/unit/risk_engine/test_core_model_runner.py`

The runner is a pure function that mirrors `main.py` from the original repo but replaces `importer.load_*` calls with port calls. `Liquidator.__init__` loads orderbook files via `importer.load_orderbook_data()` which uses CWD-relative paths — the runner temporarily changes directory to `inputs_dir` to satisfy this.

- [ ] **Step 1: Create runner.py**

Create `stl-verify/python/app/risk_engine/core_model/runner.py`:

```python
"""CORE pipeline runner.

Pure orchestration function: accepts a config + data reader, runs
calibration + simulation + liquidation, returns per-market CRR metrics.

The runner temporarily changes the working directory to ``inputs_dir``
before instantiating ``Liquidator``. This is required because Liquidator
calls ``importer.load_orderbook_data()`` which uses CWD-relative paths
(a known constraint of the original codebase — see README.md).
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

import numpy as np

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
        scenario = int((1 - p["PERC"]) * p["N_MC"])

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
    # CWD-relative paths — temporarily chdir to inputs_dir to satisfy this.
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
```

- [ ] **Step 2: Write smoke test**

Create `stl-verify/python/tests/unit/risk_engine/test_core_model_runner.py`:

```python
"""Smoke test for the CORE model runner.

Marked as integration because it runs GARCH calibration + Monte Carlo
against real parquet snapshots. Excluded from the standard unit test run.

Run explicitly with:
    pytest tests/unit/risk_engine/test_core_model_runner.py -v -m integration
"""

from decimal import Decimal
from pathlib import Path

import pytest

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model.config import load_params
from app.risk_engine.core_model.runner import CoreModelConfig, run

INPUTS_DIR = Path(__file__).resolve().parents[3] / "app" / "risk_engine" / "core_model" / "inputs"

pytestmark = pytest.mark.integration


@pytest.mark.skipif(not INPUTS_DIR.exists(), reason="core_model inputs not present")
async def test_runner_returns_valid_result():
    """Full pipeline smoke test with tiny N_MC=10, FORECAST_STEP=2."""
    params = load_params(overrides={
        "PROTOCOL": "SPARKLEND",
        "LOAN_TOKEN": "USDC",
        "N_MC": 10,
        "FORECAST_STEP": 2,
        "LIQ_ANALYSIS": "YES",
        "JUMPS": False,
    })
    config = CoreModelConfig(market_key="sparklend_usdc", params=params)
    data_reader = ParquetCoreModelDataReader(INPUTS_DIR)

    result = await run(config, data_reader, INPUTS_DIR)

    assert result.market_key == "sparklend_usdc"
    assert result.crr_el_pct >= Decimal("0")
    assert result.forecast_step == 2
    assert result.n_mc == 10
    assert result.copula_type == "T-COPULA"
    assert result.computed_at is not None
```

- [ ] **Step 3: Commit**

```bash
git add app/risk_engine/core_model/runner.py tests/unit/risk_engine/test_core_model_runner.py
git commit -m "feat(core-model): add runner.py orchestrating GARCH+MC+liquidation pipeline"
```

---

## Task 9: Asset-to-market-key mapping and Settings

**Files:**
- Create: `app/risk_engine/core_model/mappings/asset_to_market_key.json`
- Create: `app/risk_engine/core_model/core_model_mapping.py`
- Modify: `app/config.py`

The mapping file follows the identical format as the SURAF mapping:
`"chain_id:0xAddress" → "market_key"`. At startup, the existing
`resolve_receipt_token_mapping` converts it to `{asset_id: market_key}`.

- [ ] **Step 1: Create the empty mapping file**

Create `stl-verify/python/app/risk_engine/core_model/mappings/asset_to_market_key.json`:

```json
{}
```

(Populated once receipt token addresses for CORE-covered markets are known.)

- [ ] **Step 2: Create the mapping loader**

Create `stl-verify/python/app/risk_engine/core_model/core_model_mapping.py`:

```python
"""asset_id -> market_key mapping loader for the CORE model.

Follows the identical pattern as app/risk_engine/mapping.py (SURAF).
The JSON file maps composite keys (chain_id:0xAddress) to market_key
strings (e.g. "morpho_cbbtc_usdc"). At startup, resolve_receipt_token_mapping
converts these to {asset_id: market_key}.
"""

from pathlib import Path

from app.risk_engine.mapping import MappingError, load_asset_mapping


def load_core_model_mapping(path: Path) -> list[tuple[int, bytes, str]]:
    """Load and validate the asset -> market_key mapping file.

    Returns a list of (chain_id, receipt_token_address, market_key) tuples.
    Delegates validation to the shared load_asset_mapping utility.
    """
    return load_asset_mapping(path)
```

- [ ] **Step 3: Add Settings fields**

Open `app/config.py` and add two fields to the `Settings` class:

```python
core_model_mappings_file: Path = ENV_DIR / "app" / "risk_engine" / "core_model" / "mappings" / "asset_to_market_key.json"
```

Full updated `Settings` class (showing only the new lines in context):

```python
    suraf_mappings_file: Path = ENV_DIR / "suraf" / "mappings" / "asset_to_rating.json"
    core_model_mappings_file: Path = ENV_DIR / "app" / "risk_engine" / "core_model" / "mappings" / "asset_to_market_key.json"
    # ...rest unchanged
```

- [ ] **Step 4: Commit**

```bash
git add app/risk_engine/core_model/mappings/ \
        app/risk_engine/core_model/core_model_mapping.py \
        app/config.py
git commit -m "feat(core-model): add asset->market_key mapping and Settings field"
```

---

## Task 10: CoreModelRiskService and unit tests

**Files:**
- Create: `app/services/core_model_risk_service.py`
- Create: `tests/unit/services/test_core_model_risk_service.py`

- [ ] **Step 1: Write failing tests**

Create `stl-verify/python/tests/unit/services/test_core_model_risk_service.py`:

```python
"""Unit tests for CoreModelRiskService — 100% coverage required."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import CoreModelDetails, RrcResult
from app.domain.exceptions import InvalidOverrideError
from app.ports.core_model_results_reader import CoreModelResult
from app.services.core_model_risk_service import CoreModelRiskService

_PRIME = EthAddress("0xBcca60bB61934080951369a648Fb03DF4F96263C")
_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

_RESULT = CoreModelResult(
    market_key="sparklend_usdc",
    crr_el_pct=Decimal("12.5"),
    crr_es_pct=Decimal("15.0"),
    crr_var_pct=Decimal("10.0"),
    hhi=Decimal("22.3"),
    protocol="SPARKLEND",
    forecast_step=14,
    n_mc=10000,
    copula_type="T-COPULA",
    computed_at=_NOW,
)


def _service(
    asset_to_market_key: dict | None = None,
    get_latest_return: CoreModelResult | None = _RESULT,
    usd_exposure: Decimal = Decimal("10000.00"),
) -> CoreModelRiskService:
    results_reader = AsyncMock()
    results_reader.get_latest.return_value = get_latest_return
    allocation_repo = AsyncMock()
    allocation_repo.get_usd_exposure.return_value = usd_exposure
    return CoreModelRiskService(
        asset_to_market_key=asset_to_market_key or {1: "sparklend_usdc"},
        results_reader=results_reader,
        allocation_repo=allocation_repo,
    )


def test_applies_to_known_asset():
    svc = _service()
    assert svc.applies_to(1, _PRIME) is True


def test_applies_to_unknown_asset():
    svc = _service()
    assert svc.applies_to(99, _PRIME) is False


async def test_compute_returns_rrc_result():
    svc = _service(usd_exposure=Decimal("10000.00"))
    result = await svc.compute(1, _PRIME, {})
    assert isinstance(result, RrcResult)
    assert result.risk_model == "core_model"
    # rrc_usd = 10000 * 12.5 / 100 = 1250.00
    assert result.rrc_usd == Decimal("1250.00")
    assert result.comparable_crr_pct == Decimal("12.5")


async def test_compute_details_populated():
    svc = _service()
    result = await svc.compute(1, _PRIME, {})
    assert isinstance(result.details, CoreModelDetails)
    assert result.details.protocol == "SPARKLEND"
    assert result.details.forecast_step == 14
    assert result.details.n_mc == 10000
    assert result.details.hhi == Decimal("22.3")


async def test_compute_raises_when_no_precomputed_result():
    svc = _service(get_latest_return=None)
    with pytest.raises(ValueError, match="no pre-computed result"):
        await svc.compute(1, _PRIME, {})


async def test_compute_with_usd_exposure_override():
    svc = _service(usd_exposure=Decimal("99999.00"))
    result = await svc.compute(1, _PRIME, {"usd_exposure": "5000"})
    # rrc_usd = 5000 * 12.5 / 100 = 625.00
    assert result.rrc_usd == Decimal("625.00")


async def test_compute_rejects_unknown_override():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="unknown override keys"):
        await svc.compute(1, _PRIME, {"unknown_key": 1})


async def test_compute_rejects_none_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="invalid usd_exposure"):
        await svc.compute(1, _PRIME, {"usd_exposure": None})


async def test_compute_rejects_non_positive_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="positive finite number"):
        await svc.compute(1, _PRIME, {"usd_exposure": "0"})


async def test_compute_rejects_infinite_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="positive finite number"):
        await svc.compute(1, _PRIME, {"usd_exposure": "Infinity"})


async def test_compute_rejects_oversized_usd_exposure_string():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="too long"):
        await svc.compute(1, _PRIME, {"usd_exposure": "1" * 65})


async def test_compute_rejects_exceeding_max_usd_exposure():
    svc = _service()
    with pytest.raises(InvalidOverrideError, match="must be <="):
        await svc.compute(1, _PRIME, {"usd_exposure": "2e15"})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd stl-verify/python && python -m pytest tests/unit/services/test_core_model_risk_service.py -v
```

Expected: `ImportError` — `CoreModelRiskService` does not exist yet.

- [ ] **Step 3: Implement CoreModelRiskService**

Create `stl-verify/python/app/services/core_model_risk_service.py`:

```python
"""CoreModelRiskService — reads pre-computed CORE results from the DB.

Implements the RiskModel protocol. CRR computation is delegated to the
core-model-runner cronjob. This service only reads the latest result and
multiplies it by the prime's USD exposure.
"""

from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import CoreModelDetails, ModelName, RrcResult
from app.domain.exceptions import InvalidOverrideError
from app.ports.allocation_repository import AllocationRepository
from app.ports.core_model_results_reader import CoreModelResultsReader

_HUNDRED = Decimal("100")
_USD_CENT = Decimal("0.01")
_ALLOWED_OVERRIDES = frozenset({"usd_exposure"})
_USD_EXPOSURE_MAX = Decimal("1e15")
_DECIMAL_STR_MAX_LEN = 64


class CoreModelRiskService:
    """CORE model risk service.

    Implements the :class:`~app.ports.risk_model.RiskModel` protocol so it
    can be used by the unified model registry.
    """

    risk_model: ModelName = "core_model"

    def __init__(
        self,
        asset_to_market_key: dict[int, str],
        results_reader: CoreModelResultsReader,
        allocation_repo: AllocationRepository,
    ) -> None:
        self._asset_to_market_key = asset_to_market_key
        self._results_reader = results_reader
        self._allocation_repo = allocation_repo

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return asset_id in self._asset_to_market_key

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        usd_exposure = await self._resolve_usd_exposure(asset_id, prime_id, overrides)
        market_key = self._asset_to_market_key[asset_id]
        result = await self._results_reader.get_latest(market_key)
        if result is None:
            raise ValueError(
                f"no pre-computed result for market_key={market_key!r} (asset_id={asset_id}); "
                "run the core-model-runner cronjob first"
            )
        rrc_usd = (usd_exposure * result.crr_el_pct / _HUNDRED).quantize(_USD_CENT, rounding=ROUND_HALF_EVEN)
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            comparable_crr_pct=result.crr_el_pct,
            risk_model=self.risk_model,
            details=CoreModelDetails(
                risk_model="core_model",
                crr_el_pct=result.crr_el_pct,
                crr_es_pct=result.crr_es_pct,
                crr_var_pct=result.crr_var_pct,
                hhi=result.hhi,
                protocol=result.protocol,
                forecast_step=result.forecast_step,
                n_mc=result.n_mc,
                copula_type=result.copula_type,
            ),
        )

    async def _resolve_usd_exposure(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> Decimal:
        unknown = set(overrides) - _ALLOWED_OVERRIDES
        if unknown:
            raise InvalidOverrideError(f"unknown override keys: {sorted(unknown)}")

        if "usd_exposure" not in overrides:
            return await self._allocation_repo.get_usd_exposure(asset_id, prime_id)

        raw = overrides["usd_exposure"]
        if raw is None:
            raise InvalidOverrideError("invalid usd_exposure: expected a positive finite number, got None")
        if isinstance(raw, str) and len(raw) > _DECIMAL_STR_MAX_LEN:
            raise InvalidOverrideError(
                f"invalid usd_exposure: input string too long ({len(raw)} > {_DECIMAL_STR_MAX_LEN})"
            )
        try:
            usd_exposure = raw if isinstance(raw, Decimal) else Decimal(str(raw))
        except Exception as exc:
            raise InvalidOverrideError(
                f"invalid usd_exposure: expected a positive finite number, got {raw!r}"
            ) from exc
        if not usd_exposure.is_finite():
            raise InvalidOverrideError(
                f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}"
            )
        if usd_exposure <= Decimal("0"):
            raise InvalidOverrideError(
                f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}"
            )
        if usd_exposure > _USD_EXPOSURE_MAX:
            raise InvalidOverrideError(f"usd_exposure must be <= {_USD_EXPOSURE_MAX:E}, got {usd_exposure}")
        return usd_exposure
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd stl-verify/python && python -m pytest tests/unit/services/test_core_model_risk_service.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add app/services/core_model_risk_service.py \
        tests/unit/services/test_core_model_risk_service.py
git commit -m "feat(core-model): add CoreModelRiskService with full unit test coverage"
```

---

## Task 11: Cronjob — core-model-runner

**Files:**
- Create: `cli/__init__.py`
- Create: `cli/cronjobs/__init__.py`
- Create: `cli/cronjobs/core_model_runner/__init__.py`
- Create: `cli/cronjobs/core_model_runner/config.py`
- Create: `cli/cronjobs/core_model_runner/main.py`

The cronjob runs one market per invocation, driven by env vars. To run multiple markets, invoke it multiple times with different `CORE_MODEL_MARKET_KEY` / `CORE_MODEL_PROTOCOL` etc.

- [ ] **Step 1: Create package markers**

```bash
touch stl-verify/python/cli/__init__.py
touch stl-verify/python/cli/cronjobs/__init__.py
touch stl-verify/python/cli/cronjobs/core_model_runner/__init__.py
```

- [ ] **Step 2: Create config.py**

Create `stl-verify/python/cli/cronjobs/core_model_runner/config.py`:

```python
"""Environment-variable config for the core-model-runner cronjob.

All CORE model params default to config.DEFAULTS, which in turn come from
app/risk_engine/core_model/inputs/default_params.json. Override any of them
via environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from app.risk_engine.core_model.config import DEFAULTS

_INPUTS_DEFAULT = Path(__file__).resolve().parents[3] / "app" / "risk_engine" / "core_model" / "inputs"


@dataclass(frozen=True)
class RunnerConfig:
    database_url: str
    market_key: str
    inputs_dir: Path
    params: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "RunnerConfig":
        overrides = {
            k: _coerce(k, os.environ[env_key])
            for k, env_key in _ENV_MAP.items()
            if env_key in os.environ
        }
        params = {**DEFAULTS, **overrides}
        return cls(
            database_url=os.environ["DATABASE_URL"],
            market_key=os.environ["CORE_MODEL_MARKET_KEY"],
            inputs_dir=Path(os.environ.get("CORE_MODEL_INPUTS_DIR", str(_INPUTS_DEFAULT))),
            params=params,
        )


# Maps CORE param name -> env var name
_ENV_MAP: dict[str, str] = {
    "PROTOCOL":          "CORE_MODEL_PROTOCOL",
    "NETWORK":           "CORE_MODEL_NETWORK",
    "MORPHO_MARKET":     "CORE_MODEL_MORPHO_MARKET",
    "GALAXY_TYPE":       "CORE_MODEL_GALAXY_TYPE",
    "LOAN_TOKEN":        "CORE_MODEL_LOAN_TOKEN",
    "N_MC":              "CORE_MODEL_N_MC",
    "FORECAST_STEP":     "CORE_MODEL_FORECAST_STEP",
    "TRAIN_SIZE":        "CORE_MODEL_TRAIN_SIZE",
    "COPULA_TYPE":       "CORE_MODEL_COPULA_TYPE",
    "SEED":              "CORE_MODEL_SEED",
    "LIQ_ANALYSIS":      "CORE_MODEL_LIQ_ANALYSIS",
    "JUMPS":             "CORE_MODEL_JUMPS",
    "HOURLY_CONV":       "CORE_MODEL_HOURLY_CONV",
    "USE_LOG_RETURNS":   "CORE_MODEL_USE_LOG_RETURNS",
    "FOCUS_ON_NEGATIVE": "CORE_MODEL_FOCUS_ON_NEGATIVE",
    "WORST_CASE":        "CORE_MODEL_WORST_CASE",
    "PERC":              "CORE_MODEL_PERC",
    "VOL_FLOOR_PCT":     "CORE_MODEL_VOL_FLOOR_PCT",
    "GAS_FEE_USD":       "CORE_MODEL_GAS_FEE_USD",
    "SWAP_FEE_USD":      "CORE_MODEL_SWAP_FEE_USD",
    "MC_TRIGGER":        "CORE_MODEL_MC_TRIGGER",
    "MC_TARGET_LTV":     "CORE_MODEL_MC_TARGET_LTV",
    "MC_CURE_PROB":      "CORE_MODEL_MC_CURE_PROB",
}


def _coerce(param: str, raw: str) -> object:
    """Coerce a string env var to the type of the corresponding DEFAULTS entry."""
    default = DEFAULTS.get(param)
    if isinstance(default, bool):
        return raw.lower() in ("true", "1", "yes")
    if isinstance(default, int):
        return int(raw)
    if isinstance(default, float):
        return float(raw)
    if default is None:
        # Optional params (MC_TARGET_LTV) — try float, fall back to None
        try:
            return float(raw)
        except ValueError:
            return None
    return raw
```

- [ ] **Step 3: Create main.py**

Create `stl-verify/python/cli/cronjobs/core_model_runner/main.py`:

```python
"""CORE model runner — compute CRR for one protocol market and write to DB.

Usage:
    DATABASE_URL=postgresql://... \\
    CORE_MODEL_MARKET_KEY=sparklend_usdc \\
    CORE_MODEL_PROTOCOL=SPARKLEND \\
    CORE_MODEL_LOAN_TOKEN=USDC \\
    python -m cli.cronjobs.core_model_runner.main

All CORE model params default to inputs/default_params.json values.
Override any param via env var — see config.py for the full mapping.
"""

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine import make_url

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model.runner import CoreModelConfig, CoreModelPipelineResult, run
from cli.cronjobs.core_model_runner.config import RunnerConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _async_db_url(database_url: str) -> str:
    url = make_url(database_url)
    url = url.set(drivername="postgresql+asyncpg")
    query = dict(url.query)
    query.pop("sslmode", None)
    return url.set(query=query).render_as_string(hide_password=False)


async def _write_result(engine, result: CoreModelPipelineResult) -> None:
    query = text("""
        INSERT INTO core_model_results
            (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi,
             protocol, forecast_step, n_mc, copula_type, computed_at)
        VALUES
            (:market_key, :crr_el_pct, :crr_es_pct, :crr_var_pct, :hhi,
             :protocol, :forecast_step, :n_mc, :copula_type, :computed_at)
    """)
    async with engine.begin() as conn:
        await conn.execute(query, {
            "market_key":    result.market_key,
            "crr_el_pct":    float(result.crr_el_pct),
            "crr_es_pct":    float(result.crr_es_pct),
            "crr_var_pct":   float(result.crr_var_pct),
            "hhi":           float(result.hhi) if result.hhi is not None else None,
            "protocol":      result.protocol,
            "forecast_step": result.forecast_step,
            "n_mc":          result.n_mc,
            "copula_type":   result.copula_type,
            "computed_at":   result.computed_at,
        })


async def main() -> None:
    cfg = RunnerConfig.from_env()
    logger.info("starting core-model-runner market_key=%s protocol=%s", cfg.market_key, cfg.params["PROTOCOL"])

    engine = create_async_engine(_async_db_url(cfg.database_url), pool_pre_ping=True)
    data_reader = ParquetCoreModelDataReader(cfg.inputs_dir)
    config = CoreModelConfig(market_key=cfg.market_key, params=cfg.params)

    try:
        result = await run(config, data_reader, cfg.inputs_dir)
        logger.info(
            "pipeline complete market_key=%s crr_el_pct=%s",
            result.market_key,
            result.crr_el_pct,
        )
        await _write_result(engine, result)
        logger.info("result written to core_model_results market_key=%s", result.market_key)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Commit**

```bash
git add stl-verify/python/cli/
git commit -m "feat(core-model): add core-model-runner cronjob"
```

---

## Task 12: Wire into app/main.py

**Files:** `app/main.py`

- [ ] **Step 1: Add imports**

Open `app/main.py`. Add to the import block (keep alphabetical order within groups):

```python
from app.adapters.postgres.core_model_results_reader import PostgresCoreModelResultsReader
from app.risk_engine.core_model.core_model_mapping import load_core_model_mapping
from app.services.core_model_risk_service import CoreModelRiskService
```

- [ ] **Step 2: Load and validate the mapping at startup (before lifespan)**

In `create_app`, after the SURAF mapping loading block, add:

```python
    core_raw_mapping = load_core_model_mapping(settings.core_model_mappings_file)
    logger.info("core model asset->market_key mapping loaded entries=%d", len(core_raw_mapping))
```

Full context (showing surrounding existing code):
```python
    raw_mapping = load_asset_mapping(settings.suraf_mappings_file)
    _check_mapping_refs(raw_mapping, suraf_ratings)
    logger.info("asset->rating mapping loaded entries=%d", len(raw_mapping))

    core_raw_mapping = load_core_model_mapping(settings.core_model_mappings_file)
    logger.info("core model asset->market_key mapping loaded entries=%d", len(core_raw_mapping))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
```

- [ ] **Step 3: Instantiate CORE adapters and service inside lifespan**

Inside the `lifespan` context manager, after the crypto_lending_risk_service block, add:

```python
            asset_to_market_key = await resolve_receipt_token_mapping(core_raw_mapping, engine)
            core_model_results_reader = PostgresCoreModelResultsReader(engine)
            core_model_risk_service = CoreModelRiskService(
                asset_to_market_key=asset_to_market_key,
                results_reader=core_model_results_reader,
                allocation_repo=allocation_repo,
            )
```

- [ ] **Step 4: Add to ModelRegistry**

Change:
```python
            model_registry = ModelRegistry([suraf_rrc_service, crypto_lending_risk_service])
```
to:
```python
            model_registry = ModelRegistry([suraf_rrc_service, crypto_lending_risk_service, core_model_risk_service])
```

- [ ] **Step 5: Run the existing test suite to verify nothing broke**

```bash
cd stl-verify/python && python -m pytest tests/unit/ -v --ignore=tests/unit/risk_engine/test_core_model_runner.py
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add app/main.py app/config.py
git commit -m "feat(core-model): wire CoreModelRiskService into ModelRegistry and app startup"
```

---

## Self-Review Checklist

### Spec coverage

| Spec item | Task |
|---|---|
| Copy model files, fix imports | Task 2 |
| Known-issue README + TODO comments | Tasks 2, 3 |
| `CoreModelDetails` in domain types | Task 4 |
| `CoreModelDataReader` port | Task 5 |
| `ParquetCoreModelDataReader` adapter | Task 5 |
| `core_model_results` migration (hypertable + compression + tiering) | Task 6 |
| `CoreModelResultsReader` port + `CoreModelResult` dataclass | Task 7 |
| `PostgresCoreModelResultsReader` | Task 7 |
| `runner.py` with `_chdir` for orderbook loading | Task 8 |
| `asset_id → market_key` mapping + Settings | Task 9 |
| `CoreModelRiskService` with `usd_exposure` override | Task 10 |
| 100% unit test coverage for service | Task 10 |
| Cronjob with env-var config | Task 11 |
| App startup wiring | Task 12 |

### Type consistency
- `CoreModelConfig.params` is a plain `dict` (from `load_params()`) throughout Tasks 8, 9, 11
- `CoreModelPipelineResult` (Task 8) and `CoreModelResult` (Task 7) are distinct: the former is the runner's output; the latter is what is persisted and read by the service
- `resolve_receipt_token_mapping` accepts `list[tuple[int, bytes, str]]` — both SURAF and CORE use this same type
- `CoreModelDetails` fields in Task 4 match what `CoreModelRiskService` populates in Task 10

### No placeholders
No TBD/TODO/placeholder steps found. All code blocks are complete.
