"""Integration tests for ``PostgresCoreModelResultsReader``.

Each test case exercises a distinct behaviour of ``get_latest``:
- Returns the most recently inserted row when multiple rows exist for the
  same market_key.
- Returns ``None`` when no row exists for the requested market_key.
- Returns a result with ``hhi=None`` when the DB row has a NULL hhi column.
"""

from datetime import datetime, timezone
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_results_reader import PostgresCoreModelResultsReader

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def reader(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield PostgresCoreModelResultsReader(engine)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_row(
    db_url: str,
    *,
    market_key: str,
    computed_at: datetime,
    crr_el_pct: float = 0.01,
    crr_es_pct: float = 0.02,
    crr_var_pct: float = 0.03,
    hhi: float | None = 0.5,
    protocol: str = "aave",
    forecast_step: int = 1,
    n_mc: int = 1000,
    copula_type: str = "gaussian",
) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(
            """
            INSERT INTO core_model_results
                (market_key, crr_el_pct, crr_es_pct, crr_var_pct,
                 hhi, protocol, forecast_step, n_mc, copula_type, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            market_key,
            Decimal(str(crr_el_pct)),
            Decimal(str(crr_es_pct)),
            Decimal(str(crr_var_pct)),
            Decimal(str(hhi)) if hhi is not None else None,
            protocol,
            forecast_step,
            n_mc,
            copula_type,
            computed_at,
        )
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_returns_most_recent_row(reader, db_url: str) -> None:
    """When two rows exist for the same market_key, the newest one is returned."""
    market_key = "test_market_latest"
    older = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    newer = datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc)

    await _insert_row(db_url, market_key=market_key, computed_at=older, crr_el_pct=0.10)
    await _insert_row(db_url, market_key=market_key, computed_at=newer, crr_el_pct=0.99)

    result = await reader.get_latest(market_key)

    assert result is not None
    assert result.market_key == market_key
    assert result.crr_el_pct == Decimal("0.99")
    assert result.computed_at == newer


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_returns_none_when_no_rows_exist(reader) -> None:
    """A market_key with no rows yields None."""
    result = await reader.get_latest("nonexistent_market_key_xyz")
    assert result is None


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_returns_result_with_null_hhi(reader, db_url: str) -> None:
    """A row with NULL hhi is mapped to ``hhi=None`` on the dataclass."""
    market_key = "test_market_null_hhi"
    computed_at = datetime(2025, 3, 15, 12, 0, 0, tzinfo=timezone.utc)

    await _insert_row(
        db_url,
        market_key=market_key,
        computed_at=computed_at,
        hhi=None,
        protocol="morpho",
        copula_type="t_copula",
    )

    result = await reader.get_latest(market_key)

    assert result is not None
    assert result.hhi is None
    assert result.protocol == "morpho"
    assert result.copula_type == "t_copula"
