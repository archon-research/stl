"""Integration tests for ``PostgresCoreModelResultsWriter``.

Pins the three invariants the adapter exists to enforce:
- Decimals reach the NUMERIC columns exactly (no float() round-trip artifacts).
- A duplicate (market_key, computed_at) raises instead of being swallowed —
  the insert deliberately has no ON CONFLICT clause, so a collision is a
  duplicate-write bug, not something to ignore.
- The Monte Carlo diagnostics land inside the params JSONB under one key.
"""

from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_results_reader import PostgresCoreModelResultsReader
from app.adapters.postgres.core_model_results_writer import PostgresCoreModelResultsWriter
from app.risk_engine.core_model.convergence import MonteCarloDiagnostics
from app.risk_engine.core_model.runner import CoreModelPipelineResult

# The engine lives in a module-scoped fixture, so every test must share its
# event loop (same pattern as the reader tests next door).
pytestmark = pytest.mark.asyncio(loop_scope="module")

# A deliberately unconverged run: the writer stores whatever the service hands
# it, the convergence verdict is the service's to log.
_DIAGNOSTICS = MonteCarloDiagnostics(
    n_scenarios=100,
    n_loss_scenarios=7,
    n_catastrophic_scenarios=2,
    catastrophic_loss_pct=1.0,
    crr_el_se_pct=0.0421,
    crr_el_rel_se=0.38,
)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def adapters(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield PostgresCoreModelResultsWriter(engine), PostgresCoreModelResultsReader(engine), engine
    finally:
        await engine.dispose()


def _result(
    *,
    market_key: str = "sparklend_usdt",
    computed_at: datetime = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc),
    crr_el_pct: Decimal = Decimal("0.110351"),
    hhi: Decimal | None = Decimal("0.271828"),
) -> CoreModelPipelineResult:
    return CoreModelPipelineResult(
        market_key=market_key,
        crr_el_pct=crr_el_pct,
        crr_es_pct=Decimal("0.147293"),
        crr_var_pct=Decimal("0.031415"),
        hhi=hhi,
        protocol="SPARKLEND",
        forecast_step=14,
        n_mc=100,
        copula_type="T-COPULA",
        computed_at=computed_at,
        params={"PROTOCOL": "SPARKLEND", "N_MC": 100},
        mc_diagnostics=_DIAGNOSTICS,
    )


async def test_insert_stores_the_diagnostics_inside_the_params_jsonb(adapters):
    writer, _, engine = adapters
    await writer.insert(_result(market_key="writer_diagnostics"))

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT params FROM core_model_results WHERE market_key = :market_key"),
                {"market_key": "writer_diagnostics"},
            )
        ).one()

    assert row.params["mc_diagnostics"] == asdict(_DIAGNOSTICS)
    assert row.params["PROTOCOL"] == "SPARKLEND"


async def test_insert_round_trips_decimals_exactly(adapters):
    writer, reader, _ = adapters
    await writer.insert(_result(market_key="writer_decimals"))

    stored = await reader.get_latest("writer_decimals")

    assert stored is not None
    assert stored.crr_el_pct == Decimal("0.110351")
    assert stored.crr_es_pct == Decimal("0.147293")
    assert stored.crr_var_pct == Decimal("0.031415")
    assert stored.hhi == Decimal("0.271828")


async def test_insert_stores_a_null_hhi(adapters):
    writer, reader, _ = adapters
    await writer.insert(_result(market_key="writer_null_hhi", hhi=None))

    stored = await reader.get_latest("writer_null_hhi")

    assert stored is not None
    assert stored.hhi is None


async def test_duplicate_market_and_timestamp_raises(adapters):
    writer, _, _ = adapters
    await writer.insert(_result(market_key="writer_duplicate"))

    with pytest.raises(IntegrityError):
        await writer.insert(_result(market_key="writer_duplicate"))
