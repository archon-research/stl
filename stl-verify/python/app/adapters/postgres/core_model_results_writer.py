"""Postgres implementation of CoreModelResultsWriter."""

import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.risk_engine.core_model.runner import CoreModelPipelineResult

# Deliberately no ON CONFLICT clause: the PK is (market_key, computed_at) and
# two runs of one market cannot legitimately finish at the same timestamp, so
# a collision is a duplicate-write bug and must raise, not be swallowed.
_INSERT_RESULT = text("""
    INSERT INTO core_model_results
        (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi,
         protocol, forecast_step, n_mc, copula_type, computed_at, params)
    VALUES
        (:market_key, :crr_el_pct, :crr_es_pct, :crr_var_pct, :hhi,
         :protocol, :forecast_step, :n_mc, :copula_type, :computed_at, :params)
""")


class PostgresCoreModelResultsWriter:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def insert(self, result: CoreModelPipelineResult) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                _INSERT_RESULT,
                {
                    "market_key": result.market_key,
                    # Decimals go to the NUMERIC columns as-is: a float()
                    # round-trip would re-introduce the binary artifacts the
                    # runner already stripped (0.110351 -> 0.11035100000000000464...).
                    "crr_el_pct": result.crr_el_pct,
                    "crr_es_pct": result.crr_es_pct,
                    "crr_var_pct": result.crr_var_pct,
                    "hhi": result.hhi,
                    "protocol": result.protocol,
                    "forecast_step": result.forecast_step,
                    "n_mc": result.n_mc,
                    "copula_type": result.copula_type,
                    "computed_at": result.computed_at,
                    "params": json.dumps(result.params),
                },
            )
