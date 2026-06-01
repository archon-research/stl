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
