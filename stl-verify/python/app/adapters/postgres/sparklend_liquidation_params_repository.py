from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.risk_engine.entities import LiquidationParams

# liquidation_threshold and liquidation_bonus are stored as basis points in sparklend_reserve_data
# (e.g. 8250 = 82.5%, 10500 = 1.05× multiplier). Divide by 10000 to normalise.
_SQL = """
SELECT
    srd.token_id,
    srd.liquidation_threshold / 10000.0 AS liquidation_threshold,
    srd.liquidation_bonus      / 10000.0 AS liquidation_bonus
FROM (
    SELECT DISTINCT ON (token_id)
        token_id,
        liquidation_threshold,
        liquidation_bonus
    FROM sparklend_reserve_data
    WHERE protocol_id    = :protocol_id
      AND token_id       = ANY(:token_ids)
      AND usage_as_collateral_enabled = true
      AND liquidation_threshold IS NOT NULL
      AND liquidation_threshold > 0
    ORDER BY token_id, block_number DESC, block_version DESC
) srd
"""


class SparkLendLiquidationParamsRepository:
    """Liquidation params adapter for SparkLend and Aave-like protocols."""

    def __init__(self, engine: AsyncEngine, protocol_id: int) -> None:
        self._engine = engine
        self._protocol_id = protocol_id

    async def get_params(
        self, backed_asset_id: int, token_ids: list[int]
    ) -> dict[int, LiquidationParams]:
        if not token_ids:
            return {}

        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(_SQL),
                {"protocol_id": self._protocol_id, "token_ids": token_ids},
            )
            rows = result.fetchall()

        return {
            row.token_id: LiquidationParams(
                token_id=row.token_id,
                liquidation_threshold=Decimal(str(row.liquidation_threshold)),
                liquidation_bonus=Decimal(str(row.liquidation_bonus)),
            )
            for row in rows
        }
