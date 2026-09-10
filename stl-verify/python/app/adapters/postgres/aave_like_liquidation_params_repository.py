from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.risk import LiquidationParams

# liquidation_threshold and liquidation_bonus are stored as basis points
# (e.g. 8250 = 82.5%, 10500 = 1.05× multiplier). Divide by 10000 to normalise.
# Reads the *_current cache (VEC-661): the collateral filter applies to the newest
# row per reserve, so a reserve since disabled drops out, as in the breakdown read.
_SQL = """
SELECT
    token_id,
    liquidation_threshold / 10000::numeric AS liquidation_threshold,
    liquidation_bonus     / 10000::numeric AS liquidation_bonus
FROM sparklend_reserve_data_current
WHERE protocol_id = :protocol_id
  AND usage_as_collateral_enabled
  AND liquidation_threshold > 0
"""


class AaveLikeLiquidationParamsRepository:
    """Liquidation params adapter for Aave-like protocols."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_params(self, protocol_id: int) -> dict[int, LiquidationParams]:
        """Return the liquidation params of every collateral-enabled reserve of a protocol.

        Protocol-wide rather than filtered to a caller's token ids: these are
        protocol-level config, so one result serves every allocation of that protocol
        in a request (``PostgresCryptoLendingReader`` slices it per caller).
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(text(_SQL), {"protocol_id": protocol_id})
            rows = result.fetchall()

        return {
            row.token_id: LiquidationParams(
                token_id=row.token_id,
                liquidation_threshold=Decimal(str(row.liquidation_threshold)),
                liquidation_bonus=Decimal(str(row.liquidation_bonus)),
            )
            for row in rows
        }
