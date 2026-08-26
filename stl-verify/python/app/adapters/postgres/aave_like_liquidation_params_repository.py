from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.risk import LiquidationParams

# liquidation_threshold and liquidation_bonus are stored as basis points
# (e.g. 8250 = 82.5%, 10500 = 1.05× multiplier). Divide by 10000 to normalise.
#
# Read from the trigger-maintained current-state cache, not from the
# sparklend_reserve_data hypertable: a PK range scan over ~150 rows instead of a
# DISTINCT ON across ~158 chunks.
#
# One deliberate semantic difference. The history query applied
# usage_as_collateral_enabled BEFORE the DISTINCT ON, so a reserve the protocol has
# since disabled could still be served from an older, still-enabled row. The cache
# holds the newest row per reserve unconditionally and the filter is applied to
# that, so such a reserve drops out — which is what "does the protocol still accept
# this as collateral" means, and is how the backed-breakdown query already reads
# this table. The two agree on every protocol of a full-scale clone (119 rows,
# 0 diffs).
#
# `liquidation_threshold IS NOT NULL` is dropped with it: `> 0` excludes NULL anyway.
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
        in a request (``PostgresCryptoLendingReader`` slices it per caller). The
        largest protocol has 64 reserves, so returning all of them is free.
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
