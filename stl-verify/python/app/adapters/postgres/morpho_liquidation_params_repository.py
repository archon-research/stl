from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.risk import LiquidationParams
from app.risk_engine.crypto_lending.lif import compute_lif

# lltv values in morpho_market are assumed to be in [0,1] decimal range (e.g. 0.86 for 86%).
# When a collateral token appears in multiple markets of the same vault, MIN(lltv) is used
# as the conservative liquidation threshold.
_SQL = """
WITH vault_user AS (
    SELECT u.id AS user_id
    FROM morpho_vault mv
    JOIN "user" u ON u.address = mv.address AND u.chain_id = mv.chain_id
    WHERE mv.id = :backed_asset_id
),
vault_market_ids AS (
    SELECT DISTINCT mmp.morpho_market_id
    FROM morpho_market_position mmp
    WHERE mmp.user_id = (SELECT user_id FROM vault_user LIMIT 1)
)
SELECT
    mm.collateral_token_id AS token_id,
    MIN(mm.lltv)           AS lltv
FROM morpho_market mm
WHERE mm.id IN (SELECT morpho_market_id FROM vault_market_ids)
  AND mm.collateral_token_id = ANY(:token_ids)
GROUP BY mm.collateral_token_id
"""


class MorphoLiquidationParamsRepository:
    """Liquidation params adapter for Morpho Blue vaults.

    liquidation_threshold = lltv (same concept, same value).
    liquidation_bonus     = LIF computed deterministically from lltv.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_params(self, backed_asset_id: int, token_ids: list[int]) -> dict[int, LiquidationParams]:
        if not token_ids:
            return {}

        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(_SQL),
                {"backed_asset_id": backed_asset_id, "token_ids": token_ids},
            )
            rows = result.fetchall()

        params = {}
        for row in rows:
            lltv = Decimal(str(row.lltv))
            params[row.token_id] = LiquidationParams(
                token_id=row.token_id,
                liquidation_threshold=lltv,
                liquidation_bonus=compute_lif(lltv),
            )
        return params
