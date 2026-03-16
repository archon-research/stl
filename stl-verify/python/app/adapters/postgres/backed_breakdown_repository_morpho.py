from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)

# Minimum amount (in human-readable token units) below which a contribution is
# treated as dust and excluded from the breakdown. This threshold is safe for
# USD-pegged loan tokens (e.g. USDC, DAI) but should be revisited if vaults
# backed by tokens with very low unit values are added.
_MORPHO_DUST_FILTER = "0.01"

_MORPHO_BACKED_BREAKDOWN_SQL = f"""
WITH morpho_vaults AS (
    SELECT mv.id AS vault_id, mv.address, mv.chain_id, mv.asset_token_id
    FROM morpho_vault mv
    WHERE mv.id = :vault_id
      AND mv.protocol_id = :protocol_id
),
vault_users AS (
    SELECT mv.vault_id, u.id AS user_id
    FROM morpho_vaults mv
    JOIN "user" u ON u.address = mv.address AND u.chain_id = mv.chain_id
),
vault_states AS (
    SELECT DISTINCT ON (vs.morpho_vault_id)
        vs.morpho_vault_id AS vault_id,
        vs.total_assets / power(10, t.decimals) AS total_assets,
        t.symbol AS loan_token
    FROM morpho_vault_state vs
    JOIN morpho_vaults mv ON mv.vault_id = vs.morpho_vault_id
    JOIN token t ON t.id = mv.asset_token_id
    WHERE vs.morpho_vault_id IN (SELECT vault_id FROM morpho_vaults)
    ORDER BY vs.morpho_vault_id, vs.block_number DESC, vs.block_version DESC
),
vault_market_ids AS (
    SELECT DISTINCT vu.vault_id, mp.morpho_market_id
    FROM vault_users vu
    JOIN LATERAL (
        SELECT DISTINCT morpho_market_id
        FROM morpho_market_position
        WHERE user_id = vu.user_id
    ) mp ON true
),
market_allocs AS (
    SELECT vmi.vault_id,
           vmi.morpho_market_id,
           ct.symbol AS collateral,
           pos.supply_assets / power(10, lt.decimals) AS vault_supply
    FROM vault_market_ids vmi
    JOIN LATERAL (
        SELECT supply_assets, morpho_market_id
        FROM morpho_market_position
        WHERE user_id = (SELECT user_id FROM vault_users WHERE vault_id = vmi.vault_id LIMIT 1)
          AND morpho_market_id = vmi.morpho_market_id
        ORDER BY block_number DESC, block_version DESC
        LIMIT 1
    ) pos ON true
    JOIN morpho_market mm ON mm.id = vmi.morpho_market_id
    JOIN token ct ON ct.id = mm.collateral_token_id
    JOIN token lt ON lt.id = mm.loan_token_id
),
market_states AS (
    SELECT ms.*
    FROM (SELECT DISTINCT morpho_market_id FROM market_allocs) ma
    JOIN LATERAL (
        SELECT morpho_market_id,
               CASE WHEN total_supply_assets > 0
                   THEN total_borrow_assets::numeric / total_supply_assets::numeric
                   ELSE 0 END AS utilization
        FROM morpho_market_state
        WHERE morpho_market_id = ma.morpho_market_id
        ORDER BY block_number DESC, block_version DESC
        LIMIT 1
    ) ms ON true
),
breakdown AS (
    SELECT
        ma.vault_id,
        ma.collateral,
        ma.vault_supply * ms.utilization AS collateral_amount,
        ma.vault_supply * (1 - ms.utilization) AS idle_loan_amount,
        vs.loan_token
    FROM market_allocs ma
    JOIN market_states ms ON ms.morpho_market_id = ma.morpho_market_id
    JOIN vault_states vs ON vs.vault_id = ma.vault_id
),
vault_idle AS (
    SELECT vs.vault_id, vs.loan_token,
           vs.total_assets - coalesce(sum(b.collateral_amount + b.idle_loan_amount), 0) AS idle_amount
    FROM vault_states vs
    LEFT JOIN breakdown b ON b.vault_id = vs.vault_id
    GROUP BY vs.vault_id, vs.loan_token, vs.total_assets
),
all_backing AS (
    SELECT collateral AS symbol, collateral_amount AS amount FROM breakdown
    WHERE collateral_amount > {_MORPHO_DUST_FILTER}
    UNION ALL
    SELECT loan_token, sum(idle_loan_amount) FROM breakdown GROUP BY vault_id, loan_token
    UNION ALL
    SELECT loan_token, idle_amount FROM vault_idle
),
total AS (
    SELECT sum(amount) AS total_amount FROM all_backing
),
vault_chain AS (
    SELECT chain_id FROM morpho_vault WHERE id = :vault_id AND protocol_id = :protocol_id
)
SELECT t.id AS token_id,
       a.symbol,
       round(sum(a.amount)::numeric, 2) AS backed_amount,
       round((sum(a.amount) / tot.total_amount * 100)::numeric, 2) AS backing_pct
FROM all_backing a
CROSS JOIN total tot
JOIN token t ON t.symbol = a.symbol AND t.chain_id = (SELECT chain_id FROM vault_chain)
GROUP BY t.id, a.symbol, tot.total_amount
HAVING sum(a.amount) > {_MORPHO_DUST_FILTER}
ORDER BY backed_amount DESC;
"""


class MorphoBackedBreakdownRepository:
    """Postgres implementation of the backed breakdown repository for Morpho vaults."""

    def __init__(self, engine: AsyncEngine, protocol_id: int) -> None:
        self._engine = engine
        self._protocol_id = protocol_id

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown:
        """Execute the Morpho vault backed breakdown query and return domain objects."""
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_MORPHO_BACKED_BREAKDOWN_SQL),
                {"vault_id": backed_asset_id, "protocol_id": self._protocol_id},
            )
            rows = result.fetchall()

        items = tuple(
            CollateralContribution(
                token_id=row.token_id,
                symbol=row.symbol,
                amount=Decimal(str(row.backed_amount)),
                backing_pct=Decimal(str(row.backing_pct)),
            )
            for row in rows
        )

        return BackedBreakdown(
            backed_asset_id=backed_asset_id,
            protocol_id=self._protocol_id,
            items=items,
        )
