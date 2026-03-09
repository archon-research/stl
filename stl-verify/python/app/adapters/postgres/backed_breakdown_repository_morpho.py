from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)
from app.ports.backed_breakdown_repository import (
    BackedBreakdownRepository as BackedBreakdownRepositoryPort,
)

_MORPHO_BACKED_BREAKDOWN_SQL = """
WITH vault_user AS (
    SELECT u.id as user_id
    FROM morpho_vault mv
    JOIN "user" u ON u.address = mv.address AND u.chain_id = mv.chain_id
    WHERE mv.id = :vault_id
),
vault_state AS (
    SELECT
        vs.total_assets / power(10, t.decimals) as total_assets,
        t.symbol as loan_token,
        t.id as loan_token_id
    FROM morpho_vault_state vs
    JOIN morpho_vault mv ON mv.id = :vault_id
    JOIN token t ON t.id = mv.asset_token_id
    WHERE vs.morpho_vault_id = :vault_id
    ORDER BY vs.block_number DESC, vs.block_version DESC LIMIT 1
),
market_allocs AS (
    SELECT DISTINCT ON (mp.morpho_market_id)
        mp.morpho_market_id,
        ct.id as collateral_token_id,
        ct.symbol as collateral,
        mp.supply_assets / power(10, lt.decimals) as vault_supply
    FROM morpho_market_position mp
    JOIN morpho_market mm ON mm.id = mp.morpho_market_id
    JOIN token ct ON ct.id = mm.collateral_token_id
    JOIN token lt ON lt.id = mm.loan_token_id
    WHERE mp.user_id = (SELECT user_id FROM vault_user)
    ORDER BY mp.morpho_market_id, mp.block_number DESC, mp.block_version DESC
),
market_states AS (
    SELECT ms.*
    FROM market_allocs ma
    JOIN LATERAL (
        SELECT morpho_market_id,
               CASE WHEN total_supply_assets > 0
                   THEN total_borrow_assets::numeric / total_supply_assets::numeric
                   ELSE 0 END as utilization
        FROM morpho_market_state
        WHERE morpho_market_id = ma.morpho_market_id
        ORDER BY block_number DESC, block_version DESC
        LIMIT 1
    ) ms ON true
),
breakdown AS (
    SELECT
        ma.collateral_token_id,
        ma.collateral,
        ma.vault_supply * ms.utilization / vs.total_assets as collateral_pct,
        ma.vault_supply * (1 - ms.utilization) / vs.total_assets as loan_token_pct
    FROM market_allocs ma
    JOIN market_states ms ON ms.morpho_market_id = ma.morpho_market_id
    CROSS JOIN vault_state vs
),
idle_pct AS (
    SELECT 1 - coalesce(sum(collateral_pct + loan_token_pct), 0) as pct
    FROM breakdown
)
SELECT collateral_token_id as token_id,
       collateral as symbol,
       round((collateral_pct * vs.total_assets)::numeric, 2) as backed_amount,
       round((collateral_pct * 100)::numeric, 2) as backing_pct
FROM breakdown, vault_state vs
WHERE collateral_pct > 0.0001
UNION ALL
SELECT (SELECT loan_token_id FROM vault_state),
       (SELECT loan_token FROM vault_state),
       round(((sum(loan_token_pct) + (SELECT pct FROM idle_pct)) * vs.total_assets)::numeric, 2),
       round(((sum(loan_token_pct) + (SELECT pct FROM idle_pct)) * 100)::numeric, 2)
FROM breakdown, vault_state vs
GROUP BY vs.total_assets
ORDER BY backed_amount DESC;
"""


class MorphoBackedBreakdownRepository(BackedBreakdownRepositoryPort):
    """Postgres implementation of the backed breakdown repository for Morpho vaults.

    In this implementation ``debt_token_id`` is interpreted as the Morpho vault ID
    and ``protocol_id`` is ignored (Morpho vault ID fully identifies the context).
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_backed_breakdown(self, protocol_id: int, debt_token_id: int) -> BackedBreakdown:
        """Execute the Morpho vault backed breakdown query and return domain objects.

        Args:
            protocol_id: Ignored for Morpho – present only to satisfy the port interface.
            debt_token_id: The Morpho vault ID to analyze.
        """
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_MORPHO_BACKED_BREAKDOWN_SQL),
                {"vault_id": debt_token_id},
            )
            rows = result.fetchall()

        items = tuple(
            CollateralContribution(
                token_id=row.token_id,
                symbol=row.symbol,
                total_backing_usd=Decimal(str(row.backed_amount)),
                backing_pct=Decimal(str(row.backing_pct)),
            )
            for row in rows
        )

        return BackedBreakdown(
            debt_token_id=debt_token_id,
            protocol_id=protocol_id,
            items=items,
        )
