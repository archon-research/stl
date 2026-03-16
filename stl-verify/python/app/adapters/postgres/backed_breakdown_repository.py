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

_BACKED_BREAKDOWN_SQL = """
-- Step 1: Current debt per user per token (sum of deltas)
WITH user_debts AS (
    SELECT
        b.user_id,
        b.token_id,
        SUM(b.amount) AS debt_amount
    FROM borrower b
    WHERE b.protocol_id = :protocol_id
    GROUP BY b.user_id, b.token_id
    HAVING SUM(b.amount) > 0
),

-- Step 2: Latest collateral snapshot per user per token (already in human-readable units)
-- Joins reserve config to exclude assets disabled at the protocol level
user_collateral AS (
    SELECT DISTINCT ON (bc.user_id, bc.token_id)
        bc.user_id,
        bc.token_id,
        bc.amount AS collateral_amount
    FROM borrower_collateral bc
    JOIN LATERAL (
        SELECT usage_as_collateral_enabled
        FROM sparklend_reserve_data srd
        WHERE srd.token_id = bc.token_id
          AND srd.protocol_id = :protocol_id
        ORDER BY srd.block_number DESC, srd.block_version DESC
        LIMIT 1
    ) srd ON srd.usage_as_collateral_enabled = true
    WHERE bc.protocol_id = :protocol_id
      AND bc.collateral_enabled = true
    ORDER BY bc.user_id, bc.token_id, bc.block_number DESC, bc.block_version DESC
),

-- Step 3: Total debt per user and target debt token share (in raw token units)
-- Only includes users who actually hold the target debt token
user_debt_totals AS (
    SELECT
        ud.user_id,
        SUM(ud.debt_amount)                                                         AS total_debt_amount,
        SUM(ud.debt_amount) FILTER (WHERE ud.token_id = :backed_asset_id)             AS target_debt_amount
    FROM user_debts ud
    GROUP BY ud.user_id
    HAVING SUM(ud.debt_amount) FILTER (WHERE ud.token_id = :backed_asset_id) > 0
),

    -- Step 4: Attribute each user's collateral to the target debt token
attributed AS (
    SELECT
        uc.user_id,
        uc.token_id,
        uc.collateral_amount * (udt.target_debt_amount / udt.total_debt_amount) AS collateral_attributed
    FROM user_collateral uc
    JOIN user_debt_totals udt ON udt.user_id = uc.user_id
)

-- Step 5: Aggregate across all borrowers
SELECT
    t.id AS token_id,
    t.symbol,
    ROUND(SUM(a.collateral_attributed)::numeric, 8) AS amount,
    ROUND(
        SUM(a.collateral_attributed)
        / SUM(SUM(a.collateral_attributed)) OVER ()
        * 100,
        4
    ) AS backing_pct
FROM attributed a
JOIN token t ON t.id = a.token_id
GROUP BY t.id, t.symbol
ORDER BY amount DESC;
"""


class BackedBreakdownRepository(BackedBreakdownRepositoryPort):
    """Postgres implementation of the backed breakdown repository."""

    def __init__(self, engine: AsyncEngine, protocol_id: int) -> None:
        self._engine = engine
        self._protocol_id = protocol_id

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown:
        """Execute the backed breakdown query and return domain objects."""
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_BACKED_BREAKDOWN_SQL),
                {"protocol_id": self._protocol_id, "backed_asset_id": backed_asset_id},
            )
            rows = result.fetchall()

        items = tuple(
            CollateralContribution(
                token_id=row.token_id,
                symbol=row.symbol,
                amount=Decimal(str(row.amount)),
                backing_pct=Decimal(str(row.backing_pct)),
            )
            for row in rows
        )

        return BackedBreakdown(
            backed_asset_id=backed_asset_id,
            protocol_id=self._protocol_id,
            items=items,
        )
