from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)

_BACKED_BREAKDOWN_SQL = """
-- Step 1: Latest debt snapshot per user per token, scaled to human-readable units.
-- Uses DISTINCT ON to select the most-recent row; does NOT sum across events because
-- the writer stores a running balance snapshot on every Borrow/Repay, not signed deltas.
WITH user_debts AS (
    SELECT user_id, token_id, debt_amount
    FROM (
        SELECT DISTINCT ON (b.user_id, b.token_id)
            b.user_id,
            b.token_id,
            b.amount / power(10::numeric, t.decimals) AS debt_amount
        FROM borrower b
        JOIN token t ON t.id = b.token_id
        WHERE b.protocol_id = :protocol_id
        ORDER BY b.user_id, b.token_id, b.block_number DESC, b.block_version DESC
    ) latest
    WHERE debt_amount > 0
),

-- Step 2: Latest collateral snapshot per user per token, scaled to human-readable units.
-- DISTINCT ON picks the most-recent row first; the outer WHERE then filters on the
-- collateral_enabled flag from that latest row, so a subsequent disable event correctly
-- excludes the position even though earlier enabled rows exist.
-- The LATERAL join additionally excludes assets disabled at the protocol level.
user_collateral AS (
    SELECT user_id, token_id, collateral_amount
    FROM (
        SELECT DISTINCT ON (bc.user_id, bc.token_id)
            bc.user_id,
            bc.token_id,
            bc.amount / power(10::numeric, t.decimals) AS collateral_amount,
            bc.collateral_enabled
        FROM borrower_collateral bc
        JOIN token t ON t.id = bc.token_id
        JOIN LATERAL (
            SELECT usage_as_collateral_enabled
            FROM sparklend_reserve_data srd
            WHERE srd.token_id = bc.token_id
              AND srd.protocol_id = :protocol_id
            ORDER BY srd.block_number DESC, srd.block_version DESC
            LIMIT 1
        ) srd ON srd.usage_as_collateral_enabled = true
        WHERE bc.protocol_id = :protocol_id
        ORDER BY bc.user_id, bc.token_id, bc.block_number DESC, bc.block_version DESC
    ) latest
    WHERE collateral_enabled = true
),

-- Step 3: Total and target debt per user (only users who borrowed the target token)
user_debt_totals AS (
    SELECT
        ud.user_id,
        SUM(ud.debt_amount)                                                         AS total_debt_amount,
        SUM(ud.debt_amount) FILTER (WHERE ud.token_id = :backed_asset_id)           AS target_debt_amount
    FROM user_debts ud
    GROUP BY ud.user_id
    HAVING SUM(ud.debt_amount) FILTER (WHERE ud.token_id = :backed_asset_id) > 0
),

-- Step 4: Attribute each user's collateral proportionally to the target debt token
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


class BackedBreakdownRepository:
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
