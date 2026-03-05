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

-- Step 2: Latest collateral snapshot per user per token
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

-- Step 3: Latest price per token (onchain oracle)
latest_prices AS (
    SELECT DISTINCT ON (otp.token_id)
        otp.token_id,
        otp.price_usd
    FROM onchain_token_price otp
    ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC
),

-- Step 4: USD values for debts
user_debts_usd AS (
    SELECT
        ud.user_id,
        ud.token_id,
        ud.debt_amount * lp.price_usd AS debt_usd
    FROM user_debts ud
    JOIN latest_prices lp ON lp.token_id = ud.token_id
),

-- Step 5: Total debt per user and target debt token share
-- Only includes users who actually hold the target debt token
user_debt_totals AS (
    SELECT
        udu.user_id,
        SUM(udu.debt_usd)                                                AS total_debt_usd,
        SUM(udu.debt_usd) FILTER (WHERE udu.token_id = :debt_token_id)              AS target_debt_usd
    FROM user_debts_usd udu
    GROUP BY udu.user_id
    HAVING SUM(udu.debt_usd) FILTER (WHERE udu.token_id = :debt_token_id) > 0
),

-- Step 6: USD values for collateral
user_collateral_usd AS (
    SELECT
        uc.user_id,
        uc.token_id,
        uc.collateral_amount * lp.price_usd AS collateral_usd
    FROM user_collateral uc
    JOIN latest_prices lp ON lp.token_id = uc.token_id
),

-- Step 7: Attribute each user's collateral to target debt token proportionally
attributed AS (
    SELECT
        ucu.user_id,
        ucu.token_id,
        ucu.collateral_usd * (udt.target_debt_usd / udt.total_debt_usd) AS collateral_attributed_usd
    FROM user_collateral_usd ucu
    JOIN user_debt_totals udt ON udt.user_id = ucu.user_id
)

-- Step 8: Aggregate across all borrowers
SELECT
    t.id AS token_id,
    t.symbol,
    ROUND(SUM(a.collateral_attributed_usd), 2) AS total_backing_usd,
    ROUND(
        SUM(a.collateral_attributed_usd)
        / SUM(SUM(a.collateral_attributed_usd)) OVER ()
        * 100,
        4
    ) AS backing_pct
FROM attributed a
JOIN token t ON t.id = a.token_id
GROUP BY t.id, t.symbol
ORDER BY total_backing_usd DESC;
"""


class BackedBreakdownRepository(BackedBreakdownRepositoryPort):
    """Postgres implementation of the backed breakdown repository."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_backed_breakdown(self, protocol_id: int, debt_token_id: int) -> BackedBreakdown:
        """Execute the backed breakdown query and return domain objects."""
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_BACKED_BREAKDOWN_SQL),
                {"protocol_id": protocol_id, "debt_token_id": debt_token_id},
            )
            rows = result.fetchall()

        items = tuple(
            CollateralContribution(
                token_id=row.token_id,
                symbol=row.symbol,
                total_backing_usd=Decimal(str(row.total_backing_usd)),
                backing_pct=Decimal(str(row.backing_pct)),
            )
            for row in rows
        )

        return BackedBreakdown(
            debt_token_id=debt_token_id,
            protocol_id=protocol_id,
            items=items,
        )
