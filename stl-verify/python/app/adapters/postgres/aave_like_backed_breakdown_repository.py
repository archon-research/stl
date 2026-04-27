# ruff: noqa: E501
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
        ORDER BY b.user_id, b.token_id, b.block_number DESC, b.block_version DESC, b.processing_version DESC
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
            ORDER BY srd.block_number DESC, srd.block_version DESC, srd.processing_version DESC
            LIMIT 1
        ) srd ON srd.usage_as_collateral_enabled = true
        WHERE bc.protocol_id = :protocol_id
        ORDER BY bc.user_id, bc.token_id, bc.block_number DESC, bc.block_version DESC, bc.processing_version DESC
    ) latest
    WHERE collateral_enabled = true
),

-- Step 3: Latest USD price per token from the protocol's oracle
token_prices AS (
    SELECT DISTINCT ON (otp.token_id)
        otp.token_id,
        otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
    WHERE po.protocol_id = :protocol_id
    ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC
),

-- Step 4: Target (backed asset) debt per user, only for users who borrowed it.
user_target_debt AS (
    SELECT ud.user_id,
        SUM(ud.debt_amount) FILTER (WHERE ud.token_id = :backed_asset_id) AS target_debt_amount
    FROM user_debts ud
    GROUP BY ud.user_id
    HAVING SUM(ud.debt_amount) FILTER (WHERE ud.token_id = :backed_asset_id) > 0
),

-- Step 5: Collateral USD value per user per token, and each user's total collateral USD.
-- Only includes users who have target debt (inner join with user_target_debt).
-- Collateral without a price is excluded — it cannot contribute to USD-denominated backing.
user_collateral_usd AS (
    SELECT
        uc.user_id,
        uc.token_id,
        uc.collateral_amount * tp.price_usd AS collateral_usd
    FROM user_collateral uc
    JOIN token_prices tp ON tp.token_id = uc.token_id
    JOIN user_target_debt utd ON utd.user_id = uc.user_id
),
user_total_collateral_usd AS (
    SELECT user_id, SUM(collateral_usd) AS total_collateral_usd
    FROM user_collateral_usd
    GROUP BY user_id
),

-- Step 6: Attribute backing in USD space.
-- Each collateral token's contribution = its share of the user's total collateral USD
-- multiplied by the user's target debt. This ensures SUM(backing_usd) == total target debt,
-- regardless of overcollateralisation.
attributed AS (
    SELECT
        uc.user_id,
        uc.token_id,
        COALESCE((uc.collateral_usd / NULLIF(utc.total_collateral_usd, 0)) * utd.target_debt_amount, 0) AS backing_usd
    FROM user_collateral_usd uc
    JOIN user_total_collateral_usd utc ON utc.user_id = uc.user_id
    JOIN user_target_debt utd ON utd.user_id = uc.user_id
)

-- Step 7: Aggregate across all borrowers
SELECT
    t.id AS token_id,
    t.symbol,
    ROUND(SUM(a.backing_usd)::numeric, 2) AS backing_usd,
    ROUND(
        SUM(a.backing_usd)
        / SUM(SUM(a.backing_usd)) OVER ()
        * 100,
        4
    ) AS backing_pct,
    tp.price_usd
FROM attributed a
JOIN token t ON t.id = a.token_id
LEFT JOIN token_prices tp ON tp.token_id = a.token_id
GROUP BY t.id, t.symbol, tp.price_usd
HAVING SUM(a.backing_usd) > 0
ORDER BY backing_usd DESC;
"""


class AaveLikeBackedBreakdownRepository:
    """Postgres implementation of the backed breakdown repository for Aave-like protocols."""

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
                backing_value=Decimal(str(row.backing_usd)),
                backing_pct=Decimal(str(row.backing_pct)),
                price_usd=Decimal(str(row.price_usd)) if row.price_usd is not None else None,
            )
            for row in rows
        )

        return BackedBreakdown(
            backed_asset_id=backed_asset_id,
            items=items,
        )
