# ruff: noqa: E501
from collections.abc import Sequence
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

-- Step 3: Latest USD price per token from the protocol's oracles.
-- Only rows whose (oracle_id, token_id) still has an ENABLED oracle_asset
-- mapping are eligible; a retired source is excluded immediately at read time
-- (canonical rationale, incl. the no-history tradeoff, on _DIRECT_ASSET_HOLDINGS_SQL
-- in allocation_position_repository.py). oracle_id then breaks any remaining
-- same-snapshot-key tie deterministically (higher id = later-registered oracle).
token_prices AS (
    SELECT DISTINCT ON (otp.token_id)
        otp.token_id,
        otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
    WHERE po.protocol_id = :protocol_id
      AND EXISTS (
          SELECT 1 FROM oracle_asset oa
          WHERE oa.oracle_id = otp.oracle_id
            AND oa.token_id = otp.token_id
            AND oa.enabled
      )
    ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC, otp.oracle_id DESC
),

-- Step 4: Per (user, backed asset) target debt, for each requested backed asset.
-- user_debts already holds one row per (user, token) with debt_amount > 0, so a
-- user's target debt for backed asset X is simply their user_debts row for X.
-- Batching over :backed_asset_ids lets the protocol-wide CTEs above run ONCE for
-- the whole set instead of once per asset (the dominant /risk-capital cost).
user_target_debt AS (
    SELECT user_id, token_id AS backed_asset_id, debt_amount AS target_debt_amount
    FROM user_debts
    WHERE token_id = ANY(CAST(:backed_asset_ids AS bigint[]))
),

-- Step 5: Collateral USD value per user per token, and each user's total collateral USD.
-- Restricted to users who have target debt in at least one requested backed asset.
-- Collateral without a price is excluded — it cannot contribute to USD-denominated backing.
user_collateral_usd AS (
    SELECT
        uc.user_id,
        uc.token_id,
        uc.collateral_amount * tp.price_usd AS collateral_usd
    FROM user_collateral uc
    JOIN token_prices tp ON tp.token_id = uc.token_id
    WHERE uc.user_id IN (SELECT user_id FROM user_target_debt)
),
user_total_collateral_usd AS (
    SELECT user_id, SUM(collateral_usd) AS total_collateral_usd
    FROM user_collateral_usd
    GROUP BY user_id
),

-- Step 6: Attribute backing in USD space, per backed asset.
-- Each collateral token's contribution = its share of the user's total collateral USD
-- multiplied by the user's target debt for that backed asset. Joining each user's
-- collateral to every backed asset they borrowed reproduces, in one pass, exactly
-- what the per-asset query produced for each backed asset separately.
attributed AS (
    SELECT
        utd.backed_asset_id,
        uc.token_id,
        COALESCE((uc.collateral_usd / NULLIF(utc.total_collateral_usd, 0)) * utd.target_debt_amount, 0) AS backing_usd
    FROM user_collateral_usd uc
    JOIN user_total_collateral_usd utc ON utc.user_id = uc.user_id
    JOIN user_target_debt utd ON utd.user_id = uc.user_id
)

-- Step 7: Aggregate across all borrowers, per backed asset. backing_pct is relative
-- to each backed asset's own total backing (PARTITION BY backed_asset_id).
SELECT
    a.backed_asset_id,
    t.id AS token_id,
    t.symbol,
    ROUND(SUM(a.backing_usd)::numeric, 2) AS backing_usd,
    ROUND(
        SUM(a.backing_usd)
        / SUM(SUM(a.backing_usd)) OVER (PARTITION BY a.backed_asset_id)
        * 100,
        4
    ) AS backing_pct,
    tp.price_usd
FROM attributed a
JOIN token t ON t.id = a.token_id
LEFT JOIN token_prices tp ON tp.token_id = a.token_id
GROUP BY a.backed_asset_id, t.id, t.symbol, tp.price_usd
HAVING SUM(a.backing_usd) > 0
ORDER BY a.backed_asset_id, backing_usd DESC;
"""


class AaveLikeBackedBreakdownRepository:
    """Postgres implementation of the backed breakdown repository for Aave-like protocols."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_backed_breakdowns(
        self, protocol_id: int, backed_asset_ids: Sequence[int]
    ) -> dict[int, BackedBreakdown]:
        """Backed breakdown for many backed assets of one protocol in a single query.

        The protocol-wide debt/collateral/price CTEs are evaluated once for the
        whole set rather than once per asset (the dominant ``/risk-capital`` cost),
        then results are grouped back per backed asset. Every requested id gets an
        entry; assets with no backing map to an empty breakdown.
        """
        ids = list(dict.fromkeys(backed_asset_ids))
        if not ids:
            return {}

        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_BACKED_BREAKDOWN_SQL),
                {"protocol_id": protocol_id, "backed_asset_ids": ids},
            )
            rows = result.fetchall()

        items_by_asset: dict[int, list[CollateralContribution]] = {aid: [] for aid in ids}
        for row in rows:
            items_by_asset[row.backed_asset_id].append(
                CollateralContribution(
                    token_id=row.token_id,
                    symbol=row.symbol,
                    backing_value=Decimal(str(row.backing_usd)),
                    backing_pct=Decimal(str(row.backing_pct)),
                    price_usd=Decimal(str(row.price_usd)) if row.price_usd is not None else None,
                )
            )
        return {aid: BackedBreakdown(backed_asset_id=aid, items=tuple(items)) for aid, items in items_by_asset.items()}

    async def get_backed_breakdown(self, protocol_id: int, backed_asset_id: int) -> BackedBreakdown:
        """Single-asset breakdown — delegates to the batched query with one id."""
        breakdowns = await self.get_backed_breakdowns(protocol_id, [backed_asset_id])
        return breakdowns.get(backed_asset_id, BackedBreakdown(backed_asset_id=backed_asset_id, items=()))
