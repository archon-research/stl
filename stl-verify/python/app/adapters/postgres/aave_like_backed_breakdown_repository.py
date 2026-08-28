# ruff: noqa: E501
from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.reference_as_of import ReferenceAsOf, ReferenceEffectiveAtProvider
from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)

_BACKED_BREAKDOWN_SQL = """
-- Reads the trigger-maintained *_current tables (newest row per key, see
-- 20260820_120000_create_current_position_tables.sql) instead of recomputing
-- latest-over-history per request: the histories are mostly-compressed
-- hypertables, so no index makes that scan cheap.

-- Step 1: Current debt per user per token, scaled to human-readable units.
-- amount is a balance snapshot, never a delta, so there is nothing to sum.
WITH user_debts AS (
    SELECT b.user_id, b.token_id,
           b.amount / power(10::numeric, t.decimals) AS debt_amount
    FROM borrower_current b
    JOIN token t ON t.id = b.token_id
    WHERE b.protocol_id = :protocol_id
      AND b.amount > 0
),

-- Step 2: Current collateral per user per token, for deposits the user still has
-- enabled as collateral and whose token the protocol still accepts as collateral.
user_collateral AS (
    SELECT bc.user_id, bc.token_id,
           bc.amount / power(10::numeric, t.decimals) AS collateral_amount
    FROM borrower_collateral_current bc
    JOIN token t ON t.id = bc.token_id
    JOIN sparklend_reserve_data_current srd
        ON srd.token_id = bc.token_id AND srd.protocol_id = :protocol_id
       AND srd.usage_as_collateral_enabled = true
    WHERE bc.protocol_id = :protocol_id
      AND bc.collateral_enabled = true
),

-- Step 3: Current USD price per token from the protocol's oracles.
-- Which protocols an oracle serves, and whether its mapping was enabled, are resolved here
-- and pinned to :reference_effective_at rather than at write time (canonical rationale on
-- _DIRECT_ASSET_HOLDINGS_SQL in allocation_position_repository.py). FROM is
-- token_price_current, not the onchain_token_price hypertable, so the ranking runs over one
-- row per (oracle, token) rather than all of history. oracle_id breaks any remaining
-- same-snapshot-key tie deterministically (higher id = later-registered oracle).
token_prices AS (
    SELECT DISTINCT ON (tpc.token_id)
        tpc.token_id,
        tpc.price_usd
    FROM token_price_current tpc
    JOIN protocol_oracle po ON po.oracle_id = tpc.oracle_id
    WHERE po.protocol_id = :protocol_id
      AND EXISTS (
          SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa
          WHERE oa.oracle_id = tpc.oracle_id
            AND oa.token_id = tpc.token_id
            AND oa.enabled
      )
    ORDER BY tpc.token_id, tpc.block_number DESC, tpc.block_version DESC, tpc.processing_version DESC, tpc.oracle_id DESC
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

-- Step 5: Collateral USD value per user per token. Collateral without a price is
-- excluded — it cannot contribute to USD-denominated backing. Not pre-filtered to
-- users with target debt (`WHERE uc.user_id IN (SELECT user_id FROM
-- user_target_debt)`); the join in step 6 restricts to the same users anyway. That
-- pre-filter was dropped because it collapsed the row estimate to 1 and the planner
-- picked a quadratic nested loop — but that was measured against the compressed
-- hypertable scan this query no longer does, so the reasoning no longer describes
-- the plan. Whether to restore it is VEC-614: it needs an EXPLAIN ANALYZE per
-- protocol at full scale, because without it the window sum in step 6 sorts every
-- collateral row of the protocol (~164k keys for Aave V3 vs ~6k for SparkLend).
user_collateral_usd AS (
    SELECT
        uc.user_id,
        uc.token_id,
        uc.collateral_amount * tp.price_usd AS collateral_usd
    FROM user_collateral uc
    JOIN token_prices tp ON tp.token_id = uc.token_id
),

-- Step 6: Attribute backing in USD space, per backed asset. Each collateral
-- token's contribution = its share of the user's total collateral USD multiplied
-- by the user's target debt for that backed asset. The per-user total is a window
-- sum inline: as a second CTE referencing user_collateral_usd it gets materialized
-- with a broken row estimate, which again costs a quadratic nested loop.
attributed AS (
    SELECT utd.backed_asset_id, x.token_id,
           COALESCE((x.collateral_usd / NULLIF(x.total_collateral_usd, 0)) * utd.target_debt_amount, 0) AS backing_usd
    FROM (
        SELECT user_id, token_id, collateral_usd,
               SUM(collateral_usd) OVER (PARTITION BY user_id) AS total_collateral_usd
        FROM user_collateral_usd
    ) x
    JOIN user_target_debt utd ON utd.user_id = x.user_id
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

    def __init__(self, engine: AsyncEngine, reference_effective_at: ReferenceEffectiveAtProvider) -> None:
        self._engine = engine
        self._reference = ReferenceAsOf(reference_effective_at)

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
                self._reference.params(protocol_id=protocol_id, backed_asset_ids=ids),
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
