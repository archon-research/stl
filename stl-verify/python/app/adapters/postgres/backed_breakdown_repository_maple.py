# ruff: noqa: E501
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution

# Backed breakdown for a Syrup vault: external active-loan collateral aggregated by
# asset symbol, plus the pool's available liquidity (liquid_assets, valued at $1/unit
# stablecoin) as the underlying component. Internal (amm/strategy) loans are excluded
# because their collateral is a par-USDC placeholder, not real backing
# (docs/maple_spec.md "loanMeta and Internal Maple Positions"). Collateral values are
# Maple-attested (asset_value_usd, 8 decimals) — see "Collateral Data Provenance".
_POOL_ID_SQL = """
SELECT id FROM maple_pool WHERE chain_id = :chain_id AND address = :addr AND is_syrup
"""

# Collateral is tied to the SAME snapshot (synced_at, processing_version) as the latest
# Active loan state so we never mix a stale collateral row with a fresh principal. The
# Maple indexer writes loan_state and loan_collateral together per cron cycle/build, so
# their processing_version stay in lockstep.
_MAPLE_BACKED_BREAKDOWN_SQL = """
WITH pool AS (
    SELECT mp.id AS pool_id, ut.symbol AS underlying_symbol, ut.decimals AS underlying_decimals
    FROM maple_pool mp
    JOIN token ut ON ut.id = mp.asset_token_id
    WHERE mp.chain_id = :chain_id AND mp.address = :addr AND mp.is_syrup
),
loan_latest AS (
    SELECT l.id AS loan_id, ls.synced_at, ls.processing_version
    FROM maple_loan l
    JOIN LATERAL (
        SELECT synced_at, processing_version, state
        FROM maple_loan_state s
        WHERE s.maple_loan_id = l.id
        ORDER BY s.synced_at DESC, s.processing_version DESC
        LIMIT 1
    ) ls ON true
    WHERE l.maple_pool_id = (SELECT pool_id FROM pool)
      AND NOT l.is_internal
      AND ls.state = 'Active'
),
coll AS (
    SELECT c.asset_symbol AS symbol,
           c.asset_amount / power(10, c.asset_decimals)        AS amount,
           c.asset_value_usd / 100000000.0                     AS price_usd
    FROM loan_latest ll
    JOIN maple_loan_collateral c
      ON c.maple_loan_id      = ll.loan_id
     AND c.synced_at          = ll.synced_at
     AND c.processing_version = ll.processing_version
    WHERE c.asset_amount    IS NOT NULL
      AND c.asset_value_usd IS NOT NULL
),
liquidity AS (
    SELECT p.underlying_symbol AS symbol,
           lps.liquid_assets / power(10, p.underlying_decimals) AS amount,
           1.0                                                  AS price_usd
    FROM pool p
    JOIN LATERAL (
        SELECT liquid_assets
        FROM maple_pool_state mps
        WHERE mps.maple_pool_id = p.pool_id
        ORDER BY mps.synced_at DESC, mps.processing_version DESC
        LIMIT 1
    ) lps ON true
),
all_rows AS (
    SELECT symbol, amount, amount * price_usd AS amount_usd FROM coll
    UNION ALL
    SELECT symbol, amount, amount * price_usd FROM liquidity
),
agg AS (
    SELECT symbol, sum(amount) AS amount, sum(amount_usd) AS amount_usd
    FROM all_rows GROUP BY symbol
),
total AS (SELECT sum(amount_usd) AS total_usd FROM agg)
SELECT a.symbol,
       round(a.amount_usd::numeric, 2)                                       AS amount_usd,
       round(CASE WHEN a.amount > 0 THEN (a.amount_usd / a.amount) ELSE 0 END::numeric, 8) AS price_usd,
       round((a.amount_usd / NULLIF(t.total_usd, 0) * 100)::numeric, 2)      AS backing_pct
FROM agg a, total t
ORDER BY amount_usd DESC
"""


class MapleBackedBreakdownRepository:
    """Postgres backed-breakdown repository for Maple Syrup vaults (symbol-keyed)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def resolve_pool_id(self, address: bytes, chain_id: int) -> int | None:
        async with self._engine.connect() as conn:
            result = await conn.execute(text(_POOL_ID_SQL), {"addr": address, "chain_id": chain_id})
            row = result.fetchone()
        return row.id if row is not None else None

    async def get_backed_breakdown(self, receipt_token_address: bytes, chain_id: int) -> BackedBreakdown:
        pool_id = await self.resolve_pool_id(receipt_token_address, chain_id)
        if pool_id is None:
            raise ValueError(f"maple syrup pool not found for {receipt_token_address.hex()} on chain {chain_id}")
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(_MAPLE_BACKED_BREAKDOWN_SQL),
                {"addr": receipt_token_address, "chain_id": chain_id},
            )
            rows = result.fetchall()
        items = tuple(
            CollateralContribution(
                token_id=None,
                symbol=row.symbol,
                backing_value=Decimal(str(row.amount_usd)),
                backing_pct=Decimal(str(row.backing_pct)),
                price_usd=Decimal(str(row.price_usd)),
            )
            for row in rows
        )
        return BackedBreakdown(backed_asset_id=pool_id, items=items)
