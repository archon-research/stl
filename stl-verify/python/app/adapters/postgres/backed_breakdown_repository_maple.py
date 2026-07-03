# ruff: noqa: E501
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.logging import get_logger

logger = get_logger(__name__)

# Underlyings whose pool liquidity we value at $1/unit (see the liquidity CTE). A syrup
# pool whose underlying is outside this set contributes NO liquidity row rather than a
# confidently-wrong $1 valuation; the diagnostics query surfaces that omission as a
# warning instead of letting it pass silently.
_STABLE_UNDERLYINGS = ("USDC", "USDT", "USDG", "USDS", "DAI")
_STABLE_IN_LIST = ", ".join(f"'{s}'" for s in _STABLE_UNDERLYINGS)

# Backed breakdown for a Syrup vault: external active-loan collateral aggregated by
# asset symbol, plus the pool's available liquidity (liquid_assets, valued at $1/unit
# stablecoin) as the underlying component. Internal (amm/strategy) loans are excluded
# because their collateral is a par-USDC placeholder, not real backing
# (docs/maple_spec.md "loanMeta and Internal Maple Positions"). Collateral values are
# Maple-attested (asset_value_usd, 8 decimals) — see "Collateral Data Provenance".
#
# is_syrup and is_internal are editorial attributes that live in the SCD2 satellites
# (maple_pool_meta / maple_loan_meta), so we read them through the maple_pool_current /
# maple_loan_current views (hub identity + latest live satellite row), not the hubs.
_POOL_ID_SQL = """
SELECT id FROM maple_pool_current WHERE chain_id = :chain_id AND address = :addr AND is_syrup
"""

# Shared CTE prefix for the breakdown and diagnostics queries: resolves the syrup pool,
# its current sync cycle, and the current-cycle Active external loans. Both queries build
# on it so the "what we counted" and "what we dropped" views stay in lockstep.
#
# Collateral is tied to the SAME snapshot (synced_at, processing_version) as the latest
# Active loan state so we never mix a stale collateral row with a fresh principal. The
# Maple indexer writes loan_state and loan_collateral together per cron cycle/build, so
# their processing_version stay in lockstep.
#
# loan_latest is ALSO bounded to the pool's current cycle (pool_cycle). This is
# load-bearing: the indexer only queries Active loans and never emits tombstones
# (is_present stays TRUE — maple_satellite.go "no deletion-detection path exists"), so a
# repaid/closed loan simply stops receiving new rows while its last Active state +
# collateral linger in maple_loan_current forever. Without the cycle bound those stale
# rows would inflate backing indefinitely as loans repay. Anchoring on the pool cycle
# drops any loan absent from the newest cycle.
_POOL_CYCLE_LOANS_CTE = """
WITH pool AS (
    SELECT mp.id AS pool_id, ut.symbol AS underlying_symbol, ut.decimals AS underlying_decimals
    FROM maple_pool_current mp
    JOIN token ut ON ut.id = mp.asset_token_id
    WHERE mp.chain_id = :chain_id AND mp.address = :addr AND mp.is_syrup
),
pool_cycle AS (
    -- The pool's current sync cycle: the newest synced_at written for this pool.
    -- maple_pool_state gets a fresh row every cycle for every syrup pool (independent
    -- of loan activity), so its max synced_at is the authoritative cycle anchor even
    -- when every loan repaid in one cycle. A loan-derived anchor would fail that case.
    SELECT max(synced_at) AS synced_at
    FROM maple_pool_state
    WHERE maple_pool_id = (SELECT pool_id FROM pool)
),
loan_latest AS (
    SELECT l.id AS loan_id, ls.synced_at, ls.processing_version
    FROM maple_loan_current l
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
      AND ls.synced_at = (SELECT synced_at FROM pool_cycle)
)
"""

_MAPLE_BACKED_BREAKDOWN_SQL = (
    _POOL_CYCLE_LOANS_CTE
    + f""",
coll AS (
    SELECT c.asset_symbol AS symbol,
           c.asset_amount / power(10, c.asset_decimals)::numeric AS amount,
           c.asset_value_usd / 100000000                         AS price_usd
    FROM loan_latest ll
    JOIN maple_loan_collateral c
      ON c.maple_loan_id      = ll.loan_id
     AND c.synced_at          = ll.synced_at
     AND c.processing_version = ll.processing_version
    WHERE c.asset_amount    IS NOT NULL
      AND c.asset_value_usd IS NOT NULL
),
liquidity AS (
    -- BREAKING ASSUMPTION: pool liquidity is valued at $1/unit. This is correct ONLY
    -- because every Syrup vault today is stablecoin-denominated (syrupUSDC/USDT/USDG).
    -- `is_syrup` does NOT guarantee a $1-pegged underlying — that is a Maple product
    -- fact, not a schema constraint. The underlying_symbol allowlist below makes the
    -- assumption explicit: a future non-stable (or renamed) syrup underlying contributes
    -- NO liquidity row rather than a confidently-wrong $1 valuation. If Maple ships a
    -- non-stable syrup vault, price liquid_assets via an oracle here instead of widening
    -- this list.
    SELECT p.underlying_symbol AS symbol,
           lps.liquid_assets / power(10, p.underlying_decimals)::numeric AS amount,
           1.0::numeric                                                  AS price_usd
    FROM pool p
    JOIN LATERAL (
        SELECT liquid_assets
        FROM maple_pool_state mps
        WHERE mps.maple_pool_id = p.pool_id
        ORDER BY mps.synced_at DESC, mps.processing_version DESC
        LIMIT 1
    ) lps ON true
    WHERE p.underlying_symbol IN ({_STABLE_IN_LIST})
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
)

# Observability companion to the breakdown query: reports the silent degrades the
# breakdown itself cannot surface — collateral rows dropped for null amount/value (so
# backing_pct is recomputed over survivors), whether a pool_state liquidity snapshot
# exists, and whether the underlying is priceable at $1. pool_cycle always yields one
# row (max() over zero rows is NULL), so has_pool_state is a clean boolean.
_MAPLE_BREAKDOWN_DIAGNOSTICS_SQL = (
    _POOL_CYCLE_LOANS_CTE
    + """,
dropped AS (
    SELECT count(*) AS null_collateral_count
    FROM loan_latest ll
    JOIN maple_loan_collateral c
      ON c.maple_loan_id      = ll.loan_id
     AND c.synced_at          = ll.synced_at
     AND c.processing_version = ll.processing_version
    WHERE c.asset_amount IS NULL OR c.asset_value_usd IS NULL
)
SELECT (SELECT underlying_symbol FROM pool)           AS underlying_symbol,
       (SELECT synced_at IS NOT NULL FROM pool_cycle) AS has_pool_state,
       (SELECT null_collateral_count FROM dropped)    AS null_collateral_count
"""
)


class MapleBackedBreakdownRepository:
    """Postgres backed-breakdown repository for Maple Syrup vaults (symbol-keyed)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def resolve_pool_id(self, address: bytes, chain_id: int) -> int | None:
        """Resolve a Syrup pool's internal ID from its onchain vault address."""
        async with self._engine.connect() as connection:
            result = await connection.execute(text(_POOL_ID_SQL), {"addr": address, "chain_id": chain_id})
            row = result.fetchone()
        return row.id if row is not None else None

    async def get_backed_breakdown(self, receipt_token_address: bytes, chain_id: int) -> BackedBreakdown:
        """Execute the Maple Syrup backed breakdown query and return domain objects."""
        pool_id = await self.resolve_pool_id(receipt_token_address, chain_id)
        if pool_id is None:
            # The syrup receipt token is registered (static migration seed) but no
            # matching is_syrup pool resolves. Degrade to an empty breakdown rather
            # than raising: the asset is known, its backing data is just not
            # available (Maple has no share-warmup concept — this is the graceful
            # "no data yet" 200). Log it so the empty degrade is observable and the
            # normal "indexer hasn't created the pool yet" case is distinguishable
            # from a real defect (is_syrup=false on an indexed pool, or a registry
            # vs maple_pool_current address-byte mismatch), which look identical here.
            logger.warning(
                "Maple syrup pool not found for receipt token address=0x%s chain_id=%d; returning empty breakdown",
                receipt_token_address.hex(),
                chain_id,
            )
            return BackedBreakdown(backed_asset_id=0, items=())
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_MAPLE_BACKED_BREAKDOWN_SQL),
                {"addr": receipt_token_address, "chain_id": chain_id},
            )
            rows = result.fetchall()
            await self._log_degrades(connection, receipt_token_address, chain_id, pool_id)
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

    async def _log_degrades(
        self, connection: AsyncConnection, receipt_token_address: bytes, chain_id: int, pool_id: int
    ) -> None:
        """Warn on the breakdown's silent degrades (dropped rows, omitted liquidity)."""
        result = await connection.execute(
            text(_MAPLE_BREAKDOWN_DIAGNOSTICS_SQL),
            {"addr": receipt_token_address, "chain_id": chain_id},
        )
        diag = result.fetchone()
        if diag is None:
            return
        if diag.null_collateral_count:
            logger.warning(
                "Maple breakdown pool_id=%d dropped %d active-loan collateral row(s) with null amount/value; "
                "backing_pct is recomputed over the surviving rows and understates total backing",
                pool_id,
                diag.null_collateral_count,
            )
        if not diag.has_pool_state:
            logger.warning(
                "Maple breakdown pool_id=%d has no pool_state snapshot; pool liquidity is omitted from backing",
                pool_id,
            )
        elif diag.underlying_symbol not in _STABLE_UNDERLYINGS:
            logger.warning(
                "Maple breakdown pool_id=%d underlying %s is outside the stablecoin allowlist %s; pool liquidity "
                "is omitted rather than valued at $1 — price liquid_assets via an oracle before adding it",
                pool_id,
                diag.underlying_symbol,
                _STABLE_UNDERLYINGS,
            )
