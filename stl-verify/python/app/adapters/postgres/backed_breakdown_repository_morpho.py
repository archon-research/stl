# ruff: noqa: E501
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

# Minimum collateral amount (in loan-token units) to include in the breakdown.
# Filters out dust positions that would add noise to the percentage calculation.
_MIN_COLLATERAL_AMOUNT = "0.01"

from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)

_VAULT_ID_SQL = """
SELECT id FROM morpho_vault WHERE address = :addr AND chain_id = :chain_id
"""

_MORPHO_BACKED_BREAKDOWN_SQL = f"""
WITH morpho_vaults AS (
      SELECT mv.id as vault_id
      FROM morpho_vault mv
      WHERE mv.id = :backed_asset_id
  ),
  vault_users AS (
      SELECT mv.vault_id, u.id as user_id
      FROM morpho_vaults mv
      JOIN morpho_vault v ON v.id = mv.vault_id
      JOIN "user" u ON u.address = v.address AND u.chain_id = v.chain_id
  ),
  vault_states AS (
      SELECT DISTINCT ON (vs.morpho_vault_id)
          vs.morpho_vault_id as vault_id,
          vs.total_assets / power(10, t.decimals) as total_assets,
          t.id as loan_token_id,
          t.symbol as loan_token
      FROM morpho_vault_state vs
      JOIN morpho_vault v ON v.id = vs.morpho_vault_id
      JOIN token t ON t.id = v.asset_token_id
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
             ct.id as collateral_token_id,
             ct.symbol as collateral,
             pos.supply_assets / power(10, lt.decimals) as vault_supply
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
                     ELSE 0 END as utilization
          FROM morpho_market_state
          WHERE morpho_market_id = ma.morpho_market_id
          ORDER BY block_number DESC, block_version DESC
          LIMIT 1
      ) ms ON true
  ),
  breakdown AS (
      SELECT
          ma.vault_id,
          ma.collateral_token_id,
          ma.collateral,
          ma.vault_supply * ms.utilization as collateral_amount,
          ma.vault_supply * (1 - ms.utilization) as idle_loan_amount,
          vs.loan_token_id,
          vs.loan_token
      FROM market_allocs ma
      JOIN market_states ms ON ms.morpho_market_id = ma.morpho_market_id
      JOIN vault_states vs ON vs.vault_id = ma.vault_id
  ),
  vault_idle AS (
      SELECT vs.vault_id, vs.loan_token_id, vs.loan_token,
             vs.total_assets - coalesce(sum(b.collateral_amount + b.idle_loan_amount), 0) as idle_amount
      FROM vault_states vs
      LEFT JOIN breakdown b ON b.vault_id = vs.vault_id
      GROUP BY vs.vault_id, vs.loan_token_id, vs.loan_token, vs.total_assets
  ),
  all_backing AS (
      SELECT collateral_token_id as token_id, collateral as symbol, collateral_amount as amount FROM breakdown
      WHERE collateral_amount > {_MIN_COLLATERAL_AMOUNT}
      UNION ALL
      SELECT loan_token_id, loan_token, sum(idle_loan_amount) FROM breakdown GROUP BY vault_id, loan_token_id, loan_token
      UNION ALL
      SELECT loan_token_id, loan_token, idle_amount FROM vault_idle
  ),
  total AS (
      SELECT sum(amount) as total_amount FROM all_backing
  )
  SELECT a.token_id,
         a.symbol,
         round(sum(a.amount)::numeric, 2) as backed_amount,
         round((sum(a.amount) / t.total_amount * 100)::numeric, 2) as backing_pct
  FROM all_backing a, total t
  GROUP BY a.token_id, a.symbol, t.total_amount
  HAVING sum(a.amount) > {_MIN_COLLATERAL_AMOUNT}
  ORDER BY backed_amount DESC
"""


class MorphoBackedBreakdownRepository:
    """Postgres implementation of the backed breakdown repository for Morpho vaults."""

    def __init__(self, engine: AsyncEngine, protocol_id: int) -> None:
        self._engine = engine
        self._protocol_id = protocol_id

    async def resolve_vault_id(self, address: bytes, chain_id: int) -> int | None:
        """Resolve a Morpho vault's internal ID from its onchain address."""
        async with self._engine.connect() as conn:
            result = await conn.execute(text(_VAULT_ID_SQL), {"addr": address, "chain_id": chain_id})
            row = result.fetchone()
        return row.id if row is not None else None

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown:
        """Execute the Morpho vault backed breakdown query and return domain objects."""
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_MORPHO_BACKED_BREAKDOWN_SQL),
                {"backed_asset_id": backed_asset_id},
            )
            rows = result.fetchall()

        items = tuple(
            CollateralContribution(
                token_id=row.token_id,
                symbol=row.symbol,
                backing_value=Decimal(str(row.backed_amount)),
                backing_pct=Decimal(str(row.backing_pct)),
                price_usd=None,
            )
            for row in rows
        )

        return BackedBreakdown(
            backed_asset_id=backed_asset_id,
            items=items,
        )
