# ruff: noqa: E501
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.reference_as_of import (
    ORACLE_ASSET_AS_OF,
    ReferenceAsOf,
    ReferenceEffectiveAtProvider,
)
from app.domain.entities.backed_breakdown import (
    BackedBreakdown,
    CollateralContribution,
)

# Minimum collateral amount in loan-token units (not USD) to include in the breakdown.
# Filters out dust positions that would add noise to the percentage calculation.
_MIN_COLLATERAL_AMOUNT = Decimal("0.01")


_VAULT_ID_SQL = """
SELECT id FROM morpho_vault WHERE address = :addr AND chain_id = :chain_id
"""

MORPHO_VAULT_USERS_SQL = """
      SELECT v.id as vault_id, u.id as user_id
      FROM morpho_vault v
      JOIN "user" u ON u.address = v.address AND u.chain_id = v.chain_id
      WHERE v.id = :backed_asset_id AND v.vault_version IN (1, 2)
      UNION ALL
      SELECT v.id, u.id
      FROM morpho_vault v
      JOIN morpho_adapter_current a ON a.morpho_vault_id = v.id AND a.adapter_type = 1
      JOIN "user" u ON u.address = a.address AND u.chain_id = v.chain_id
      WHERE v.id = :backed_asset_id AND v.vault_version = 3
"""
"""The (vault_id, user_id) rows whose morpho_market_position entries are the vault's allocations.

A MetaMorpho V1/V1.1 vault (vault_version 1, 2) supplies to Morpho Blue markets itself.
A VaultV2 (vault_version 3) holds nothing directly: its current member adapters of type 1
(Morpho Blue market adapters) do. Every other adapter type (2 nested MetaMorpho V1 vault,
3-5 external ERC-4626 / Box / Compound V3, 99 unclassified) is not walked, so its value
shows up as idle loan token (total_assets minus the walked positions), not as the
collateral behind it.
"""

_MORPHO_BACKED_BREAKDOWN_SQL = f"""
WITH vault_users AS (
      {MORPHO_VAULT_USERS_SQL}
  ),
  -- vault_states, market_allocs and market_states read the trigger-maintained
  -- *_current caches (newest row per key: 20260910_140000 for the vault and market
  -- state, 20260909_150000 for positions) instead of reducing each history per
  -- request, so this query plans over no hypertable chunk at all.
  vault_states AS (
      SELECT vsc.morpho_vault_id as vault_id,
             vsc.total_assets / power(10, t.decimals) as total_assets,
             t.id as loan_token_id,
             t.symbol as loan_token
      FROM morpho_vault_state_current vsc
      JOIN morpho_vault v ON v.id = vsc.morpho_vault_id AND v.id = :backed_asset_id
      JOIN token t ON t.id = v.asset_token_id
  ),
  -- One row per (vault, market): the newest position of every walked user, summed,
  -- since several VaultV2 adapters may supply the same market. The position cache
  -- holds exactly one row per (user, market), so a user's rows ARE the set of
  -- markets it has ever held — a fully exited market is still present, at zero.
  market_allocs AS (
      SELECT vu.vault_id,
             mpc.morpho_market_id,
             ct.id as collateral_token_id,
             ct.symbol as collateral,
             sum(mpc.supply_assets) / power(10, lt.decimals) as vault_supply
      FROM vault_users vu
      JOIN morpho_market_position_current mpc ON mpc.user_id = vu.user_id
      JOIN morpho_market mm ON mm.id = mpc.morpho_market_id
      JOIN token ct ON ct.id = mm.collateral_token_id
      JOIN token lt ON lt.id = mm.loan_token_id
      GROUP BY vu.vault_id, mpc.morpho_market_id, ct.id, ct.symbol, lt.decimals
  ),
  market_states AS (
      SELECT msc.morpho_market_id,
             CASE WHEN msc.total_supply_assets > 0
                 THEN msc.total_borrow_assets::numeric / msc.total_supply_assets::numeric
                 ELSE 0 END as utilization
      FROM morpho_market_state_current msc
      WHERE msc.morpho_market_id IN (SELECT morpho_market_id FROM market_allocs)
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
  ),
  -- Latest USD price per token from the vault's Morpho Blue protocol_oracle
  -- binding, mirroring the Aave repo's token_prices CTE (same enabled-oracle_asset
  -- gate + snapshot order, same token_price_current source so the ranking runs over
  -- one row per (oracle, token) rather than the onchain_token_price hypertable).
  -- Each row exposes its OWN token's price so amount/price stay denominated in the
  -- row's symbol, as Aave does.
  token_prices AS (
      SELECT DISTINCT ON (tpc.token_id)
          tpc.token_id,
          tpc.price_usd
      FROM token_price_current tpc
      JOIN protocol_oracle po ON po.oracle_id = tpc.oracle_id
      JOIN morpho_vault v ON v.id = :backed_asset_id AND po.protocol_id = v.protocol_id
      WHERE EXISTS (
          SELECT 1 FROM {ORACLE_ASSET_AS_OF} oa
          WHERE oa.oracle_id = tpc.oracle_id AND oa.token_id = tpc.token_id AND oa.enabled
      )
      ORDER BY tpc.token_id, tpc.block_number DESC, tpc.block_version DESC, tpc.processing_version DESC, tpc.oracle_id DESC
  ),
  -- The vault's loan token converts every (loan-token-denominated) backing amount
  -- to USD, so it is pulled out separately as the scaling factor for backed_amount.
  loan_token_price AS (
      SELECT tp.price_usd
      FROM token_prices tp
      JOIN morpho_vault v ON v.id = :backed_asset_id AND tp.token_id = v.asset_token_id
  )
  SELECT a.token_id,
         a.symbol,
         round(sum(a.amount)::numeric, 2) as backed_amount,
         round((sum(a.amount) / NULLIF(t.total_amount, 0) * 100)::numeric, 2) as backing_pct,
         ltp.price_usd as loan_token_price,
         tp.price_usd as token_price_usd
  FROM all_backing a
  CROSS JOIN total t
  LEFT JOIN loan_token_price ltp ON true
  LEFT JOIN token_prices tp ON tp.token_id = a.token_id
  GROUP BY a.token_id, a.symbol, t.total_amount, ltp.price_usd, tp.price_usd
  HAVING sum(a.amount) > {_MIN_COLLATERAL_AMOUNT}
  ORDER BY backed_amount DESC
"""


class MorphoBackedBreakdownRepository:
    """Postgres implementation of the backed breakdown repository for Morpho vaults."""

    def __init__(self, engine: AsyncEngine, reference_effective_at: ReferenceEffectiveAtProvider) -> None:
        self._engine = engine
        self._reference = ReferenceAsOf(reference_effective_at)

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
                self._reference.params(backed_asset_id=backed_asset_id),
            )
            rows = result.fetchall()

        items = [self._to_contribution(row) for row in rows]
        return BackedBreakdown(backed_asset_id=backed_asset_id, items=tuple(items))

    @staticmethod
    def _to_contribution(row: Any) -> CollateralContribution:
        backed_amount = Decimal(str(row.backed_amount))
        loan_token_price = Decimal(str(row.loan_token_price)) if row.loan_token_price is not None else None
        if loan_token_price is None:
            # The vault's loan token has no USD price, so no backing amount can be
            # converted to USD: the whole vault is unpriced. Keep raw loan-token units
            # and force price_usd None on every row. The risk service treats an
            # all-unpriced breakdown as price_data_missing and never reads the raw
            # value as USD.
            return CollateralContribution(
                token_id=row.token_id,
                symbol=row.symbol,
                backing_value=backed_amount,
                backing_pct=Decimal(str(row.backing_pct)),
                price_usd=None,
            )
        # backed_amount is in loan-token units; scale by the loan-token price so
        # backing_value is USD (what enrichment reads as amount_usd), correct even when
        # the loan token is not ~$1. price_usd is each row token's own price so amount
        # and price stay denominated in the row's symbol (as Aave does); it is None for
        # a collateral token the oracle does not price, and that row drops at enrichment.
        token_price_usd = Decimal(str(row.token_price_usd)) if row.token_price_usd is not None else None
        return CollateralContribution(
            token_id=row.token_id,
            symbol=row.symbol,
            backing_value=backed_amount * loan_token_price,
            backing_pct=Decimal(str(row.backing_pct)),
            price_usd=token_price_usd,
        )
