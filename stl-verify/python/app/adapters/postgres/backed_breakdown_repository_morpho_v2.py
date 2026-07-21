# ruff: noqa: E501
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._morpho_breakdown_common import MorphoVaultRef, resolve_morpho_vault, to_contribution
from app.domain.entities.backed_breakdown import BackedBreakdown

# Minimum backing amount in loan-token (vault-asset) units to include; filters dust
# that would only add noise to the percentage split. Mirrors the V1 repository.
_MIN_COLLATERAL_AMOUNT = Decimal("0.01")

# ---------------------------------------------------------------------------
# Morpho VaultV2 backed-breakdown walk (VEC-219).
#
# A VaultV2 never allocates to Morpho Blue directly. Its assets live in a set of
# liquidity adapters (morpho_adapter) plus an idle remainder held by the vault
# itself. Each active adapter (removed_at_block IS NULL) reports its current value
# via realAssets() (latest morpho_adapter_state.real_assets). We resolve each
# adapter's downstream composition by its type:
#
#   type 1 (MorphoMarketV1AdapterV2): the adapter address is the "user" on
#       Morpho Blue markets, so its market positions split by market utilization
#       exactly like the V1 walk (collateral = supply·util, idle-loan =
#       supply·(1-util)). We take those position-derived amounts DIRECTLY rather
#       than normalising them to real_assets, so V1 and V2 numbers are computed by
#       identical math and stay comparable; real_assets is used only as the
#       vault-idle anchor below.
#   type 2 (MorphoVaultV1Adapter): the adapter address is the "user" holding
#       shares in a nested MetaMorpho V1 vault (morpho_vault_position). We recurse
#       ONE level using the V1 walk over that V1 vault's OWN market allocations
#       (its user = the V1 vault address) and scale every line by the adapter's
#       share of the V1 vault (adapter_assets / v1_total_assets). MetaMorpho V1/V1.1
#       allocate only to Morpho Blue markets (never to adapters), so those markets
#       bottom out the recursion; VaultV2-in-VaultV2 nesting does not exist on-chain
#       today, so no deeper level is walked.
#   type 99 (unknown): surfaced as a single line of its real_assets in the vault's
#       own asset token — recorded, never silently dropped (no-silent-holes rule).
#
# Vault idle = latest V2 total_assets − Σ active-adapter real_assets, in the vault
# asset token (mirrors the V1 vault_idle remainder).
#
# Every amount is denominated in the vault's underlying asset: type-1 markets and
# every nested V1 market have that asset as their loan token, so utilisation splits
# stay in loan-token units and only need the vault asset's decimals to scale to
# human units. The collateral token_id merely labels the row (as in V1). Rows are
# priced by the same token_prices + loan_token_price CTEs the V1 repository uses, so
# backing_value is USD; see the shared ``to_contribution`` for the mapping.
# ---------------------------------------------------------------------------
_MORPHO_V2_BACKED_BREAKDOWN_SQL = f"""
WITH v2 AS (
      SELECT mv.id AS vault_id,
             mv.chain_id,
             mv.asset_token_id AS asset_token_id,
             at.symbol AS asset_symbol,
             at.decimals AS asset_decimals
      FROM morpho_vault mv
      JOIN token at ON at.id = mv.asset_token_id
      WHERE mv.id = :backed_asset_id
  ),
  -- Latest V2 vault total_assets (idle anchor's minuend).
  v2_state AS (
      SELECT vs.total_assets
      FROM morpho_vault_state vs
      WHERE vs.morpho_vault_id = (SELECT vault_id FROM v2)
      ORDER BY vs.block_number DESC, vs.block_version DESC, vs.processing_version DESC
      LIMIT 1
  ),
  -- Active adapters + latest realAssets() + the adapter address's user id. The whole
  -- walk keys on ADAPTER users: a VaultV2 never supplies to Morpho Blue under its own
  -- address, so any market_position attributed to the vault address itself (e.g. vault
  -- 760 wethpv's single dust row) is a misattributed artifact and is deliberately
  -- ignored — never unioned in.
  adapters AS (
      SELECT ma.id AS adapter_id,
             ma.adapter_type,
             u.id AS adapter_user_id,
             st.real_assets
      FROM morpho_adapter ma
      CROSS JOIN v2
      LEFT JOIN "user" u ON u.address = ma.address AND u.chain_id = v2.chain_id
      LEFT JOIN LATERAL (
          SELECT mas.real_assets
          FROM morpho_adapter_state mas
          WHERE mas.morpho_adapter_id = ma.id
          ORDER BY mas.block_number DESC, mas.block_version DESC, mas.processing_version DESC
          LIMIT 1
      ) st ON true
      WHERE ma.morpho_vault_id = v2.vault_id
        AND ma.removed_at_block IS NULL
  ),
  -- Σ real_assets over active adapters — the idle anchor's subtrahend.
  adapter_real_total AS (
      SELECT COALESCE(SUM(real_assets), 0) AS total_real_assets FROM adapters
  ),
  -- type 1: adapter's Morpho Blue positions split by market utilization.
  type1 AS (
      SELECT mm.collateral_token_id AS token_id,
             ct.symbol AS symbol,
             (pos.supply_assets / power(10, v2.asset_decimals)) * ms.utilization AS collateral_amount,
             (pos.supply_assets / power(10, v2.asset_decimals)) * (1 - ms.utilization) AS idle_loan_amount
      FROM adapters a
      CROSS JOIN v2
      JOIN LATERAL (
          SELECT DISTINCT morpho_market_id
          FROM morpho_market_position
          WHERE user_id = a.adapter_user_id
      ) mids ON true
      JOIN morpho_market mm ON mm.id = mids.morpho_market_id
      JOIN token ct ON ct.id = mm.collateral_token_id
      JOIN LATERAL (
          SELECT mp.supply_assets
          FROM morpho_market_position mp
          WHERE mp.user_id = a.adapter_user_id AND mp.morpho_market_id = mm.id
          ORDER BY mp.block_number DESC, mp.block_version DESC, mp.processing_version DESC
          LIMIT 1
      ) pos ON true
      JOIN LATERAL (
          SELECT CASE WHEN mst.total_supply_assets > 0
                      THEN mst.total_borrow_assets::numeric / mst.total_supply_assets::numeric
                      ELSE 0 END AS utilization
          FROM morpho_market_state mst
          WHERE mst.morpho_market_id = mm.id
          ORDER BY mst.block_number DESC, mst.block_version DESC, mst.processing_version DESC
          LIMIT 1
      ) ms ON true
      WHERE a.adapter_type = 1
  ),
  -- type 2: adapter's holdings in a nested MetaMorpho V1 vault + that V1 vault's
  -- own user and latest total_assets (the scaling denominator).
  type2_holdings AS (
      SELECT a.adapter_id,
             v1.id AS v1_vault_id,
             v1u.id AS v1_vault_user_id,
             vp.assets AS adapter_assets,
             v1s.total_assets AS v1_total_assets
      FROM adapters a
      JOIN LATERAL (
          SELECT DISTINCT morpho_vault_id
          FROM morpho_vault_position
          WHERE user_id = a.adapter_user_id
      ) vids ON true
      JOIN LATERAL (
          SELECT mvp.assets
          FROM morpho_vault_position mvp
          WHERE mvp.user_id = a.adapter_user_id AND mvp.morpho_vault_id = vids.morpho_vault_id
          ORDER BY mvp.block_number DESC, mvp.block_version DESC, mvp.processing_version DESC
          LIMIT 1
      ) vp ON true
      JOIN morpho_vault v1 ON v1.id = vids.morpho_vault_id
      JOIN "user" v1u ON v1u.address = v1.address AND v1u.chain_id = v1.chain_id
      JOIN LATERAL (
          SELECT mvs.total_assets
          FROM morpho_vault_state mvs
          WHERE mvs.morpho_vault_id = v1.id
          ORDER BY mvs.block_number DESC, mvs.block_version DESC, mvs.processing_version DESC
          LIMIT 1
      ) v1s ON true
      WHERE a.adapter_type = 2
  ),
  -- The nested V1 vault's own Morpho Blue allocations, split by utilization and
  -- scaled to the adapter's share of the V1 vault (adapter_assets / v1_total_assets).
  type2_markets AS (
      SELECT mm.collateral_token_id AS token_id,
             ct.symbol AS symbol,
             (mp.supply_assets / power(10, v2.asset_decimals)) * ms.utilization
                 * (h.adapter_assets / NULLIF(h.v1_total_assets, 0)) AS collateral_amount,
             (mp.supply_assets / power(10, v2.asset_decimals)) * (1 - ms.utilization)
                 * (h.adapter_assets / NULLIF(h.v1_total_assets, 0)) AS idle_loan_amount
      FROM type2_holdings h
      CROSS JOIN v2
      JOIN LATERAL (
          SELECT DISTINCT morpho_market_id
          FROM morpho_market_position
          WHERE user_id = h.v1_vault_user_id
      ) mids ON true
      JOIN morpho_market mm ON mm.id = mids.morpho_market_id
      JOIN token ct ON ct.id = mm.collateral_token_id
      JOIN LATERAL (
          SELECT mp2.supply_assets
          FROM morpho_market_position mp2
          WHERE mp2.user_id = h.v1_vault_user_id AND mp2.morpho_market_id = mm.id
          ORDER BY mp2.block_number DESC, mp2.block_version DESC, mp2.processing_version DESC
          LIMIT 1
      ) mp ON true
      JOIN LATERAL (
          SELECT CASE WHEN mst.total_supply_assets > 0
                      THEN mst.total_borrow_assets::numeric / mst.total_supply_assets::numeric
                      ELSE 0 END AS utilization
          FROM morpho_market_state mst
          WHERE mst.morpho_market_id = mm.id
          ORDER BY mst.block_number DESC, mst.block_version DESC, mst.processing_version DESC
          LIMIT 1
      ) ms ON true
  ),
  -- The nested V1 vault's idle remainder (total − Σ supplied), scaled to the adapter.
  type2_supplied AS (
      SELECT h.adapter_id, h.adapter_assets, h.v1_total_assets,
             COALESCE(SUM(mp.supply_assets), 0) AS total_supplied
      FROM type2_holdings h
      LEFT JOIN LATERAL (
          SELECT DISTINCT morpho_market_id
          FROM morpho_market_position
          WHERE user_id = h.v1_vault_user_id
      ) mids ON true
      LEFT JOIN LATERAL (
          SELECT mp3.supply_assets
          FROM morpho_market_position mp3
          WHERE mp3.user_id = h.v1_vault_user_id AND mp3.morpho_market_id = mids.morpho_market_id
          ORDER BY mp3.block_number DESC, mp3.block_version DESC, mp3.processing_version DESC
          LIMIT 1
      ) mp ON true
      GROUP BY h.adapter_id, h.adapter_assets, h.v1_total_assets
  ),
  type2_idle AS (
      SELECT v2.asset_token_id AS token_id,
             v2.asset_symbol AS symbol,
             ((s.v1_total_assets - s.total_supplied) * (s.adapter_assets / NULLIF(s.v1_total_assets, 0)))
                 / power(10, v2.asset_decimals) AS idle_amount
      FROM type2_supplied s
      CROSS JOIN v2
  ),
  -- type 99: unknown adapter, surfaced as its real_assets in the vault asset.
  type99 AS (
      SELECT v2.asset_token_id AS token_id,
             v2.asset_symbol AS symbol,
             COALESCE(a.real_assets, 0) / power(10, v2.asset_decimals) AS amount
      FROM adapters a
      CROSS JOIN v2
      WHERE a.adapter_type = 99
  ),
  -- V2 vault idle remainder (total_assets − Σ adapter real_assets), in the vault asset.
  v2_idle AS (
      SELECT v2.asset_token_id AS token_id,
             v2.asset_symbol AS symbol,
             ((SELECT total_assets FROM v2_state) - (SELECT total_real_assets FROM adapter_real_total))
                 / power(10, v2.asset_decimals) AS idle_amount
      FROM v2
  ),
  all_backing AS (
      SELECT token_id, symbol, collateral_amount AS amount FROM type1
      WHERE collateral_amount > {_MIN_COLLATERAL_AMOUNT}
      UNION ALL
      SELECT (SELECT asset_token_id FROM v2), (SELECT asset_symbol FROM v2), COALESCE(SUM(idle_loan_amount), 0) FROM type1
      UNION ALL
      SELECT token_id, symbol, collateral_amount FROM type2_markets
      WHERE collateral_amount > {_MIN_COLLATERAL_AMOUNT}
      UNION ALL
      SELECT (SELECT asset_token_id FROM v2), (SELECT asset_symbol FROM v2), COALESCE(SUM(idle_loan_amount), 0) FROM type2_markets
      UNION ALL
      SELECT token_id, symbol, idle_amount FROM type2_idle
      UNION ALL
      SELECT token_id, symbol, amount FROM type99
      UNION ALL
      SELECT token_id, symbol, idle_amount FROM v2_idle
  ),
  total AS (
      SELECT SUM(amount) AS total_amount FROM all_backing
  ),
  -- Latest USD price per token from the vault's Morpho Blue protocol_oracle binding,
  -- identical to the V1 repo's token_prices CTE (same enabled-oracle_asset gate +
  -- snapshot order). Each row exposes its OWN token's price so amount/price stay
  -- denominated in the row's symbol. VaultV2 shares its parent protocol's oracle
  -- bindings, so every collateral token (direct + nested-V1) is priced the same way.
  token_prices AS (
      SELECT DISTINCT ON (otp.token_id)
          otp.token_id,
          otp.price_usd
      FROM onchain_token_price otp
      JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
      JOIN morpho_vault v ON v.id = :backed_asset_id AND po.protocol_id = v.protocol_id
      WHERE EXISTS (
          SELECT 1 FROM oracle_asset oa
          WHERE oa.oracle_id = otp.oracle_id AND oa.token_id = otp.token_id AND oa.enabled
      )
      ORDER BY otp.token_id, otp.block_number DESC, otp.block_version DESC, otp.processing_version DESC, otp.oracle_id DESC
  ),
  -- The vault's loan token converts every (loan-token-denominated) backing amount to
  -- USD, so it is pulled out separately as the scaling factor for backed_amount.
  loan_token_price AS (
      SELECT tp.price_usd
      FROM token_prices tp
      JOIN morpho_vault v ON v.id = :backed_asset_id AND tp.token_id = v.asset_token_id
  )
  SELECT a.token_id,
         a.symbol,
         round(sum(a.amount)::numeric, 2) AS backed_amount,
         round((sum(a.amount) / NULLIF(t.total_amount, 0) * 100)::numeric, 2) AS backing_pct,
         ltp.price_usd AS loan_token_price,
         tp.price_usd AS token_price_usd
  FROM all_backing a
  CROSS JOIN total t
  LEFT JOIN loan_token_price ltp ON true
  LEFT JOIN token_prices tp ON tp.token_id = a.token_id
  WHERE a.token_id IS NOT NULL
  GROUP BY a.token_id, a.symbol, t.total_amount, ltp.price_usd, tp.price_usd
  HAVING sum(a.amount) > {_MIN_COLLATERAL_AMOUNT}
  ORDER BY backed_amount DESC
"""


class MorphoV2BackedBreakdownRepository:
    """Postgres implementation of the backed-breakdown repository for Morpho VaultV2 (adapter-based)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def resolve_vault(self, address: bytes, chain_id: int) -> MorphoVaultRef | None:
        """Resolve a Morpho vault's internal id and ``vault_version`` from its onchain address."""
        return await resolve_morpho_vault(self._engine, address, chain_id)

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown:
        """Execute the VaultV2 backed-breakdown walk and return domain objects.

        Rows map to ``CollateralContribution`` via the shared ``to_contribution`` (same
        USD-scaling as the V1 repo): backing_value = loan-token amount × loan-token
        price, price_usd per row, all-None when the loan token is unpriced.
        """
        async with self._engine.connect() as connection:
            result = await connection.execute(
                text(_MORPHO_V2_BACKED_BREAKDOWN_SQL),
                {"backed_asset_id": backed_asset_id},
            )
            rows = result.fetchall()

        return BackedBreakdown(backed_asset_id=backed_asset_id, items=tuple(to_contribution(row) for row in rows))
