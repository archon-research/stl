# ruff: noqa: E501
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.risk import LiquidationParams
from app.risk_engine.crypto_lending.lif import compute_lif

# lltv is stored WAD (18 decimals, e.g. 860000000000000000 = 86%): the Go indexer
# persists the raw big.Int, so divide by 1e18 to normalise into [0, 1] before
# compute_lif(). MIN(lltv) is the conservative threshold when a collateral token
# appears in several of the vault's markets.
_WAD = Decimal("1000000000000000000")

# Dust floor (in vault-asset units) below which an adapter's realAssets() is treated
# as immaterial. Mirrors the breakdown repos' _MIN_COLLATERAL_AMOUNT so a genuinely
# idle adapter (value ≈ 0) is never flagged as an unwalkable data gap.
_MIN_ADAPTER_VALUE = Decimal("0.01")

_HAS_ACTIVE_ADAPTERS_SQL = """
SELECT EXISTS (
    SELECT 1 FROM morpho_adapter
    WHERE morpho_vault_id = :backed_asset_id AND removed_at_block IS NULL
) AS has_adapters
"""

# TRUE iff some ACTIVE adapter's value cannot be walked into collateral lines:
# either it has no morpho_adapter_state row at all (its value is unknown, so the
# idle computation mis-attributes it), or its latest realAssets() is material
# (> dust) yet its type-specific walk resolves zero markets (positions/user/nested
# path not indexed; type-99/unknown resolves nothing by construction). A missing
# "user" row counts as zero markets. This is the partial-index signal that adapter
# ROW existence misses.
_HAS_UNWALKABLE_ADAPTER_VALUE_SQL = f"""
WITH v AS (
    SELECT mv.id AS vault_id, mv.chain_id, t.decimals AS asset_decimals
    FROM morpho_vault mv
    JOIN token t ON t.id = mv.asset_token_id
    WHERE mv.id = :backed_asset_id
),
active_adapters AS (
    SELECT ma.id AS adapter_id, ma.adapter_type, u.id AS adapter_user_id
    FROM morpho_adapter ma
    CROSS JOIN v
    LEFT JOIN "user" u ON u.address = ma.address AND u.chain_id = v.chain_id
    WHERE ma.morpho_vault_id = v.vault_id AND ma.removed_at_block IS NULL
)
SELECT EXISTS (
    SELECT 1
    FROM active_adapters aa
    CROSS JOIN v
    LEFT JOIN LATERAL (
        SELECT mas.real_assets
        FROM morpho_adapter_state mas
        WHERE mas.morpho_adapter_id = aa.adapter_id
        ORDER BY mas.block_number DESC, mas.block_version DESC, mas.processing_version DESC
        LIMIT 1
    ) st ON true
    WHERE st.real_assets IS NULL
       OR (
           (st.real_assets / power(10, v.asset_decimals)) > {_MIN_ADAPTER_VALUE}
           AND CASE aa.adapter_type
               WHEN 1 THEN NOT EXISTS (
                   SELECT 1 FROM morpho_market_position mmp WHERE mmp.user_id = aa.adapter_user_id
               )
               WHEN 2 THEN NOT EXISTS (
                   SELECT 1
                   FROM morpho_vault_position mvp
                   JOIN morpho_vault v1 ON v1.id = mvp.morpho_vault_id
                   JOIN "user" v1u ON v1u.address = v1.address AND v1u.chain_id = v.chain_id
                   JOIN morpho_market_position mmp ON mmp.user_id = v1u.id
                   WHERE mvp.user_id = aa.adapter_user_id
               )
               ELSE true
           END
       )
) AS unwalkable
"""

# Collateral tokens' MIN(lltv) reachable through the vault's ACTIVE adapters. A
# VaultV2 never supplies to Morpho Blue under its own address, so the walk keys
# strictly on adapter users and deliberately ignores any morpho_market_position
# attributed to the vault address itself (e.g. vault 760 wethpv's single dust row
# is a misattributed artifact, not a real allocation).
_SQL = """
WITH v AS (
    SELECT id AS vault_id, chain_id FROM morpho_vault WHERE id = :backed_asset_id
),
adapters AS (
    SELECT ma.adapter_type, u.id AS adapter_user_id
    FROM morpho_adapter ma
    CROSS JOIN v
    LEFT JOIN "user" u ON u.address = ma.address AND u.chain_id = v.chain_id
    WHERE ma.morpho_vault_id = v.vault_id AND ma.removed_at_block IS NULL
),
-- type 1 (MorphoMarketV1AdapterV2): the adapter address is the Blue-market user directly.
type1_markets AS (
    SELECT DISTINCT mmp.morpho_market_id
    FROM adapters a
    JOIN morpho_market_position mmp ON mmp.user_id = a.adapter_user_id
    WHERE a.adapter_type = 1
),
-- type 2 (MorphoVaultV1Adapter): recurse ONE level through the nested MetaMorpho V1
-- vault (its own user is the V1 vault address). V1/V1.1 allocate only to Blue markets,
-- so these bottom out the recursion; VaultV2-in-VaultV2 nesting does not exist on-chain.
type2_markets AS (
    SELECT DISTINCT mmp.morpho_market_id
    FROM adapters a
    JOIN morpho_vault_position mvp ON mvp.user_id = a.adapter_user_id
    JOIN morpho_vault v1 ON v1.id = mvp.morpho_vault_id
    JOIN "user" v1u ON v1u.address = v1.address AND v1u.chain_id = v1.chain_id
    JOIN morpho_market_position mmp ON mmp.user_id = v1u.id
    WHERE a.adapter_type = 2
),
market_ids AS (
    SELECT morpho_market_id FROM type1_markets
    UNION
    SELECT morpho_market_id FROM type2_markets
)
SELECT mm.collateral_token_id AS token_id, MIN(mm.lltv) AS lltv
FROM morpho_market mm
WHERE mm.id IN (SELECT morpho_market_id FROM market_ids)
  AND mm.collateral_token_id = ANY(:token_ids)
GROUP BY mm.collateral_token_id
"""


class MorphoV2LiquidationParamsRepository:
    """Liquidation-params adapter for Morpho VaultV2 (adapter-based).

    Mirrors ``MorphoLiquidationParamsRepository``'s surface, but resolves the
    vault's collateral markets through its active adapter graph rather than the
    vault address's own positions:

      liquidation_threshold = lltv (WAD-normalised, MIN per collateral token)
      liquidation_bonus     = LIF computed deterministically from lltv

    Two composition probes let the reader distinguish a deployed-but-unindexed v3
    vault (degrade to ``adapter_data_missing``) from a genuinely idle one (a real
    ``rrc=0``): ``has_active_adapters`` (any active adapter at all) and
    ``has_unwalkable_adapter_value`` (an active adapter holding material value whose
    walk resolves zero markets, or with no state row). Adapter-ROW existence alone is
    insufficient because morpho_adapter and morpho_adapter_state are written by
    different code paths and lag one another during backfill.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def has_active_adapters(self, backed_asset_id: int) -> bool:
        """Return whether the VaultV2 has at least one active (non-removed) adapter row."""
        async with self._engine.connect() as conn:
            result = await conn.execute(text(_HAS_ACTIVE_ADAPTERS_SQL), {"backed_asset_id": backed_asset_id})
            row = result.fetchone()
        return bool(row.has_adapters) if row is not None else False

    async def has_unwalkable_adapter_value(self, backed_asset_id: int) -> bool:
        """Return whether any active adapter holds value the walk cannot resolve.

        See ``_HAS_UNWALKABLE_ADAPTER_VALUE_SQL``: a stateless adapter, or a material
        adapter whose type-specific walk resolves zero markets. Used to distinguish a
        deployed-but-unindexed VaultV2 (degrade to ``adapter_data_missing``) from a
        genuinely idle one (a real ``rrc=0``).
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(text(_HAS_UNWALKABLE_ADAPTER_VALUE_SQL), {"backed_asset_id": backed_asset_id})
            row = result.fetchone()
        return bool(row.unwalkable) if row is not None else False

    async def get_params(self, backed_asset_id: int, token_ids: list[int]) -> dict[int, LiquidationParams]:
        if not token_ids:
            return {}

        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(_SQL),
                {"backed_asset_id": backed_asset_id, "token_ids": token_ids},
            )
            rows = result.fetchall()

        params: dict[int, LiquidationParams] = {}
        for row in rows:
            lltv = Decimal(str(row.lltv)) / _WAD  # WAD → [0, 1]
            params[row.token_id] = LiquidationParams(
                token_id=row.token_id,
                liquidation_threshold=lltv,
                liquidation_bonus=compute_lif(lltv),
            )
        return params
