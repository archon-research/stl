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

_HAS_ACTIVE_ADAPTERS_SQL = """
SELECT EXISTS (
    SELECT 1 FROM morpho_adapter
    WHERE morpho_vault_id = :backed_asset_id AND removed_at_block IS NULL
) AS has_adapters
"""

# Collateral tokens' MIN(lltv) reachable through the vault's ACTIVE adapters. A
# VaultV2 never supplies to Morpho Blue under its own address, so the walk keys
# strictly on adapter users and deliberately ignores any morpho_market_position
# attributed to the vault address itself (e.g. vault 760 wethpv's single dust row
# is a misattributed artifact, not a real allocation — VEC-219 investigation).
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

    ``has_active_adapters`` lets the reader distinguish a v3 vault whose adapters
    are not indexed yet (degrade to ``adapter_data_missing``) from a genuinely idle
    vault with adapters but no collateral markets (a real ``rrc=0``).
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def has_active_adapters(self, backed_asset_id: int) -> bool:
        """Return whether the VaultV2 has at least one active (non-removed) adapter row."""
        async with self._engine.connect() as conn:
            result = await conn.execute(text(_HAS_ACTIVE_ADAPTERS_SQL), {"backed_asset_id": backed_asset_id})
            row = result.fetchone()
        return bool(row.has_adapters) if row is not None else False

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
