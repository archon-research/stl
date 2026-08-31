"""Postgres implementation of MorphoVaultAllocationsReader.

Reads the same tables the gap_sweep Morpho breakdown reads (morpho_vault,
morpho_vault_state, morpho_market_position), but keeps the per-market supply
amounts whole instead of splitting them by utilization — CORE results are per
market, so the serving weights must be the vault's raw market allocations.
"""

from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.ports.morpho_vault_allocations import (
    MorphoVaultAllocations,
    MorphoVaultMarketAllocation,
)

# The vault, its loan token, and its latest total_assets (NULL with no state
# row yet — a vault the indexer created but has not snapshotted).
_VAULT_SQL = text("""
    SELECT v.id AS vault_id,
           t.symbol AS loan_symbol,
           t.decimals AS loan_decimals,
           vs.total_assets
    FROM morpho_vault v
    JOIN token t ON t.id = v.asset_token_id
    LEFT JOIN LATERAL (
        SELECT total_assets
        FROM morpho_vault_state
        WHERE morpho_vault_id = v.id
        ORDER BY block_number DESC, block_version DESC, processing_version DESC
        LIMIT 1
    ) vs ON true
    WHERE v.address = :addr AND v.chain_id = :chain_id
""")

# A market the vault fully exited keeps a latest row with supply_assets = 0;
# it carries no weight, so it is dropped after the latest-row selection.
_ALLOCATIONS_SQL = text("""
    WITH vault_user AS (
        SELECT u.id AS user_id
        FROM morpho_vault v
        JOIN "user" u ON u.address = v.address AND u.chain_id = v.chain_id
        WHERE v.id = :vault_id
    ),
    latest_positions AS (
        SELECT DISTINCT ON (mp.morpho_market_id)
               mp.morpho_market_id, mp.supply_assets
        FROM morpho_market_position mp
        JOIN vault_user vu ON vu.user_id = mp.user_id
        ORDER BY mp.morpho_market_id,
                 mp.block_number DESC, mp.block_version DESC, mp.processing_version DESC
    )
    SELECT lp.morpho_market_id,
           ct.symbol AS collateral_symbol,
           lt.symbol AS loan_symbol,
           lt.decimals AS loan_decimals,
           lp.supply_assets
    FROM latest_positions lp
    JOIN morpho_market mm ON mm.id = lp.morpho_market_id
    JOIN token ct ON ct.id = mm.collateral_token_id
    JOIN token lt ON lt.id = mm.loan_token_id
    WHERE lp.supply_assets > 0
    ORDER BY lp.supply_assets DESC
""")


class MorphoVaultAllocationsRepository:
    """Postgres reader for a Morpho vault's per-market allocations."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_vault_allocations(self, receipt_token_address: bytes, chain_id: int) -> MorphoVaultAllocations | None:
        async with self._engine.connect() as conn:
            vault_row = (
                await conn.execute(_VAULT_SQL, {"addr": receipt_token_address, "chain_id": chain_id})
            ).fetchone()
            if vault_row is None:
                return None
            rows = (await conn.execute(_ALLOCATIONS_SQL, {"vault_id": vault_row.vault_id})).fetchall()

        total_assets = (
            Decimal(str(vault_row.total_assets)) / (Decimal(10) ** int(vault_row.loan_decimals))
            if vault_row.total_assets is not None
            else Decimal("0")
        )
        allocations = tuple(
            MorphoVaultMarketAllocation(
                morpho_market_id=row.morpho_market_id,
                collateral_symbol=row.collateral_symbol,
                loan_symbol=row.loan_symbol,
                supply_assets=Decimal(str(row.supply_assets)) / (Decimal(10) ** int(row.loan_decimals)),
            )
            for row in rows
        )
        return MorphoVaultAllocations(
            vault_id=vault_row.vault_id,
            loan_token_symbol=vault_row.loan_symbol,
            total_assets=total_assets,
            allocations=allocations,
        )
