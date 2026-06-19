import logging
from typing import Any

from sqlalchemy import TextClause, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.psm3 import PriceQuote, Psm3Snapshot

logger = logging.getLogger(__name__)

_SNAPSHOT_COLUMNS = """
    chain_id,
    encode(address, 'hex') AS address_hex,
    usds_balance,
    susds_balance,
    usdc_balance,
    total_assets,
    conversion_rate,
    block_number,
    block_version,
    block_timestamp
"""

# Reorg re-emissions (block_version) and reprocessing (processing_version) must win
# deterministically, so order by versions — never by block_timestamp.
_VERSION_ORDER = "block_number DESC, block_version DESC, processing_version DESC"

_HISTORY_SQL = text(
    f"""
    SELECT {_SNAPSHOT_COLUMNS}
    FROM psm3_reserves
    WHERE chain_id = :chain_id
    ORDER BY {_VERSION_ORDER}
    LIMIT :limit
    """
)

_LATEST_PRICE_SQL = text(
    """
    SELECT otp.price_usd, otp.timestamp
    FROM offchain_token_price otp
    JOIN offchain_price_asset opa
        ON opa.token_id = otp.token_id AND opa.source_id = otp.source_id
    JOIN offchain_price_source ops ON ops.id = opa.source_id
    WHERE ops.name = 'coingecko' AND opa.source_asset_id = :source_asset_id
    ORDER BY otp.timestamp DESC, otp.processing_version DESC
    LIMIT 1
    """
)


class PostgresPsm3Repository:
    """PostgreSQL adapter for PSM3 reserve snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def list_latest_snapshots(self, chain_id: int | None = None) -> list[Psm3Snapshot]:
        where = "WHERE chain_id = :chain_id" if chain_id is not None else ""
        query = text(
            f"""
            SELECT DISTINCT ON (chain_id) {_SNAPSHOT_COLUMNS}
            FROM psm3_reserves
            {where}
            ORDER BY chain_id, {_VERSION_ORDER}
            """
        )
        params: dict[str, Any] = {} if chain_id is None else {"chain_id": chain_id}
        rows = await self._fetch_all(query, params, "fetching latest PSM3 snapshots")
        return [self._to_snapshot(row) for row in rows]

    async def list_snapshot_history(self, chain_id: int, *, limit: int = 100) -> list[Psm3Snapshot]:
        params = {"chain_id": chain_id, "limit": limit}
        rows = await self._fetch_all(_HISTORY_SQL, params, "fetching PSM3 snapshot history")
        return [self._to_snapshot(row) for row in rows]

    async def get_latest_price(self, source_asset_id: str) -> PriceQuote | None:
        params = {"source_asset_id": source_asset_id}
        rows = await self._fetch_all(_LATEST_PRICE_SQL, params, f"fetching latest {source_asset_id} price")
        if not rows:
            return None
        return PriceQuote(price_usd=rows[0].price_usd, timestamp=rows[0].timestamp)

    async def _fetch_all(self, query: TextClause, params: dict[str, Any], action: str) -> list[Any]:
        try:
            async with self._engine.connect() as conn:
                return list((await conn.execute(query, params)).fetchall())
        except Exception as exc:
            logger.error(
                "PSM3 repository query failed",
                extra={"action": action, "error_type": type(exc).__name__, "error_message": str(exc)},
                exc_info=True,
            )
            raise ValueError(f"Database query failed while {action}: {exc}") from exc

    @staticmethod
    def _to_snapshot(row: Any) -> Psm3Snapshot:
        return Psm3Snapshot(
            chain_id=row.chain_id,
            address="0x" + row.address_hex,
            usds_balance=row.usds_balance,
            susds_balance=row.susds_balance,
            usdc_balance=row.usdc_balance,
            total_assets=row.total_assets,
            conversion_rate=row.conversion_rate,
            block_number=row.block_number,
            block_version=row.block_version,
            block_timestamp=row.block_timestamp,
        )
