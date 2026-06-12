from typing import Protocol

from app.domain.entities.psm3 import PriceQuote, Psm3Snapshot


class Psm3Repository(Protocol):
    """Repository interface for PSM3 reserve snapshot and price queries."""

    async def list_latest_snapshots(self, chain_id: int | None = None) -> list[Psm3Snapshot]:
        """Return the latest snapshot per chain, optionally filtered to one chain."""
        ...

    async def list_snapshot_history(self, chain_id: int, *, limit: int = 100) -> list[Psm3Snapshot]:
        """Return snapshots for a chain, newest first."""
        ...

    async def get_latest_price(self, source_asset_id: str) -> PriceQuote | None:
        """Return the latest coingecko USD price for a source asset id, or None."""
        ...
