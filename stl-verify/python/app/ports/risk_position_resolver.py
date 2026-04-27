from __future__ import annotations

from typing import Protocol

from app.domain.entities.risk import AssetId, PrimeId, ResolvedRiskPosition


class RiskPositionResolver(Protocol):
    async def resolve(self, asset_id: AssetId, prime_id: PrimeId) -> ResolvedRiskPosition:
        """Resolve the current risk position for an external ``(asset_id, prime_id)`` pair."""
        ...
