from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Protocol, runtime_checkable

from app.domain.entities.risk import AssetId, PrimeId, RrcResult


@runtime_checkable
class RiskModel(Protocol):
    async def applies_to(self, asset_id: AssetId, prime_id: PrimeId) -> bool:
        ...

    async def compute(
        self,
        asset_id: AssetId,
        prime_id: PrimeId,
        overrides: Mapping[str, Decimal] | None = None,
    ) -> RrcResult:
        ...
