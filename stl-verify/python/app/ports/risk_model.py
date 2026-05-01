"""RiskModel port — the abstraction every risk model must satisfy.

The registry dispatches to models via ``applies_to`` and ``compute``.
Domain code depends only on this protocol; concrete implementations
live in adapters or services.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import RrcResult


class RiskModel(Protocol):
    """Inbound port that any risk model (SURAF, gap-sweep, ...) must implement."""

    model: str
    """Discriminator string used in responses and override dispatch."""

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:
        """Return True if this model handles the given asset/prime pair."""
        ...

    async def compute(self, asset_id: int, prime_id: EthAddress, overrides: Mapping[str, Any]) -> RrcResult:
        """Compute the RRC for the given asset and prime."""
        ...
