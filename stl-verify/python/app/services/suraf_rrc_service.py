"""SURAF RRC service — thin seam between the router and ``compute_rrc``.

Constructed once at startup with the loaded ratings and asset mapping;
stateless across requests. Exists so the router depends on a named
service rather than two raw dicts, matching the pattern of the other
risk endpoints (``RiskCalculationService``).
"""

from __future__ import annotations

from decimal import Decimal

from app.risk_engine.rrc import RrcResult, compute_rrc
from app.risk_engine.suraf.result import SurafResult


class SurafRrcService:
    def __init__(
        self,
        asset_to_rating: dict[str, str],
        suraf_ratings: dict[str, SurafResult],
    ) -> None:
        self._asset_to_rating = asset_to_rating
        self._suraf_ratings = suraf_ratings

    def compute(self, asset: str, usd_exposure: Decimal) -> RrcResult | None:
        """Return the RRC for ``asset`` at the given USD exposure, or ``None`` if unmapped."""
        return compute_rrc(asset, usd_exposure, self._asset_to_rating, self._suraf_ratings)
