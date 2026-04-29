"""SURAF RRC service.

SURAF is an asset-level model: the CRR is pre-computed per rating package,
so ``RRC = usd_exposure * CRR``. The service is constructed once at
startup with the loaded ratings and asset mapping and is stateless
across requests.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from app.risk_engine.suraf.result import SurafResult

_HUNDRED = Decimal("100")


class RrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    receipt_token_id: int
    usd_exposure: Decimal
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


class SurafRrcService:
    def __init__(
        self,
        asset_to_rating: dict[int, str],
        suraf_ratings: dict[str, SurafResult],
    ) -> None:
        self._asset_to_rating = asset_to_rating
        self._suraf_ratings = suraf_ratings

    def compute(self, receipt_token_id: int, usd_exposure: Decimal) -> RrcResult | None:
        """Return the RRC for ``receipt_token_id`` at the given USD exposure, or ``None`` if unmapped."""
        rating_id = self._asset_to_rating.get(receipt_token_id)
        if rating_id is None:
            return None
        rating = self._suraf_ratings[rating_id]
        return RrcResult(
            receipt_token_id=receipt_token_id,
            usd_exposure=usd_exposure,
            rating_id=rating_id,
            rating_version=rating.version,
            crr_pct=rating.crr_pct,
            rrc_usd=usd_exposure * rating.crr_pct / _HUNDRED,
            source_commit_sha=rating.source_commit_sha,
        )
