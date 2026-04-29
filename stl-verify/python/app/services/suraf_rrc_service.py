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


# TODO(VEC-179): Replace with shared RrcResult from app.domain.entities.risk
# when SurafRrcService is ported to the RiskModel protocol.
class RrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    usd_exposure: Decimal
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


class SurafRrcService:
    def __init__(
        self,
        asset_to_rating: dict[str, str],
        suraf_ratings: dict[str, SurafResult],
    ) -> None:
        self._asset_to_rating = asset_to_rating
        self._suraf_ratings = suraf_ratings

    def compute(self, asset: str, usd_exposure: Decimal) -> RrcResult | None:
        """Return the RRC for ``asset`` at the given USD exposure, or ``None`` if unmapped.

        Asset lookup is case-insensitive — mapping keys are casefolded at
        load time (see ``app.risk_engine.mapping``).
        """
        rating_id = self._asset_to_rating.get(asset.casefold())
        if rating_id is None:
            return None
        rating = self._suraf_ratings[rating_id]
        return RrcResult(
            asset=asset,
            usd_exposure=usd_exposure,
            rating_id=rating_id,
            rating_version=rating.version,
            crr_pct=rating.crr_pct,
            rrc_usd=usd_exposure * rating.crr_pct / _HUNDRED,
            source_commit_sha=rating.source_commit_sha,
        )
