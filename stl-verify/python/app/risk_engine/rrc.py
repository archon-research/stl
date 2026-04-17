"""Compute RRC for a single asset at caller-supplied USD exposure.

Pure function over ``(asset, usd_exposure, mapping, ratings)``. No I/O,
no service dependencies. SURAF is an asset-level model: the CRR is
pre-computed per rating package, so ``RRC = usd_exposure * CRR``.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from app.risk_engine.suraf.result import SurafResult

_HUNDRED = Decimal("100")


class RrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    usd_exposure: Decimal
    rating_id: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


def compute_rrc(
    asset: str,
    usd_exposure: Decimal,
    asset_to_rating: dict[str, str],
    suraf_ratings: dict[str, SurafResult],
) -> RrcResult | None:
    """Return ``None`` if no rating is mapped for ``asset``.

    Asset lookup is case-insensitive
    """
    rating_id = asset_to_rating.get(asset.casefold())
    if rating_id is None:
        return None
    rating = suraf_ratings[rating_id]
    return RrcResult(
        asset=asset,
        usd_exposure=usd_exposure,
        rating_id=rating_id,
        crr_pct=rating.crr_pct,
        rrc_usd=usd_exposure * rating.crr_pct / _HUNDRED,
        source_commit_sha=rating.source_commit_sha,
    )
