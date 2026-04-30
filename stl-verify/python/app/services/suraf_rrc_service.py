"""SURAF RRC service.

SURAF is an asset-level model: the CRR is pre-computed per rating package,
so ``RRC = usd_exposure * CRR``. The service is constructed once at
startup with the loaded ratings and asset mapping and is stateless
across requests.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import RrcResult, SurafDetails
from app.ports.allocation_repository import AllocationRepository
from app.risk_engine.suraf.result import SurafResult

_HUNDRED = Decimal("100")
_ALLOWED_OVERRIDES = frozenset({"usd_exposure"})


# Legacy result type — used by old /risk/rrc/scenario endpoint until VEC-183.
class LegacyRrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    receipt_token_id: int
    usd_exposure: Decimal
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


class SurafRrcService:
    """SURAF risk model.

    Implements the :class:`~app.ports.risk_model.RiskModel` protocol so it can
    be used by the unified risk-model registry.
    """

    model: str = "suraf"

    def __init__(
        self,
        asset_to_rating: dict[int, str],
        suraf_ratings: dict[str, SurafResult],
        allocation_repo: AllocationRepository,
    ) -> None:
        self._asset_to_rating = asset_to_rating
        self._suraf_ratings = suraf_ratings
        self._allocation_repo = allocation_repo

    # ------------------------------------------------------------------
    # RiskModel interface implementation
    # ------------------------------------------------------------------

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        """SURAF applies iff the asset_id maps to a known rating."""
        return asset_id in self._asset_to_rating

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        """Compute the RRC for the given asset and prime via SURAF."""
        usd_exposure = await self._resolve_usd_exposure(asset_id, prime_id, overrides)
        rating_id, rating = self._lookup_rating(asset_id)
        rrc_usd = usd_exposure * rating.crr_pct / _HUNDRED
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            model="suraf",
            details=SurafDetails(
                rating_id=rating_id,
                rating_version=rating.version,
                crr_pct=rating.crr_pct,
                source_commit_sha=rating.source_commit_sha,
            ),
        )

    async def _resolve_usd_exposure(self, asset_id: int, prime_id: EthAddress, overrides: Mapping[str, Any]) -> Decimal:
        """Extract usd_exposure from overrides, or derive from position."""
        unknown = set(overrides) - _ALLOWED_OVERRIDES
        if unknown:
            raise ValueError(f"unknown override keys: {sorted(unknown)}")

        raw = overrides.get("usd_exposure")
        if raw is not None:
            try:
                usd_exposure = raw if isinstance(raw, Decimal) else Decimal(str(raw))
            except Exception as exc:
                raise ValueError(f"invalid usd_exposure: expected a positive finite number, got {raw!r}") from exc
            if not usd_exposure.is_finite():
                raise ValueError(f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}")
            if usd_exposure <= Decimal("0"):
                raise ValueError(f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}")
            return usd_exposure

        return await self._allocation_repo.get_usd_exposure(asset_id, prime_id)

    def _lookup_rating(self, asset_id: int) -> tuple[str, SurafResult]:
        """Look up the rating for the given asset_id, or raise."""
        rating_id = self._asset_to_rating.get(asset_id)
        if rating_id is None:
            raise ValueError(f"no rating mapped for asset_id={asset_id}")
        rating = self._suraf_ratings.get(rating_id)
        if rating is None:
            raise ValueError(f"rating_id={rating_id!r} not found in suraf_ratings (asset_id={asset_id})")
        return rating_id, rating

    # ------------------------------------------------------------------
    # Legacy API — used by old /risk/rrc/scenario endpoint, will be removed in VEC-183.
    # ------------------------------------------------------------------

    def compute_legacy(self, receipt_token_id: int, usd_exposure: Decimal) -> LegacyRrcResult | None:
        """Return the RRC for ``receipt_token_id`` at the given USD exposure, or ``None`` if unmapped."""
        rating_id = self._asset_to_rating.get(receipt_token_id)
        if rating_id is None:
            return None
        rating = self._suraf_ratings.get(rating_id)
        if rating is None:
            raise ValueError(
                f"rating_id={rating_id!r} not found in suraf_ratings (receipt_token_id={receipt_token_id})"
            )
        return LegacyRrcResult(
            receipt_token_id=receipt_token_id,
            usd_exposure=usd_exposure,
            rating_id=rating_id,
            rating_version=rating.version,
            crr_pct=rating.crr_pct,
            rrc_usd=usd_exposure * rating.crr_pct / _HUNDRED,
            source_commit_sha=rating.source_commit_sha,
        )
