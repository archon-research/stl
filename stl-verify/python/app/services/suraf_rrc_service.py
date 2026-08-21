"""SURAF RRC service.

SURAF is an asset-level model: the CRR is pre-computed per rating package,
so ``RRC = usd_exposure * CRR``. The service is constructed once at
startup with the loaded ratings and asset mapping and is stateless
across requests.
"""

from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import ModelName, RrcResult, SurafDetails
from app.ports.allocation_repository import AllocationRepositoryPort
from app.risk_engine.suraf.result import SurafResult
from app.services._overrides import parse_usd_exposure_override

_HUNDRED = Decimal("100")
# Quantize RRC to USD cents at the service boundary so clients get a
# bounded-precision number instead of the 25+ significant digits that
# fall out of unconstrained Decimal arithmetic.
_USD_CENT = Decimal("0.01")


class SurafRrcService:
    """SURAF risk model.

    Implements the :class:`~app.ports.risk_model.RiskModel` protocol so it can
    be used by the unified risk-model registry.
    """

    risk_model: ModelName = "suraf"

    def __init__(
        self,
        asset_to_rating: dict[int, str],
        suraf_ratings: dict[str, SurafResult],
        allocation_repo: AllocationRepositoryPort,
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
        rrc_usd = (usd_exposure * rating.crr_pct / _HUNDRED).quantize(_USD_CENT, rounding=ROUND_HALF_EVEN)
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            comparable_crr_pct=rating.crr_pct,
            risk_model=self.risk_model,
            details=SurafDetails(
                risk_model="suraf",
                rating_id=rating_id,
                rating_version=rating.version,
                crr_pct=rating.crr_pct,
                unadjusted_crr_pct=rating.unadjusted_crr_pct,
                penalty_pp=rating.penalty_pp,
                source_commit_sha=rating.source_commit_sha,
            ),
        )

    async def _resolve_usd_exposure(self, asset_id: int, prime_id: EthAddress, overrides: Mapping[str, Any]) -> Decimal:
        """Extract usd_exposure from overrides, or derive from position."""
        override = parse_usd_exposure_override(overrides)
        if override is not None:
            return override
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
