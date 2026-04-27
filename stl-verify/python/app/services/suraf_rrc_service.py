"""SURAF RRC service.

SURAF is an asset-level model: the CRR is pre-computed per rating package,
so ``RRC = usd_exposure * CRR``. The service is constructed once at
startup with the loaded ratings and asset mapping and is stateless
across requests.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.domain.entities.risk import (
    AssetId,
    InvalidRiskModelOverridesError,
    PrimeId,
    ResolvedRiskPosition,
    RiskModelInputsUnavailableError,
    RiskModelNotApplicableError,
    RrcResult,
    SurafRrcDetails,
)
from app.ports.risk_position_resolver import RiskPositionResolver
from app.risk_engine.suraf.result import SurafResult

_HUNDRED = Decimal("100")
_ZERO = Decimal("0")
_ALLOWED_OVERRIDE_KEYS = frozenset({"usd_exposure"})
_MODEL_NAME = "suraf"


class ScenarioRrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    usd_exposure: Decimal
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


@dataclass(frozen=True)
class _SurafComputation:
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
        position_resolver: RiskPositionResolver,
    ) -> None:
        self._asset_to_rating = asset_to_rating
        self._suraf_ratings = suraf_ratings
        self._position_resolver = position_resolver

    async def applies_to(self, asset_id: AssetId, prime_id: PrimeId) -> bool:
        position = await self._position_resolver.resolve(asset_id, prime_id)
        return self._lookup_rating_for_position(position) is not None

    async def compute(
        self,
        asset_id: AssetId,
        prime_id: PrimeId,
        overrides: Mapping[str, Decimal] | None = None,
    ) -> RrcResult:
        usd_exposure_override = self._validate_overrides(overrides)
        position = await self._position_resolver.resolve(asset_id, prime_id)

        resolved = self._lookup_rating_for_position(position)
        if resolved is None:
            raise RiskModelNotApplicableError(_MODEL_NAME, asset_id, prime_id)

        rating_id, rating = resolved
        usd_exposure, exposure_source = self._resolve_usd_exposure(
            position=position,
            asset_id=asset_id,
            prime_id=prime_id,
            usd_exposure_override=usd_exposure_override,
        )
        computation = self._build_computation(rating_id=rating_id, rating=rating, usd_exposure=usd_exposure)

        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=computation.rrc_usd,
            details=SurafRrcDetails(
                chain_id=position.chain_id,
                protocol_name=position.protocol_name,
                symbol=position.symbol,
                underlying_symbol=position.underlying_symbol,
                receipt_token_address=position.receipt_token_address,
                underlying_token_address=position.underlying_token_address,
                usd_exposure=computation.usd_exposure,
                usd_exposure_source=exposure_source,
                rating_id=computation.rating_id,
                rating_version=computation.rating_version,
                crr_pct=computation.crr_pct,
                source_commit_sha=computation.source_commit_sha,
            ),
        )

    def compute_scenario(self, asset: str, usd_exposure: Decimal) -> ScenarioRrcResult | None:
        """Compatibility shim for the legacy symbol-based scenario endpoint."""
        resolved = self._lookup_rating_for_asset(asset)
        if resolved is None:
            return None

        rating_id, rating = resolved
        computation = self._build_computation(rating_id=rating_id, rating=rating, usd_exposure=usd_exposure)
        return ScenarioRrcResult(
            asset=asset,
            usd_exposure=computation.usd_exposure,
            rating_id=computation.rating_id,
            rating_version=computation.rating_version,
            crr_pct=computation.crr_pct,
            rrc_usd=computation.rrc_usd,
            source_commit_sha=computation.source_commit_sha,
        )

    def _lookup_rating_for_asset(self, asset: str) -> tuple[str, SurafResult] | None:
        rating_id = self._asset_to_rating.get(asset.casefold())
        if rating_id is None:
            return None

        rating = self._suraf_ratings.get(rating_id)
        if rating is None:
            return None
        return rating_id, rating

    def _lookup_rating_for_position(self, position: ResolvedRiskPosition) -> tuple[str, SurafResult] | None:
        return self._lookup_rating_for_asset(position.symbol)

    def _validate_overrides(self, overrides: Mapping[str, Decimal] | None) -> Decimal | None:
        if overrides is None:
            return None

        unknown = sorted(set(overrides) - _ALLOWED_OVERRIDE_KEYS)
        if unknown:
            raise InvalidRiskModelOverridesError(_MODEL_NAME, f"unknown override keys: {unknown}")

        usd_exposure = overrides.get("usd_exposure")
        if usd_exposure is None:
            return None
        if usd_exposure <= _ZERO:
            raise InvalidRiskModelOverridesError(_MODEL_NAME, "usd_exposure must be positive")
        return usd_exposure

    def _resolve_usd_exposure(
        self,
        *,
        position: ResolvedRiskPosition,
        asset_id: AssetId,
        prime_id: PrimeId,
        usd_exposure_override: Decimal | None,
    ) -> tuple[Decimal, Literal["position", "override"]]:
        if usd_exposure_override is not None:
            return usd_exposure_override, "override"

        if position.usd_exposure is None:
            raise RiskModelInputsUnavailableError(
                _MODEL_NAME,
                asset_id,
                prime_id,
                "usd_exposure is unavailable for the resolved position",
            )
        if position.usd_exposure <= _ZERO:
            raise RiskModelInputsUnavailableError(
                _MODEL_NAME,
                asset_id,
                prime_id,
                f"derived usd_exposure must be positive, got {position.usd_exposure}",
            )
        return position.usd_exposure, "position"

    @staticmethod
    def _build_computation(rating_id: str, rating: SurafResult, usd_exposure: Decimal) -> _SurafComputation:
        return _SurafComputation(
            usd_exposure=usd_exposure,
            rating_id=rating_id,
            rating_version=rating.version,
            crr_pct=rating.crr_pct,
            rrc_usd=usd_exposure * rating.crr_pct / _HUNDRED,
            source_commit_sha=rating.source_commit_sha,
        )
