from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from app.domain.entities.risk import (
    AssetId,
    GapSweepRrcDetails,
    InvalidRiskModelOverridesError,
    PrimeId,
    RiskModelInputsUnavailableError,
    RiskModelNotApplicableError,
    RrcResult,
)
from app.domain.risk_protocols import supports_gap_sweep
from app.ports.risk_position_resolver import RiskPositionResolver
from app.risk_engine.crypto_lending import gap_sweep
from app.services.risk_service_factory import RiskServiceFactory

_ALLOWED_OVERRIDE_KEYS = frozenset({"gap_pct"})
_MODEL_NAME = "gap_sweep"
_ZERO = Decimal("0")
_ONE = Decimal("1")


class GapSweepRiskModel:
    def __init__(
        self,
        position_resolver: RiskPositionResolver,
        factory: RiskServiceFactory,
        default_gap_pct: Decimal,
    ) -> None:
        self._position_resolver = position_resolver
        self._factory = factory
        self._default_gap_pct = default_gap_pct

    async def applies_to(self, asset_id: AssetId, prime_id: PrimeId) -> bool:
        position = await self._position_resolver.resolve(asset_id, prime_id)
        return supports_gap_sweep(position.protocol_name)

    async def compute(
        self,
        asset_id: AssetId,
        prime_id: PrimeId,
        overrides: Mapping[str, Decimal] | None = None,
    ) -> RrcResult:
        gap_pct = self._validate_overrides(overrides)
        position = await self._position_resolver.resolve(asset_id, prime_id)
        if not supports_gap_sweep(position.protocol_name):
            raise RiskModelNotApplicableError(_MODEL_NAME, asset_id, prime_id)

        try:
            construction = await self._factory.create_for_position(position)
        except ValueError as exc:
            raise RiskModelInputsUnavailableError(_MODEL_NAME, asset_id, prime_id, str(exc)) from exc

        breakdown = await construction.service.get_risk_breakdown(backed_asset_id=construction.backed_asset_id)
        raw_bad_debt = gap_sweep.total_bad_debt(breakdown.items, gap_pct)

        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=abs(raw_bad_debt),
            details=GapSweepRrcDetails(
                chain_id=position.chain_id,
                protocol_name=position.protocol_name,
                symbol=position.symbol,
                underlying_symbol=position.underlying_symbol,
                receipt_token_address=position.receipt_token_address,
                underlying_token_address=position.underlying_token_address,
                effective_gap_pct=gap_pct,
                backed_asset_id=construction.backed_asset_id,
                collateral_item_count=len(breakdown.items),
            ),
        )

    def _validate_overrides(self, overrides: Mapping[str, Decimal] | None) -> Decimal:
        if overrides is None:
            return self._default_gap_pct

        unknown = sorted(set(overrides) - _ALLOWED_OVERRIDE_KEYS)
        if unknown:
            raise InvalidRiskModelOverridesError(_MODEL_NAME, f"unknown override keys: {unknown}")

        gap_pct = overrides.get("gap_pct", self._default_gap_pct)
        if not (_ZERO <= gap_pct <= _ONE):
            raise InvalidRiskModelOverridesError(_MODEL_NAME, "gap_pct must be between 0 and 1")
        return gap_pct
