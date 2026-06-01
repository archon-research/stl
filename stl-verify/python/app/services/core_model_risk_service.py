"""CoreModelRiskService — reads pre-computed CORE results from the DB.

Implements the RiskModel protocol. CRR computation is delegated to the
core-model-runner cronjob. This service only reads the latest result and
multiplies it by the prime's USD exposure.
"""

from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import CoreModelDetails, ModelName, RrcResult
from app.domain.exceptions import InvalidOverrideError
from app.ports.allocation_repository import AllocationRepository
from app.ports.core_model_results_reader import CoreModelResultsReader

_HUNDRED = Decimal("100")
_USD_CENT = Decimal("0.01")
_ALLOWED_OVERRIDES = frozenset({"usd_exposure"})
_USD_EXPOSURE_MAX = Decimal("1e15")
_DECIMAL_STR_MAX_LEN = 64


class CoreModelRiskService:
    """CORE model risk service.

    Implements the :class:`~app.ports.risk_model.RiskModel` protocol so it
    can be used by the unified model registry.
    """

    risk_model: ModelName = "core_model"

    def __init__(
        self,
        asset_to_market_key: dict[int, str],
        results_reader: CoreModelResultsReader,
        allocation_repo: AllocationRepository,
    ) -> None:
        self._asset_to_market_key = asset_to_market_key
        self._results_reader = results_reader
        self._allocation_repo = allocation_repo

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return asset_id in self._asset_to_market_key

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        usd_exposure = await self._resolve_usd_exposure(asset_id, prime_id, overrides)
        market_key = self._asset_to_market_key[asset_id]
        result = await self._results_reader.get_latest(market_key)
        if result is None:
            raise ValueError(
                f"no pre-computed result for market_key={market_key!r} (asset_id={asset_id}); "
                "run the core-model-runner cronjob first"
            )
        rrc_usd = (usd_exposure * result.crr_el_pct / _HUNDRED).quantize(_USD_CENT, rounding=ROUND_HALF_EVEN)
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            comparable_crr_pct=result.crr_el_pct,
            risk_model=self.risk_model,
            details=CoreModelDetails(
                risk_model="core_model",
                crr_el_pct=result.crr_el_pct,
                crr_es_pct=result.crr_es_pct,
                crr_var_pct=result.crr_var_pct,
                hhi=result.hhi,
                protocol=result.protocol,
                forecast_step=result.forecast_step,
                n_mc=result.n_mc,
                copula_type=result.copula_type,
            ),
        )

    async def _resolve_usd_exposure(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> Decimal:
        unknown = set(overrides) - _ALLOWED_OVERRIDES
        if unknown:
            raise InvalidOverrideError(f"unknown override keys: {sorted(unknown)}")

        if "usd_exposure" not in overrides:
            return await self._allocation_repo.get_usd_exposure(asset_id, prime_id)

        raw = overrides["usd_exposure"]
        if raw is None:
            raise InvalidOverrideError("invalid usd_exposure: expected a positive finite number, got None")
        if isinstance(raw, str) and len(raw) > _DECIMAL_STR_MAX_LEN:
            raise InvalidOverrideError(
                f"invalid usd_exposure: input string too long ({len(raw)} > {_DECIMAL_STR_MAX_LEN})"
            )
        try:
            usd_exposure = raw if isinstance(raw, Decimal) else Decimal(str(raw))
        except Exception as exc:
            raise InvalidOverrideError(
                f"invalid usd_exposure: expected a positive finite number, got {raw!r}"
            ) from exc
        if not usd_exposure.is_finite():
            raise InvalidOverrideError(
                f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}"
            )
        if usd_exposure <= Decimal("0"):
            raise InvalidOverrideError(
                f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}"
            )
        if usd_exposure > _USD_EXPOSURE_MAX:
            raise InvalidOverrideError(f"usd_exposure must be <= {_USD_EXPOSURE_MAX:E}, got {usd_exposure}")
        return usd_exposure
