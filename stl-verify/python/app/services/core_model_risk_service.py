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
from app.domain.exceptions import ModelDataUnavailableError
from app.ports.allocation_repository import AllocationRepositoryPort
from app.ports.core_model_results_reader import CoreModelResult, CoreModelResultsReader
from app.services._overrides import parse_usd_exposure_override

_HUNDRED = Decimal("100")
_USD_CENT = Decimal("0.01")


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
        allocation_repo: AllocationRepositoryPort,
    ) -> None:
        self._asset_to_market_key = asset_to_market_key
        self._results_reader = results_reader
        self._allocation_repo = allocation_repo

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return asset_id in self._asset_to_market_key

    async def get_latest_result(self, asset_id: int) -> CoreModelResult | None:
        market_key = self._asset_to_market_key.get(asset_id)
        if market_key is None:
            return None
        return await self._results_reader.get_latest(market_key)

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
            raise ModelDataUnavailableError(
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
        override = parse_usd_exposure_override(overrides)
        if override is not None:
            return override
        try:
            return await self._allocation_repo.get_usd_exposure(asset_id, prime_id)
        except ValueError as exc:
            # get_usd_exposure raises ValueError when the prime holds no
            # resolvable position; without exposure this model has nothing to
            # multiply, so the envelope should skip it, not 500.
            raise ModelDataUnavailableError(
                f"no resolvable position for asset_id={asset_id} prime_id={prime_id}: {exc}"
            ) from exc
