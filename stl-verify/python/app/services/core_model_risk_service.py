"""CoreModelRiskService — reads pre-computed CORE results from the DB.

Implements the RiskModel protocol. CRR computation is delegated to the
core-model-runner cronjob. This service only reads the latest results and
multiplies them by the prime's USD exposure.

Two serving shapes:

- A SparkLend receipt token maps 1:1 to a market (``asset_to_market_key.json``),
  so the latest result for that market is the answer.
- A Morpho receipt token is a MetaMorpho vault share spread across many Blue
  markets at once (n:m), so the vault's live per-market allocations weight the
  per-market results into one figure, with an explicit ``coverage_pct``.
"""

import asyncio
from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any, NamedTuple

from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import CoreModelDetails, CoreModelMarketAllocation, ModelName, RrcResult
from app.domain.exceptions import ModelDataUnavailableError
from app.ports.allocation_repository import AllocationRepositoryPort
from app.ports.core_model_results_reader import CoreModelResult, CoreModelResultsReader
from app.ports.morpho_vault_allocations import MorphoVaultAllocations, MorphoVaultAllocationsReader
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.services._overrides import parse_usd_exposure_override

_HUNDRED = Decimal("100")
_USD_CENT = Decimal("0.01")
_PCT = Decimal("0.01")
_CRR_PCT = Decimal("0.0001")


def morpho_market_key_index(market_configs: Mapping[str, Mapping[str, Any]]) -> dict[tuple[str, str], str]:
    """Index MORPHO market keys by uppercased ``(collateral symbol, loan symbol)``.

    A Blue market key spans the token pair, not one LLTV tranche, and the pair
    is matched by display symbol — the same identity the runner's positions
    reader uses to select the markets it computes (``MORPHO_MARKET`` is the
    collateral symbol). Call at startup: a malformed config must fail the boot,
    not the first Morpho request.
    """
    index: dict[tuple[str, str], str] = {}
    for market_key, config in market_configs.items():
        if str(config.get("PROTOCOL", "")).upper() != "MORPHO":
            continue
        pair = (str(config["MORPHO_MARKET"]).upper(), str(config["LOAN_TOKEN"]).upper())
        if pair in index:
            raise ValueError(f"duplicate MORPHO market pair {pair}: {index[pair]!r} and {market_key!r}")
        index[pair] = market_key
    return index


class _CoveredMarket(NamedTuple):
    """One CORE-computed market with the vault supply allocated to it."""

    supply: Decimal
    result: CoreModelResult


class _WeightBase(NamedTuple):
    """The weight partition of a vault: covered supply + idle over ``total``."""

    total: Decimal
    covered_weight: Decimal
    coverage_pct: Decimal


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
        *,
        receipt_tokens: ReceiptTokenLookup,
        morpho_allocations: MorphoVaultAllocationsReader,
        morpho_market_keys: Mapping[tuple[str, str], str],
        morpho_asset_ids: frozenset[int],
        min_coverage_pct: Decimal,
    ) -> None:
        self._asset_to_market_key = asset_to_market_key
        self._results_reader = results_reader
        self._allocation_repo = allocation_repo
        self._receipt_tokens = receipt_tokens
        self._morpho_allocations = morpho_allocations
        self._morpho_market_keys = morpho_market_keys
        self._morpho_asset_ids = morpho_asset_ids
        self._min_coverage_pct = min_coverage_pct

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return asset_id in self._asset_to_market_key or asset_id in self._morpho_asset_ids

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
        market_key = self._asset_to_market_key.get(asset_id)
        if market_key is not None:
            return await self._compute_direct(asset_id, prime_id, usd_exposure, market_key)
        if asset_id in self._morpho_asset_ids:
            return await self._compute_morpho_vault(asset_id, prime_id, usd_exposure)
        raise ValueError(f"unsupported asset_id={asset_id}")

    async def _compute_direct(
        self,
        asset_id: int,
        prime_id: EthAddress,
        usd_exposure: Decimal,
        market_key: str,
    ) -> RrcResult:
        result = await self._results_reader.get_latest(market_key)
        if result is None:
            raise ModelDataUnavailableError(
                f"no pre-computed result for market_key={market_key!r} (asset_id={asset_id}); "
                "run the core-model-runner cronjob first"
            )
        return self._build_result(
            asset_id,
            prime_id,
            usd_exposure,
            crr_el_pct=result.crr_el_pct,
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

    async def _compute_morpho_vault(
        self,
        asset_id: int,
        prime_id: EthAddress,
        usd_exposure: Decimal,
    ) -> RrcResult:
        """Aggregate per-market results into one figure for a vault share.

        Weights are the vault's live per-market supply amounts (a Morpho
        supplier's bad-debt exposure is its whole supply in a market, socialized
        pro rata) plus idle liquidity at zero risk. The uncovered slice — markets
        without a computed CORE result — is excluded from the weighted average
        and reported through ``coverage_pct``; below the configured minimum the
        aggregate is not served at all, so the caller's preference chain falls
        back instead of extrapolating from a thin covered slice.
        """
        vault = await self._resolve_vault(asset_id)
        covered = await self._covered_markets(vault)
        if not covered:
            raise ModelDataUnavailableError(
                f"no CORE-computed market behind any allocation of asset_id={asset_id} "
                f"(vault_id={vault.vault_id}); coverage is 0%"
            )
        base = self._weight_base(vault, covered)
        if base.coverage_pct < self._min_coverage_pct:
            raise ModelDataUnavailableError(
                f"CORE coverage for asset_id={asset_id} is {base.coverage_pct}% of vault assets, "
                f"below the {self._min_coverage_pct}% minimum (vault_id={vault.vault_id})"
            )
        details = self._aggregate_details(covered, base)
        return self._build_result(asset_id, prime_id, usd_exposure, crr_el_pct=details.crr_el_pct, details=details)

    async def _covered_markets(self, vault: MorphoVaultAllocations) -> list[_CoveredMarket]:
        """Latest result per distinct covered market, weighted by summed supply.

        Several LLTV tranches of one token pair share one CORE market key, so
        allocations are grouped by resolved key before the result lookup: one
        slice and one read per market, weights added.
        """
        supply_by_key: dict[str, Decimal] = {}
        for alloc in vault.allocations:
            key = self._morpho_market_keys.get((alloc.collateral_symbol.upper(), alloc.loan_symbol.upper()))
            if key is not None:
                supply_by_key[key] = supply_by_key.get(key, Decimal("0")) + alloc.supply_assets
        results = await asyncio.gather(*(self._results_reader.get_latest(key) for key in supply_by_key))
        covered = [
            _CoveredMarket(supply, result)
            for supply, result in zip(supply_by_key.values(), results, strict=True)
            if result is not None
        ]
        return sorted(covered, key=lambda market: market.supply, reverse=True)

    @staticmethod
    def _weight_base(vault: MorphoVaultAllocations, covered: list[_CoveredMarket]) -> _WeightBase:
        allocated = sum((alloc.supply_assets for alloc in vault.allocations), Decimal("0"))
        # State and position snapshots are written at different blocks, so the
        # allocation sum can transiently exceed total_assets; the larger of the
        # two keeps the weights a partition (idle >= 0, coverage <= 100).
        total = max(vault.total_assets, allocated)
        idle = total - allocated
        covered_weight = sum((market.supply for market in covered), Decimal("0")) + idle
        coverage_pct = (covered_weight / total * _HUNDRED).quantize(_PCT, rounding=ROUND_HALF_EVEN)
        return _WeightBase(total=total, covered_weight=covered_weight, coverage_pct=coverage_pct)

    @staticmethod
    def _aggregate_details(covered: list[_CoveredMarket], base: _WeightBase) -> CoreModelDetails:
        def weighted(values: list[tuple[Decimal, Decimal]]) -> Decimal:
            total_value = sum((weight * value for weight, value in values), Decimal("0"))
            return (total_value / base.covered_weight).quantize(_CRR_PCT, rounding=ROUND_HALF_EVEN)

        return CoreModelDetails(
            risk_model="core_model",
            crr_el_pct=weighted([(market.supply, market.result.crr_el_pct) for market in covered]),
            crr_es_pct=weighted([(market.supply, market.result.crr_es_pct) for market in covered]),
            crr_var_pct=weighted([(market.supply, market.result.crr_var_pct) for market in covered]),
            hhi=None,
            protocol="MORPHO",
            forecast_step=min(market.result.forecast_step for market in covered),
            n_mc=min(market.result.n_mc for market in covered),
            # Every market config pins the same copula; per-slice params stay
            # visible in ``markets`` if that ever diverges.
            copula_type=covered[0].result.copula_type,
            coverage_pct=base.coverage_pct,
            markets=tuple(
                CoreModelMarketAllocation(
                    market_key=market.result.market_key,
                    allocation_pct=(market.supply / base.total * _HUNDRED).quantize(_PCT, rounding=ROUND_HALF_EVEN),
                    crr_el_pct=market.result.crr_el_pct,
                    crr_es_pct=market.result.crr_es_pct,
                    crr_var_pct=market.result.crr_var_pct,
                    n_mc=market.result.n_mc,
                    computed_at=market.result.computed_at,
                )
                for market in covered
            ),
        )

    def _build_result(
        self,
        asset_id: int,
        prime_id: EthAddress,
        usd_exposure: Decimal,
        *,
        crr_el_pct: Decimal,
        details: CoreModelDetails,
    ) -> RrcResult:
        rrc_usd = (usd_exposure * crr_el_pct / _HUNDRED).quantize(_USD_CENT, rounding=ROUND_HALF_EVEN)
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            comparable_crr_pct=crr_el_pct,
            risk_model=self.risk_model,
            details=details,
        )

    async def _resolve_vault(self, asset_id: int) -> MorphoVaultAllocations:
        info = await self._receipt_tokens.get(asset_id)
        if info is None:
            raise ModelDataUnavailableError(f"no receipt-token record for asset_id={asset_id}")
        vault = await self._morpho_allocations.get_vault_allocations(info.receipt_token_address, info.chain_id)
        if vault is None:
            raise ModelDataUnavailableError(
                f"no indexed morpho vault at the receipt-token address of asset_id={asset_id}"
            )
        return vault

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
