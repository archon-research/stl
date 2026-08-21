from collections.abc import Callable
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import ProxyAddressPathParam
from app.api.deps import get_engine, get_model_registry, get_reference_risk_capital_service_factory
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_risk_capital import PrimeRiskCapital, UnpricedReason
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.domain.exceptions import ReferenceDataUnavailableError
from app.domain.prime_registry import ProxyKind, alm_proxies_for_prime, classify_proxy
from app.domain.serialization import PlainDecimal
from app.services.model_registry import ModelRegistry
from app.services.prime_risk_capital_service import PrimeRiskCapitalService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

router = APIRouter(tags=["primes", "capital"])


class AllocationRiskCapitalResponse(BaseModel):
    """Per-allocation risk capital from the default model, or from the reference feed."""

    receipt_token_id: int | None = Field(
        description=(
            "Surrogate id of the receipt token. Always set in self mode. Under `reference=true` it is "
            "`null` when the upstream position does not join to STL's token registry — an unmapped "
            "network, a token STL does not index, or a Uniswap V4 position, which identifies itself by "
            "32-byte pool id where an address is expected and so can never resolve. `token_address` "
            "carries the raw upstream value in that case."
        )
    )
    symbol: str = Field(description="Receipt-token symbol.")
    protocol_name: str = Field(description="Protocol the allocation sits in.")
    exposure_usd: PlainDecimal = Field(description="On-chain USD exposure of the allocation.")
    applied: bool = Field(
        description=(
            "Whether the figure is priced. Always `true` under `reference=true`: the upstream monitor "
            "reports only positions it has already priced."
        )
    )
    required_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Per-allocation RRC (USD). `null` when the allocation is unpriced."
    )
    crr_pct: PlainDecimal | None = Field(
        default=None,
        description=(
            "Comparable capital-risk ratio (0-100). `null` when the allocation is unpriced. Under "
            "`reference=true` this is upstream's `crr` rescaled from its native 0-1 fraction, so the "
            "scale matches self mode."
        ),
    )
    model: str | None = Field(
        default=None,
        description="Model that produced the figure. `null` when unpriced, and always `null` under `reference=true`.",
    )
    chain: str | None = Field(
        default=None,
        description=(
            "Internal chain name the position sits on. Reference-only: `null` in self mode, and `null` "
            "under `reference=true` for a network STL has no chain id for."
        ),
        examples=["mainnet"],
    )
    token_address: str | None = Field(
        default=None,
        description=(
            "Upstream's raw position identifier, normally the receipt-token address. Reference-only "
            "(`null` in self mode). Not always an address: a Uniswap V4 row carries a 66-character "
            "pool id here, which is why `receipt_token_id` can be `null`."
        ),
    )
    loan_token_symbol: str | None = Field(
        default=None,
        description="Symbol of the loan token the exposure is denominated against. Reference-only.",
        examples=["USDS"],
    )
    unpriced_reason: UnpricedReason | None = Field(
        default=None,
        description=(
            "Why the allocation is unpriced (`null` when `applied`): `no_model` (no default model applies), "
            "`share_data_missing` / `share_data_stale` (a model applies but its pool-share lookup could "
            "not be resolved, e.g. a warm-up window or an un-indexed receipt token), or "
            "`price_data_missing` (the backed asset's loan token has no USD price)."
        ),
    )


class ChainRiskCapitalResponse(BaseModel):
    """One ALM proxy's contribution to the prime's aggregated figures.

    A row exists for every ALM proxy the axis-synome contract lists for this
    prime, including chains STL has no allocation tracker for. On such a chain the
    figures are `null`, not `"0"`: STL holds no positions for it at all, so a zero
    would assert the prime is empty there when the truth is that it is not
    indexed. `prime_unserved_chains` names those chains, and the `prime_*` totals
    exclude them.
    """

    proxy_address: str = Field(description="0x-prefixed ALM proxy address.")
    chain: str | None = Field(
        default=None,
        description="Internal chain name. `null` for a proxy absent from the axis-synome contract.",
        examples=["avalanche-c"],
    )
    exposure_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Priced receipt-token exposure held through this proxy (USD). `null` when no allocation "
            "tracker serves this chain, so nothing is known either way."
        ),
    )
    required_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description="Required Risk Capital from this proxy's positions (USD). `null` when the chain is unserved.",
    )
    allocation_count: int | None = Field(
        default=None,
        description="Number of allocations this proxy contributed. `null` when the chain is unserved.",
    )


class PrimeRiskCapitalResponse(BaseModel):
    """Capital metrics for a prime, from one of two provenances — see `source`.

    Under `source: "self"` (the default) the figures are model-derived from
    on-chain data: `required_risk_capital_usd` sums per-allocation RRC from the
    default model (`model`), so it is **partial** — only allocations the model
    can price contribute — and **will not** match Sky's dashboard.

    Under `source: "reference"` they are Sky's own published figures, and every
    figure is **prime-scoped**: upstream reports per prime, so the unprefixed
    fields carry the same values as their `prime_`-prefixed counterparts. Do not
    sum them across a prime's proxies — dedupe, as for any `prime_` field.
    """

    prime_id: str = Field(
        deprecated=True,
        description=(
            "DEPRECATED — despite the `prime_` prefix this is the queried ALM **proxy** address, not a "
            "prime identity, and it varies across a prime's proxies. It is byte-identical to "
            "`proxy_address` in the same response. Its value is unchanged for backwards compatibility. "
            "Use `proxy_address` to identify the proxy these figures are scoped to, and `prime_name` or "
            "`prime_proxies` to group by prime."
        ),
        examples=["0x1601843c5e9bc251a3272907010afa41fa18347e"],
    )
    proxy_address: str = Field(
        description=(
            "The 0x-prefixed ALM proxy address from the path, echoed back. This is what the unprefixed "
            "figures are scoped to, so a client fanning out across a prime's proxies can match each "
            "response to the request it answers."
        ),
        examples=["0x1601843c5e9bc251a3272907010afa41fa18347e"],
    )
    source: Literal["self", "reference"] = Field(
        default="self",
        description=(
            "Provenance of every figure in this response. `self` is STL's own on-chain model; "
            "`reference` is Sky's Star Agents Risk Capital & Requirements Monitor, returned when "
            "`reference=true`. Never mixed: one response is entirely one or the other."
        ),
    )
    model: str | None = Field(
        description="Default RRC model used (e.g. `gap_sweep`). `null` under `reference=true`, which runs no model.",
        examples=["gap_sweep"],
    )
    exposure_usd: PlainDecimal = Field(
        description=(
            "Σ priced receipt-token allocation exposure (USD). Under `reference=true` this is upstream's "
            "own total, which deliberately does not equal the sum of `per_allocation` — the two come "
            "from separately-computed snapshots and reconcile only to about 1e-6."
        )
    )
    total_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "On-chain SubProxy treasury balance (USD). `null` when absent. Under `reference=true` this "
            "is upstream's Total Risk Capital, which is neither on-chain nor a treasury balance."
        ),
    )
    required_risk_capital_usd: PlainDecimal = Field(
        description=(
            "Σ per-allocation RRC from the default model (USD). Under `reference=true` this is upstream's "
            "own Required Risk Capital total; no model runs."
        )
    )
    encumbrance_ratio: PlainDecimal | None = Field(
        default=None,
        deprecated=True,
        description=(
            "DEPRECATED — divides this proxy's Required Risk Capital by the whole prime's Total "
            "Risk Capital, mixing scopes, so the figure is not meaningful for either. Its value "
            "is unchanged for backwards compatibility. Use `prime_encumbrance_ratio`."
        ),
    )
    modeled_exposure_usd: PlainDecimal = Field(
        description=(
            "Exposure the default model could price (USD). Under `reference=true` it equals "
            "`exposure_usd`: the monitor publishes only positions it has already priced."
        )
    )
    modeled_pct: PlainDecimal | None = Field(
        default=None, description="`modeled_exposure_usd / exposure_usd` (0-1). `null` when exposure is zero."
    )
    per_allocation: list[AllocationRiskCapitalResponse] = Field(
        description="Per-allocation breakdown, newest-exposure first."
    )
    prime_name: str | None = Field(
        default=None,
        description="Prime this proxy belongs to. `null` for a proxy absent from the axis-synome contract.",
        examples=["spark"],
    )
    prime_exposure_usd: PlainDecimal = Field(
        default=Decimal("0"),
        description=(
            "Σ priced exposure across the prime's ALM proxies on chains STL indexes (USD). "
            "Prime-scoped: dedupe, never sum. Chains listed in `prime_unserved_chains` contribute "
            "nothing, so this is a lower bound on what the prime holds."
        ),
    )
    prime_required_risk_capital_usd: PlainDecimal = Field(
        default=Decimal("0"),
        description=(
            "Σ Required Risk Capital across the prime's ALM proxies on chains STL indexes (USD). "
            "Prime-scoped. Bounded by `prime_unserved_chains` in the same way as `prime_exposure_usd`, "
            "so `prime_encumbrance_ratio` built on it reads low rather than high."
        ),
    )
    prime_modeled_exposure_usd: PlainDecimal = Field(
        default=Decimal("0"), description="Σ exposure the default model could price, prime-wide (USD)."
    )
    prime_modeled_pct: PlainDecimal | None = Field(
        default=None, description="`prime_modeled_exposure_usd / prime_exposure_usd` (0-1)."
    )
    prime_encumbrance_ratio: PlainDecimal | None = Field(
        default=None,
        description=(
            "`prime_required_risk_capital_usd / total_risk_capital_usd` — the prime's true "
            "encumbrance. Both sides are prime-scoped, so this is identical whichever of the "
            "prime's proxies is queried. `null` when Total Risk Capital is absent or zero."
        ),
        examples=["0.9397"],
    )
    prime_proxies: list[str] = Field(
        default_factory=list,
        description=(
            "Every ALM proxy of the prime, address-sorted. Those on served chains carry the figures the "
            "`prime_*` totals are aggregated from; see `prime_per_chain` for which did."
        ),
    )
    prime_per_chain: list[ChainRiskCapitalResponse] = Field(
        default_factory=list, description="Per-proxy breakdown of the aggregated numerator, so the sum is auditable."
    )
    prime_unserved_chains: list[str] = Field(
        default_factory=list,
        description=(
            "Chains the prime has an ALM proxy on that no allocation tracker serves, so they contribute "
            "nothing to the `prime_*` totals and read `null` in `prime_per_chain`. Non-empty means the "
            "totals are a lower bound. Always empty under `reference=true`: upstream's totals are not "
            "bounded by what STL indexes, so the caveat does not apply to them."
        ),
        examples=[["arbitrum", "optimism", "unichain"]],
    )
    junior_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Junior (first-loss) risk capital (USD). Reference-only — `null` unless `reference=true`. "
            "This is the measured junior/senior split, which self mode has no equivalent for: it can "
            "only approximate a buffer as `total_risk_capital_usd - required_risk_capital_usd`."
        ),
    )
    senior_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Senior risk capital (USD). Reference-only."
    )
    internal_junior_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Junior risk capital held internally (USD). Reference-only."
    )
    external_junior_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Junior risk capital held externally (USD). Reference-only."
    )
    tokenized_junior_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Tokenized junior risk capital (USD). Reference-only."
    )
    internal_senior_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Senior risk capital held internally (USD). Reference-only."
    )
    external_senior_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Senior risk capital held externally (USD). Reference-only."
    )
    epi_utilization: PlainDecimal | None = Field(
        default=None, description="Upstream EPI utilization ratio. Reference-only."
    )
    spj_utilization: PlainDecimal | None = Field(
        default=None, description="Upstream SPJ utilization ratio. Reference-only."
    )
    exposure_share: PlainDecimal | None = Field(
        default=None,
        description="The prime's share of total protocol exposure, as reported upstream. Reference-only.",
    )


async def _get_service(
    engine: AsyncEngine = Depends(get_engine),
    registry: ModelRegistry = Depends(get_model_registry),
) -> PrimeRiskCapitalService:
    return PrimeRiskCapitalService(AllocationRepository(engine), registry)


@router.get(
    "/primes/{prime_id}/risk-capital",
    response_model=PrimeRiskCapitalResponse,
    tags=["primes", "capital"],
    summary="Self-computed prime risk capital",
    description=(
        "Compute the prime's capital metrics from on-chain data and the default RRC model "
        "(`gap_sweep`), with no dependency on the upstream Star feed. Returns exposure (priced "
        "receipt-token allocations), Total Risk Capital (on-chain treasury), Required Risk Capital "
        "(sum of per-allocation model RRC), encumbrance, a `modeled_pct` coverage figure, and a "
        "per-allocation breakdown. The figures are model-derived and partial (only allocations the "
        "model can price contribute Required Risk Capital) and will not match Sky's dashboard. "
        "A backed allocation whose pool-share lookup can't be resolved (e.g. a warm-up window or an "
        "un-indexed receipt token) is reported as unpriced (`applied=false` with an `unpriced_reason`) "
        "rather than failing the whole response. Returns `404` if the prime is unknown, and also "
        "if the address is a SubProxy treasury wallet: those hold a prime's treasury rather than "
        "its allocations, so they have no prime-level risk capital to report. Read the treasury at "
        "`/v1/primes/{prime_id}/total-capital` with one of the prime's ALM proxies, which "
        "`/v1/primes` lists.\n\n"
        "Figures without a prefix are scoped to the proxy in the path. Figures prefixed `prime_` are "
        "scoped to the whole prime — summed across the ALM proxies of the prime the given address "
        "belongs to that sit on chains STL indexes — and are therefore identical whichever proxy you "
        "query; use `prime_per_chain` for the split and `prime_unserved_chains` for what is missing "
        "from it. The one exception is an address the axis-synome contract does not list: it has no "
        "discoverable siblings, so its `prime_` figures cover that proxy alone and will not agree with "
        "what the prime's known proxies report. `total_risk_capital_usd` is prime-wide despite having "
        "no prefix. `prime_id` breaks the convention the other way — it is the queried proxy address "
        "rather than the prime — and is deprecated in favour of the identically-valued `proxy_address`. "
        "`encumbrance_ratio` is deprecated because it mixes the two scopes; use "
        "`prime_encumbrance_ratio`."
    ),
)
async def get_prime_risk_capital(
    prime_id: ProxyAddressPathParam,
    reference: bool = Query(
        False,
        description=(
            "Answer from Sky's upstream Star monitor instead of STL's own model. The response shape is "
            "unchanged; `source` reports which provenance produced it, and the reference-only fields "
            "(`junior_risk_capital_usd`, `senior_risk_capital_usd`, the internal/external/tokenized "
            "splits, the utilization ratios and `exposure_share`) are populated only in this mode. "
            "**Every figure becomes prime-scoped**, because the monitor reports per prime: the "
            "unprefixed fields carry the same values as their `prime_` counterparts, so a client "
            "fanning out across a prime's proxies must dedupe rather than sum. "
            "Returns `404` when the monitor does not track the prime, and `502` when it cannot be read "
            "— the two are held apart so an outage is never served as an absence of exposure."
        ),
    ),
    service: PrimeRiskCapitalService = Depends(_get_service),
    reference_services: Callable[[], ReferenceRiskCapitalService] = Depends(get_reference_risk_capital_service_factory),
) -> PrimeRiskCapitalResponse:
    # A SubProxy holds the prime's treasury, not its allocations, so it is not a
    # member of the prime's ALM fan-out set. Answering for one folds the treasury
    # into prime_exposure_usd and adds a chain-less prime_per_chain row, so the
    # prime_ fields stop being identical across the prime's proxies — the one
    # guarantee consumers are told to dedupe on. Checked before prime_exists: a
    # SubProxy does have allocation_position rows, so that gate cannot catch it,
    # and classify_proxy is an in-memory lookup that saves the round trip.
    if classify_proxy(prime_id) is ProxyKind.SUB_PROXY:
        raise HTTPException(
            status_code=404,
            detail=(
                "SubProxy treasury wallets carry no prime-level risk capital; "
                "query one of the prime's ALM proxies, or /total-capital for the treasury"
            ),
        )

    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    if reference:
        return await _reference_response(prime_address, reference_services())
    return _self_response(await service.compute(prime_address))


def _self_response(result: PrimeRiskCapital) -> PrimeRiskCapitalResponse:
    """Project STL's own model output onto the response."""
    return PrimeRiskCapitalResponse(
        source="self",
        prime_id=result.proxy_address,
        proxy_address=result.proxy_address,
        model=result.model,
        exposure_usd=result.exposure_usd,
        total_risk_capital_usd=result.total_risk_capital_usd,
        required_risk_capital_usd=result.required_risk_capital_usd,
        encumbrance_ratio=result.encumbrance_ratio,
        modeled_exposure_usd=result.modeled_exposure_usd,
        modeled_pct=result.modeled_pct,
        per_allocation=[AllocationRiskCapitalResponse(**alloc.__dict__) for alloc in result.per_allocation],
        prime_name=result.prime_name,
        prime_exposure_usd=result.prime_exposure_usd,
        prime_required_risk_capital_usd=result.prime_required_risk_capital_usd,
        prime_modeled_exposure_usd=result.prime_modeled_exposure_usd,
        prime_modeled_pct=result.prime_modeled_pct,
        prime_encumbrance_ratio=result.prime_encumbrance_ratio,
        prime_proxies=list(result.prime_proxies),
        prime_per_chain=[ChainRiskCapitalResponse(**row.__dict__) for row in result.prime_per_chain],
        prime_unserved_chains=list(result.prime_unserved_chains),
    )


async def _reference_response(
    prime_address: EthAddress, reference_service: ReferenceRiskCapitalService
) -> PrimeRiskCapitalResponse:
    """Project the upstream Star monitor's snapshot onto the same response."""
    try:
        snapshot = await reference_service.get(prime_address)
    except ReferenceDataUnavailableError as exc:
        raise HTTPException(status_code=502, detail=f"Reference risk-capital upstream unavailable: {exc}") from exc

    if snapshot is None:
        raise HTTPException(
            status_code=404,
            detail="The upstream Star monitor does not track this prime, so it has no reference risk capital",
        )
    return _project_reference(prime_address, snapshot)


def _project_reference(prime_address: EthAddress, snapshot: ReferencePrimeRiskCapital) -> PrimeRiskCapitalResponse:
    """Map an upstream snapshot onto the response, prime-scoped fields included.

    Upstream reports per prime, so the proxy-scoped and `prime_`-scoped figures
    carry the same values here — unlike self mode, where they genuinely differ.
    `prime_per_chain` stays empty because upstream publishes no proxy topology,
    so there is no per-proxy split to audit the total against.
    """
    # Upstream publishes only positions it has already priced, so its coverage
    # is complete by construction. Deriving a ratio from its own two endpoints
    # would land at ~1.0 with a ~1e-6 wobble that can exceed the documented
    # 0-1 range, and would read as "STL priced all of this" — which no model did.
    return PrimeRiskCapitalResponse(
        source="reference",
        prime_id=str(prime_address),
        proxy_address=str(prime_address),
        model=None,
        exposure_usd=snapshot.exposure_usd,
        total_risk_capital_usd=snapshot.total_risk_capital_usd,
        required_risk_capital_usd=snapshot.required_risk_capital_usd,
        encumbrance_ratio=snapshot.encumbrance_ratio,
        modeled_exposure_usd=snapshot.exposure_usd,
        modeled_pct=Decimal("1"),
        per_allocation=[_reference_allocation(row) for row in snapshot.per_allocation],
        prime_name=snapshot.star,
        prime_exposure_usd=snapshot.exposure_usd,
        prime_required_risk_capital_usd=snapshot.required_risk_capital_usd,
        prime_modeled_exposure_usd=snapshot.exposure_usd,
        prime_modeled_pct=Decimal("1"),
        prime_encumbrance_ratio=snapshot.encumbrance_ratio,
        prime_proxies=[entry.address for entry in alm_proxies_for_prime(snapshot.star)],
        prime_per_chain=[],
        prime_unserved_chains=[],
        junior_risk_capital_usd=snapshot.junior_risk_capital_usd,
        senior_risk_capital_usd=snapshot.senior_risk_capital_usd,
        internal_junior_risk_capital_usd=snapshot.internal_junior_risk_capital_usd,
        external_junior_risk_capital_usd=snapshot.external_junior_risk_capital_usd,
        tokenized_junior_risk_capital_usd=snapshot.tokenized_junior_risk_capital_usd,
        internal_senior_risk_capital_usd=snapshot.internal_senior_risk_capital_usd,
        external_senior_risk_capital_usd=snapshot.external_senior_risk_capital_usd,
        epi_utilization=snapshot.epi_utilization,
        spj_utilization=snapshot.spj_utilization,
        exposure_share=snapshot.exposure_share,
    )


def _reference_allocation(row: ReferenceAllocation) -> AllocationRiskCapitalResponse:
    return AllocationRiskCapitalResponse(
        receipt_token_id=row.receipt_token_id,
        symbol=row.symbol,
        protocol_name=row.protocol_name,
        exposure_usd=row.exposure_usd,
        applied=True,
        required_risk_capital_usd=row.required_risk_capital_usd,
        crr_pct=row.crr_pct,
        model=None,
        unpriced_reason=None,
        chain=row.chain,
        token_address=row.token_address,
        loan_token_symbol=row.loan_token_symbol,
    )


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    return numerator / denominator if denominator else None
