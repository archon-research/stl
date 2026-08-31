import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import ProxyAddressPathParam
from app.api.deps import get_engine, get_model_registry, get_reference_risk_capital_service_factory, require_prime_view
from app.api.provenance import (
    get_requested_provenance,
    resolve_or_422,
)
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, PrimeRiskCapital, UnpricedReason
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.domain.position_identity import PositionFacts, position_identities
from app.domain.prime_registry import ProxyKind, alm_proxies_for_prime, classify_proxy
from app.domain.provenance import Provenance
from app.domain.serialization import PlainDecimal
from app.services.model_registry import ModelRegistry
from app.services.prime_risk_capital_service import PrimeRiskCapitalService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["primes", "capital"])


class AllocationRiskCapitalResponse(BaseModel):
    """Per-allocation risk capital from the default model, or from the reference feed."""

    receipt_token_id: int | None = Field(
        description=(
            "Surrogate id of the receipt token. Always set for an indexed row. For a Sky-reported one it is "
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
            "Whether the figure is priced. Always `true` for a Sky-reported row: the upstream monitor "
            "reports only positions it has already priced."
        )
    )
    required_risk_capital_usd: PlainDecimal | None = Field(
        default=None, description="Per-allocation RRC (USD). `null` when the allocation is unpriced."
    )
    source: Provenance = Field(
        default=Provenance.INDEXED,
        description=(
            "Which provenance reported this position's figures. Under `source=both` a position both "
            "report keeps STL's, with Sky's in `reference_*`."
        ),
    )
    reference_exposure_usd: PlainDecimal | None = Field(
        default=None,
        description="Sky's exposure for this position. Populated only under `source=both`.",
    )
    reference_required_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description="Sky's requirement for this position. Populated only under `source=both`.",
    )
    position_keys: list[str] = Field(
        default_factory=list,
        description=(
            "Keys this position answers to, strongest first, computed the same way as the allocations "
            "endpoint's. Two rows describe the same position when they share any one of them, which is "
            "how a client attaches this row's figures to an allocation: a position Sky reports and STL "
            "does not index has no `receipt_token_id` to join by. Opaque — the spelling is not a "
            "contract, only the equality is."
        ),
        examples=[["custody:anchorage"]],
    )
    reference_crr_pct: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sky's comparable capital-risk ratio for this position (0-100). Populated only under "
            "`source=both`. Carried rather than derived from the two figures above: it is upstream's "
            "own ratio, and a consumer dividing them would publish a number Sky does not."
        ),
    )
    encumbrance_contribution: PlainDecimal | None = Field(
        default=None,
        description=(
            "This position's share of the prime's encumbrance: its required risk capital over the "
            "prime's *total* risk capital. Summing the column gives the prime's encumbrance ratio. "
            "Attributed rather than decomposed — risk capital is held by the prime, not the "
            "position, so only the numerator is per-position."
        ),
    )
    crr_pct: PlainDecimal | None = Field(
        default=None,
        description=(
            "Comparable capital-risk ratio (0-100). `null` when the allocation is unpriced. Under "
            "`source=reference` this is upstream's `crr` rescaled from its native 0-1 fraction, so the "
            "scale matches self mode."
        ),
    )
    model: str | None = Field(
        default=None,
        description=(
            "Model that produced the figure. `null` when unpriced, and always `null` for a Sky-reported "
            "row, which runs no model."
        ),
    )
    chain: str | None = Field(
        default=None,
        description=(
            "Internal chain name the position sits on. Reference-only: `null` in self mode, and `null` "
            "for a Sky-reported row on a network STL has no chain id for."
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
    source: Provenance = Field(
        default=Provenance.INDEXED,
        description=(
            "Provenance of the figures in this response. `indexed` is STL's own on-chain model; "
            "`reference` is Sky's Star Agents Risk Capital & Requirements Monitor as STL observed "
            "it; `both` carries the two side by side, STL's in the unprefixed fields and Sky's in "
            "the `reference_`-prefixed ones. Never reconciled: no field holds a blend of the two, "
            "and `both` degrades to `indexed` — reporting itself as such — for a prime no reference "
            "cycle has ever reported on."
        ),
    )
    model: str | None = Field(
        description=(
            "The default RRC model this view prefers (`core_model`). `null` under `source=reference`, "
            "which runs no model; under `source=both` it is STL's preference, since the unprefixed "
            "figures are STL's. A given `per_allocation` row can still carry a different model: "
            "`indexed` falls back to `gap_sweep` for a position `core_model` has no data for."
        ),
        examples=["core_model"],
    )
    exposure_usd: PlainDecimal = Field(
        description=(
            "Σ priced receipt-token allocation exposure (USD). Under `source=reference` this is upstream's "
            "own total, which deliberately does not equal the sum of `per_allocation` — the two come "
            "from separately-computed snapshots and reconcile only to about 1e-6."
        )
    )
    total_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "On-chain SubProxy treasury balance (USD). `null` when absent. Under `source=reference` this "
            "is upstream's Total Risk Capital, which is neither on-chain nor a treasury balance."
        ),
    )
    required_risk_capital_usd: PlainDecimal = Field(
        description=(
            "Σ per-allocation RRC from the default model (USD). Under `source=reference` this is upstream's "
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
            "Exposure the default model could price (USD). Under `source=reference` it equals "
            "`exposure_usd`: the monitor publishes only positions it has already priced."
        )
    )
    modeled_pct: PlainDecimal | None = Field(
        default=None, description="`modeled_exposure_usd / exposure_usd` (0-1). `null` when exposure is zero."
    )
    per_allocation: list[AllocationRiskCapitalResponse] = Field(
        description=(
            "Per-allocation breakdown, largest exposure first. Under `source=both` the merged rows are "
            "re-sorted, so a row's position reflects STL's exposure where it has one and Sky's otherwise."
        )
    )
    prime_name: str | None = Field(
        default=None,
        description="Prime this proxy belongs to. `null` for a proxy absent from the axis-synome contract.",
        examples=["spark"],
    )
    reference_prime_exposure_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sky's reported exposure for the prime, populated only under `source=both`. Beside "
            "STL's rather than replacing it: STL prices only the chains it indexes, so the two "
            "differ by that coverage and the gap is the point."
        ),
    )
    reference_prime_required_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description="Sky's reported required risk capital. Populated only under `source=both`.",
    )
    reference_total_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description="Sky's reported total risk capital. Populated only under `source=both`.",
    )
    reference_prime_encumbrance_ratio: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sky's reported encumbrance, its own required over its own total. Populated only under "
            "`source=both`; never a ratio built from one provenance over the other."
        ),
    )
    reference_synced_at: datetime | None = Field(
        default=None,
        description=(
            "When the Sky figures in this response were observed. Populated wherever the response "
            "carries them (`source=reference` or `source=both`), and `null` under `source=indexed`. "
            "STL reads them from its own record of the monitor rather than the monitor itself, so "
            "they are as of the last sync cycle — up to 15 minutes old. Consumers should show this "
            "rather than implying the figures are current."
        ),
        examples=["2026-08-26T09:15:00+00:00"],
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
            "totals are a lower bound. Always empty under `source=reference`: upstream's totals are not "
            "bounded by what STL indexes, so the caveat does not apply to them."
        ),
        examples=[["arbitrum", "optimism", "unichain"]],
    )
    junior_risk_capital_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Junior (first-loss) risk capital (USD). Reference-only — `null` unless the response carries "
            "Sky's figures (`source=reference` or `source=both`). "
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
        "(`core_model`, falling back to `gap_sweep` under `source=indexed` where core has no data), "
        "with no dependency on the upstream Star feed. Returns exposure (priced "
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
    requested_provenance: Provenance | None = Depends(get_requested_provenance),
    service: PrimeRiskCapitalService = Depends(_get_service),
    reference_services: Callable[[], ReferenceRiskCapitalService] = Depends(get_reference_risk_capital_service_factory),
    _authz: None = Depends(require_prime_view),
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

    source = resolve_or_422(requested_provenance, available=frozenset(Provenance), default=Provenance.INDEXED)

    if source is Provenance.REFERENCE:
        return _with_encumbrance_contributions(await _reference_response(prime_address, reference_services()))

    indexed = _self_response(await service.compute(prime_address, source))
    if source is Provenance.INDEXED:
        return _with_encumbrance_contributions(indexed)

    merged = await _with_reference_totals(prime_address, indexed, reference_services())
    return _with_encumbrance_contributions(merged)


def _with_encumbrance_contributions(
    response: PrimeRiskCapitalResponse,
) -> PrimeRiskCapitalResponse:
    """Attribute each position a share of the prime's encumbrance.

    The denominator is the prime's whole risk capital, the same for every row, so
    the column sums to the prime's own ratio. A zero or absent total leaves the
    column null rather than dividing by it.
    """
    total = response.total_risk_capital_usd
    if total is None or total == 0:
        return response

    return response.model_copy(
        update={
            "per_allocation": [
                allocation.model_copy(
                    update={
                        "encumbrance_contribution": (
                            None
                            # Under `both` the denominator is STL's own total, so a
                            # row Sky alone reports (source=reference there) has no
                            # comparable share of it — leaving it in would stop the
                            # column summing to the ratio. A pure `source=reference`
                            # response has no STL half to be incomparable with: every
                            # row is `reference` there, against Sky's own total, so
                            # the exclusion does not apply.
                            if allocation.required_risk_capital_usd is None
                            or (response.source is Provenance.BOTH and allocation.source is Provenance.REFERENCE)
                            else allocation.required_risk_capital_usd / total
                        )
                    }
                )
                for allocation in response.per_allocation
            ]
        }
    )


def _merge_per_allocation(
    indexed: PrimeRiskCapitalResponse, reference: PrimeRiskCapitalResponse
) -> list[AllocationRiskCapitalResponse]:
    """One row per position, from either provenance, keyed the same way the
    allocation union is.

    A position both report keeps STL's figures with Sky's beside them: STL's are
    computed from the chain, and the two differ by STL's chain coverage.
    """

    def facts(row: AllocationRiskCapitalResponse) -> PositionFacts:
        return PositionFacts(
            chain_id=None,
            network=row.chain,
            position_address=row.token_address,
            receipt_token_id=row.receipt_token_id,
            protocol_name=row.protocol_name,
            symbol=row.symbol,
        )

    by_identity: dict[str, AllocationRiskCapitalResponse] = {}
    for row in indexed.per_allocation:
        for key in position_identities(facts(row)):
            by_identity.setdefault(key, row)

    merged: list[AllocationRiskCapitalResponse] = []
    matched: set[int] = set()
    for row in reference.per_allocation:
        counterpart = next(
            (by_identity[key] for key in position_identities(facts(row)) if key in by_identity),
            None,
        )
        if counterpart is None:
            merged.append(row.model_copy(update={"source": Provenance.REFERENCE}))
            continue
        matched.add(id(counterpart))
        merged.append(
            counterpart.model_copy(
                update={
                    "source": Provenance.BOTH,
                    "reference_exposure_usd": row.exposure_usd,
                    "reference_required_risk_capital_usd": row.required_risk_capital_usd,
                    "reference_crr_pct": row.crr_pct,
                }
            )
        )

    merged.extend(row for row in indexed.per_allocation if id(row) not in matched)
    # Both halves arrive ordered by their own exposure, so concatenating them
    # yields neither order. `per_allocation` is published as largest-exposure
    # first and a consumer paginating or truncating it reads the wrong rows
    # otherwise.
    return sorted(merged, key=lambda row: row.exposure_usd, reverse=True)


async def _with_reference_totals(
    prime_address: EthAddress,
    indexed: PrimeRiskCapitalResponse,
    reference_service: ReferenceRiskCapitalService,
) -> PrimeRiskCapitalResponse:
    """STL's model, plus Sky's totals in their own fields.

    The two cannot share a field: they populate disjoint sets — Sky's junior and
    senior splits have no STL equivalent, STL's model name has no Sky one — and
    the figures they do share disagree by STL's chain coverage. A prime with no
    reference figures at all leaves STL's own answer whole rather than failing
    it; every other outcome is an error, and surfaces as one.
    """
    try:
        reference = await _reference_response(prime_address, reference_service)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        logger.info(
            "Serving STL's risk capital alone; no reference cycle has reported on this prime",
            extra={"prime_address": str(prime_address)},
        )
        return indexed

    return indexed.model_copy(
        update={
            "source": Provenance.BOTH,
            "per_allocation": _merge_per_allocation(indexed, reference),
            "reference_prime_exposure_usd": reference.prime_exposure_usd,
            "reference_prime_required_risk_capital_usd": reference.prime_required_risk_capital_usd,
            "reference_total_risk_capital_usd": reference.total_risk_capital_usd,
            "reference_prime_encumbrance_ratio": reference.prime_encumbrance_ratio,
            "reference_synced_at": reference.reference_synced_at,
            # Sky reports these and STL models none of them, so they belong to
            # the merged answer whole.
            "junior_risk_capital_usd": reference.junior_risk_capital_usd,
            "senior_risk_capital_usd": reference.senior_risk_capital_usd,
            "exposure_share": reference.exposure_share,
        }
    )


def _self_response(result: PrimeRiskCapital) -> PrimeRiskCapitalResponse:
    """Project STL's own model output onto the response."""
    return PrimeRiskCapitalResponse(
        source=Provenance.INDEXED,
        prime_id=result.proxy_address,
        proxy_address=result.proxy_address,
        model=result.model,
        exposure_usd=result.exposure_usd,
        total_risk_capital_usd=result.total_risk_capital_usd,
        required_risk_capital_usd=result.required_risk_capital_usd,
        encumbrance_ratio=result.encumbrance_ratio,
        modeled_exposure_usd=result.modeled_exposure_usd,
        modeled_pct=result.modeled_pct,
        per_allocation=[_indexed_allocation(alloc) for alloc in result.per_allocation],
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
    """Project STL's stored Star-monitor snapshot onto the same response."""
    snapshot = await reference_service.get(prime_address)
    if snapshot is None:
        raise HTTPException(
            status_code=404,
            detail="No reference risk capital has been observed for this prime",
        )
    return _project_reference(prime_address, snapshot)


def _project_reference(prime_address: EthAddress, snapshot: ReferencePrimeRiskCapital) -> PrimeRiskCapitalResponse:
    """Map an observed snapshot onto the response, prime-scoped fields included.

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
        source=Provenance.REFERENCE,
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
        reference_synced_at=snapshot.synced_at,
    )


def _indexed_allocation(allocation: AllocationRiskCapital) -> AllocationRiskCapitalResponse:
    """STL's own row, keyed by the registry id it always carries."""
    return AllocationRiskCapitalResponse(
        **allocation.__dict__,
        position_keys=position_identities(
            PositionFacts(
                chain_id=None,
                network=None,
                position_address=None,
                receipt_token_id=allocation.receipt_token_id,
                protocol_name=allocation.protocol_name,
                symbol=allocation.symbol,
            )
        ),
    )


def _reference_allocation(row: ReferenceAllocation) -> AllocationRiskCapitalResponse:
    return AllocationRiskCapitalResponse(
        # From the entity, not the projected row: the response carries no chain
        # id, and keying on the network name where the allocations endpoint keys
        # on the number would put the same position under two keys.
        position_keys=position_identities(
            PositionFacts(
                chain_id=row.chain_id,
                network=row.network,
                position_address=row.token_address,
                receipt_token_id=row.receipt_token_id,
                protocol_name=row.protocol_name,
                symbol=row.symbol,
            )
        ),
        receipt_token_id=row.receipt_token_id,
        symbol=row.symbol,
        protocol_name=row.protocol_name,
        exposure_usd=row.exposure_usd,
        applied=True,
        required_risk_capital_usd=row.required_risk_capital_usd,
        source=Provenance.REFERENCE,
        crr_pct=row.crr_pct,
        model=None,
        unpriced_reason=None,
        chain=row.chain,
        token_address=row.token_address,
        loan_token_symbol=row.loan_token_symbol,
    )


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    return numerator / denominator if denominator else None
