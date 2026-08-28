import asyncio
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import (
    OptionalEthAddressParam,
    OptionalTxHashParam,
    ProxyAddressPathParam,
)
from app.api.deps import get_engine, get_reference_positions_service_factory
from app.api.provenance import (
    get_requested_provenance,
    resolve_or_422,
)
from app.api.time_series import TimeSeriesWindow, apply_cache_control, build_window, get_time_series_query_params
from app.domain.entities.allocation import (
    AnchorageCustodyHolding,
    DirectAssetHolding,
    EthAddress,
    ReceiptTokenPosition,
    as_address,
)
from app.domain.entities.allocation_category import AllocationCategory
from app.domain.entities.reference_position import ReferencePosition
from app.domain.position_identity import PositionFacts, position_identities
from app.domain.provenance import Provenance
from app.domain.serialization import PlainDecimal
from app.domain.time_series import TimeSeriesQuery, enforce_filter_for_window
from app.services.allocation_category_service import AllocationCategoryService
from app.services.allocation_service import AllocationService
from app.services.reference_positions_service import ReferencePositionsService

logger = logging.getLogger(__name__)
router = APIRouter()


class PrimeResponse(BaseModel):
    """One of a prime's proxy wallets tracked by STL.

    A prime allocates through one ALM proxy per chain, so `name` repeats across
    rows and is not a key. Use `/v1/primes/{address}/risk-capital` for
    prime-level figures — it aggregates across a prime's proxies regardless of
    which one you address it by.
    """

    id: str = Field(
        deprecated=True,
        description=(
            "DEPRECATED — despite the name this is the ALM **proxy** address, not a prime "
            "identifier, and it is byte-identical to `address` in the same row. Its value is "
            "unchanged for backwards compatibility. Use `address` to address a proxy and "
            "`prime_vault_address` (or `name`) to group rows by prime."
        ),
        examples=["0x1601843c5e9bc251a3272907010afa41fa18347e"],
    )
    name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    address: str = Field(
        description="0x-prefixed Ethereum address controlled by the prime.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    chain_id: int = Field(description="EVM chain id this proxy holds positions on.", examples=[43114])
    chain: str | None = Field(
        default=None,
        description="Internal chain name derived from `chain_id`. `null` for an untaught chain id.",
        examples=["avalanche-c"],
    )
    role: Literal["alm"] = Field(
        description=(
            "Always `alm`: this endpoint lists allocation venues only. SubProxy treasury wallets share "
            "a prime's `prime_id` but hold no allocations, so they are excluded rather than labelled."
        ),
        examples=["alm"],
    )
    prime_vault_address: str | None = Field(
        default=None,
        description=(
            "The owning prime's on-chain vault address — identical across every proxy of a "
            "prime, so consumers group rows by it. Prime-scoped: dedupe, never sum."
        ),
        examples=["0x691a6c29e9e96dd897718305427ad5d534db16ba"],
    )


class ChainResponse(BaseModel):
    """An EVM chain that STL tracks data for."""

    chain_id: int = Field(description="EVM chain id.", examples=[1])
    name: str = Field(description="Human-readable chain name.", examples=["Ethereum Mainnet"])


class ProtocolResponse(BaseModel):
    """A protocol (lender, AMM, etc.) that STL classifies positions against."""

    id: int = Field(description="Surrogate protocol id.", examples=[7])
    chain_id: int = Field(description="EVM chain id the protocol instance lives on.", examples=[1])
    encode: str = Field(
        description="Machine-readable protocol code used in joins (`<name>-<version>`).",
        examples=["aave-v3"],
    )
    name: str = Field(description="Human-readable protocol name.", examples=["Aave v3"])


class AllocationResponse(BaseModel):
    """Enriched allocation response with category and metadata.

    Three row shapes share this model:
    - Receipt-token positions (e.g. spUSDT wrapping USDT): all fields populated.
    - Direct asset holdings (e.g. PYUSD held in the proxy with no wrapper):
      ``receipt_token_id`` / ``receipt_token_address`` / ``protocol_name`` are
      null; ``symbol`` and ``held_token_address`` name the held asset.
      ``underlying_*`` usually point at the held asset itself, except holdings
      valued on the underlying-value basis (allowlisted, e.g. a Uni V3 pool
      position valued in USDC) with a resolvable underlying, where they point
      at that underlying.
      ``amount_usd`` is populated when an oracle price exists for the pricing
      basis and null otherwise (e.g. LP/curve shares with no oracle feed).
    - Off-chain custody holdings (Anchorage BTC): ``chain_id`` is 0 (the
      off-chain sentinel), ``protocol_name`` is ``anchorage``, ``symbol`` is the
      custodied asset (BTC), and both ``underlying_token_id`` and
      ``underlying_token_address`` are null (off-chain assets have no token-
      registry row). ``amount_usd`` is the loan drawn against the collateral and
      ``latest_activity_at`` is the snapshot time — surfaced verbatim even when
      the upstream feed is frozen, so staleness is visible rather than hidden.

    ``chain_id`` therefore has three states, and 0 is not one of the other two:
    an EVM chain id, 0 for off-chain custody, and null for a chain STL has no id
    for (reference rows only, where ``network`` carries the upstream name).
    """

    chain_id: int | None = Field(
        description=(
            "EVM chain id of the position. `0` for off-chain custody. `null` when the position "
            "is on a chain STL has no id for, which only happens on reference rows — read "
            "`network` for the label in that case."
        ),
        examples=[1],
    )
    position_keys: list[str] = Field(
        default_factory=list,
        description=(
            "Keys this position answers to, strongest first. Two rows describe the same position when "
            "they share any one of them, which is how a client joins this row to its risk-capital "
            "counterpart: the two endpoints do not carry the same kind of identifier, so a position "
            "Sky reports and STL does not index has no `receipt_token_id` to join by. Opaque — the "
            "spelling is not a contract, only the equality is."
        ),
        examples=[["token:736", "position:1:0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359"]],
    )
    source: Provenance = Field(
        default=Provenance.INDEXED,
        description=(
            "Which provenance reported this position. `both` means the two agreed it exists and "
            "the figures shown are STL's; `reference` means only Sky reports it, which is either a "
            "position STL does not index or one on a chain it does not serve."
        ),
    )
    network: str | None = Field(
        default=None,
        description=(
            "The upstream feed's own name for the chain, e.g. `plume`. Populated on reference "
            "rows only, and the sole label available when `chain_id` is `null`."
        ),
        examples=["ethereum"],
    )
    wallet_address: str | None = Field(
        default=None,
        description=(
            "The ALM proxy holding this position, as upstream reports it. Populated on reference "
            "rows only — the same (`network`, `receipt_token_address`/`held_token_address`) can "
            "legitimately recur under a prime's different proxy wallets, and this is what "
            "distinguishes those rows. `null` on an indexed row, which is already scoped to a "
            "single queried proxy."
        ),
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    receipt_token_id: int | None = Field(
        default=None,
        description="Surrogate id of the receipt token. `null` for direct asset holdings.",
        examples=[42],
    )
    receipt_token_address: str | None = Field(
        default=None,
        description="0x-prefixed receipt-token contract address. `null` for direct asset holdings.",
        examples=["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
    )
    held_token_address: str | None = Field(
        default=None,
        description=(
            "0x-prefixed address of the token held in the proxy, on a direct asset holding. It names "
            "what the position *is*, unlike `underlying_token_address`, which names the token the "
            "holding is *priced* through — a different asset wherever a wrapper is valued through "
            "the token it wraps. `null` on receipt-token positions, where `receipt_token_address` "
            "already names the held token, and on off-chain custody holdings, which have no on-chain "
            "address."
        ),
        examples=["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
    )
    underlying_token_id: int | None = Field(
        default=None,
        description=(
            "Surrogate id of the underlying token. For direct holdings, this is the held asset itself, "
            "unless the holding is valued on the underlying-value basis (allowlisted). `null` for off-chain "
            "custody holdings (e.g. Anchorage BTC), which have no token-registry row, and for a reference "
            "row (`source=reference`) whose position does not resolve to STL's receipt-token registry."
        ),
        examples=[1],
    )
    underlying_token_address: str | None = Field(
        default=None,
        description=(
            "0x-prefixed underlying-token contract address. For direct holdings, this is the held asset itself, "
            "unless the holding is valued on the underlying-value basis (allowlisted). `null` for off-chain "
            "custody holdings (e.g. Anchorage BTC), which have no on-chain address, and for a reference row "
            "(`source=reference`) whose position does not resolve to STL's receipt-token registry."
        ),
        examples=["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"],
    )
    symbol: str = Field(
        description="Display symbol: receipt-token symbol for wrapped positions, asset symbol for direct holdings.",
        examples=["aUSDC"],
    )
    underlying_symbol: str = Field(
        description=(
            "Underlying-token symbol. For direct holdings, same as ``symbol``, "
            "unless the holding is valued on the underlying-value basis (allowlisted). Empty on a reference "
            "row (`source=reference`) whose position does not resolve to STL's receipt-token registry — "
            "that feed never names an underlying of its own — and also empty on a resolved reference row "
            "whose registry token has no symbol recorded yet; `underlying_token_id`/`underlying_token_address` "
            "are the reliable resolution signal, not this field."
        ),
        examples=["USDC"],
    )
    protocol_name: str | None = Field(
        default=None,
        description="Protocol the position is held in. `null` for direct holdings (no registered wrapper).",
        examples=["aave-v3"],
    )
    balance: PlainDecimal | None = Field(
        description=(
            "Balance held by the prime, in token units. Decimal serialized as a JSON string. "
            "Always present for an indexed row. Always `null` for a Sky-reported one: the upstream Star "
            "monitor reports USD exposure only and never a token quantity, so there is no balance "
            "to report — read `amount_usd` instead."
        ),
        examples=["1234567.89"],
    )
    amount_usd: PlainDecimal | None = Field(
        default=None,
        description="USD value of the position when a price is available; `null` otherwise.",
        examples=["1234567.89"],
    )
    reference_amount_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sky's USD value for the same position, populated only under `source=both` on a row "
            "both provenances report. Carried beside `amount_usd` rather than replacing it: the "
            "two are computed differently and a consumer needs the gap shown rather than "
            "reconciled. It is also the only figure available where STL holds the position but "
            "prices none of it — an unindexed chain leaves `amount_usd` null against a real "
            "`balance`, and Sky's figure is what a total can fall back to."
        ),
        examples=["1234567.89"],
    )
    reference_synced_at: datetime | None = Field(
        default=None,
        description=(
            "When Sky's figures for this row were observed. Populated on any row carrying them — "
            "a `reference` row, or a `both` row's `reference_amount_usd` — and `null` on an "
            "indexed-only row. STL reads them from its own record of the feed rather than the feed "
            "itself, so they are as of the last sync cycle, up to 15 minutes old. Consumers should "
            "show this rather than implying the figures are current."
        ),
        examples=["2026-08-26T09:15:00+00:00"],
    )
    latest_activity_at: str | None = Field(
        default=None,
        description="ISO-8601 timestamp of the most recent on-chain activity for this position, or `null`.",
        examples=["2026-05-07T12:00:00Z"],
    )
    latest_activity_action: str | None = Field(
        default=None,
        description="Direction of the most recent activity (`in`, `out`, `sweep`), or `null`.",
        examples=["out"],
    )
    latest_activity_amount: PlainDecimal | None = Field(
        default=None,
        description=(
            "Token-unit magnitude of the most recent activity (unsigned). Decimal serialized as a "
            "JSON string. `null` when there is no activity."
        ),
        examples=["12.5"],
    )
    category: AllocationCategory = Field(
        description=(
            "Allocation category derived from protocol/symbol (`allocation`, `pol`, `psm3`, `asset`, `custody`)."
        ),
    )
    scope: Literal["proxy", "prime"] = Field(
        default="proxy",
        description=(
            "Whether the row belongs to the queried proxy (`proxy`) or to the prime as a whole (`prime`). "
            "A `prime`-scoped row is served under the prime's primary proxy only, so unioning a prime's "
            "proxies never double-counts it."
        ),
        examples=["proxy"],
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "chain_id": 1,
                "receipt_token_id": 42,
                "receipt_token_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "underlying_token_id": 1,
                "underlying_token_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "symbol": "aUSDC",
                "underlying_symbol": "USDC",
                "protocol_name": "aave-v3",
                "balance": "1234567.89",
                "amount_usd": "1234567.89",
                "latest_activity_at": "2026-05-07T12:00:00Z",
                "latest_activity_action": "out",
                "latest_activity_amount": "12.5",
                "category": "allocation",
                "scope": "proxy",
            }
        }
    }

    @model_validator(mode="after")
    def _check_underlying_identity_pairing(self) -> "AllocationResponse":
        # The underlying id and address are two halves of one identity: a receipt
        # or direct row carries both, an off-chain custody row carries neither.
        # This response model previously typed both non-optional, so relaxing them
        # to Optional here (for the custody shape) reintroduces the risk of a
        # contradictory one-set-one-null row that the plain int/str fields used to
        # reject for free. Guard it, mirroring the both-or-neither pairing
        # validator on RrcResult (RrcResult._check_risk_model_details_pairing).
        if (self.underlying_token_id is None) != (self.underlying_token_address is None):
            raise ValueError(
                "underlying_token_id and underlying_token_address must be set or null together: "
                f"underlying_token_id={self.underlying_token_id!r}, "
                f"underlying_token_address={self.underlying_token_address!r}"
            )
        return self


class AllocationActivityResponse(BaseModel):
    """Allocation activity event record for timeline feeds."""

    chain_id: int = Field(description="EVM chain id where the event occurred.", examples=[1])
    prime_address: str = Field(
        description="0x-prefixed ALM proxy address the event occurred on.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    prime_name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    protocol_name: str | None = Field(
        default=None, description="Protocol the event was emitted by.", examples=["aave-v3"]
    )
    token_id: int = Field(description="Surrogate id of the receipt token involved.", examples=[42])
    token_symbol: str | None = Field(default=None, description="Receipt-token symbol, when known.", examples=["aUSDC"])
    action_type: str = Field(description="One of `in`, `out`, `sweep`.", examples=["in"])
    tx_amount: PlainDecimal = Field(
        description="Token-unit amount moved by this event. Decimal serialized as a JSON string.",
        examples=["1000.5"],
    )
    balance: PlainDecimal = Field(
        description="Resulting balance after the event, in token units.",
        examples=["1234567.89"],
    )
    tx_hash: str | None = Field(
        default=None,
        description="0x-prefixed transaction hash, when available.",
        examples=["0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"],
    )
    log_index: int = Field(description="Index of the originating log within the transaction.", examples=[3])
    block_number: int = Field(description="Block number containing the event.", examples=[18000000])
    block_version: int = Field(description="Cache-key version that increments on chain reorgs.", examples=[1])
    created_at: str = Field(description="ISO-8601 timestamp the event row was persisted.")


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(AllocationRepository(engine))


@router.get(
    "/primes",
    response_model=list[PrimeResponse],
    tags=["primes"],
    summary="List all primes",
    description=(
        "Return every ALM proxy of every prime tracked by STL, one row per proxy per chain, with its "
        "surrogate id, name, on-chain address, chain, and proxy role. A prime allocates through one ALM "
        "proxy per chain, so `name` repeats across rows and is not a key; group rows by "
        "`prime_vault_address` instead. Use `/v1/primes/{address}/risk-capital` for prime-level figures."
    ),
)
async def list_primes(service: AllocationService = Depends(_get_service)):
    primes = await service.list_primes()
    return [
        PrimeResponse(
            id=p.id,
            name=p.name,
            address=p.address,
            chain_id=p.chain_id,
            chain=p.chain,
            role=p.role,
            prime_vault_address=p.prime_vault_address,
        )
        for p in primes
    ]


@router.get(
    "/chains",
    response_model=list[ChainResponse],
    tags=["metadata"],
    summary="List supported chains",
    description="Return every EVM chain that STL tracks data for, for use as a filter value.",
)
async def list_chains(service: AllocationService = Depends(_get_service)):
    chains = await service.list_chains()
    return [ChainResponse(chain_id=chain.chain_id, name=chain.name) for chain in chains]


@router.get(
    "/protocols",
    response_model=list[ProtocolResponse],
    tags=["metadata"],
    summary="List supported protocols",
    description="Return every protocol/chain pair STL classifies positions against, for use as a filter value.",
)
async def list_protocols(service: AllocationService = Depends(_get_service)):
    protocols = await service.list_protocols()
    return [
        ProtocolResponse(
            id=protocol.id,
            chain_id=protocol.chain_id,
            encode=protocol.encode,
            name=protocol.name,
        )
        for protocol in protocols
    ]


@router.get(
    "/primes/{prime_id}/allocations",
    response_model=list[AllocationResponse],
    tags=["allocations"],
    summary="List a prime's current allocations",
    description=(
        "Return every current allocation held by the given prime — receipt-token "
        "positions (enriched with USD value when a price is available), direct asset "
        "holdings (tokens held in the proxy with no registered receipt-token wrapper, "
        "surfaced with `receipt_token_id`, `receipt_token_address` and `protocol_name` "
        "set to `null`, and `amount_usd` valued from the token's oracle price when one "
        "exists), and off-chain Anchorage BTC custody (chain_id 0, `protocol_name` "
        "`anchorage`, `amount_usd` the loan drawn against the collateral). Each row "
        "includes the latest activity timestamp and a derived `category` "
        "(`allocation` / `pol` / `psm3` / `asset` / `custody`). Rows are proxy-scoped "
        "except the Anchorage custody leg, which is prime-scoped and returned only "
        "under the one proxy of the prime that carries its prime-scoped rows (its mainnet proxy when "
        "indexed, else its lowest-addressed one) — see the `scope` field.\n\n"
        "Under `source=reference` (and the reference half of `source=both`) the rows are Sky's "
        "published balance sheet instead: every position the prime holds, prime-scoped, with "
        "`amount_usd` carrying upstream's `assets`. That is the same measurement as the indexed "
        "rows' `amount_usd`, so the two halves of `both` are comparable — deliberately not the Star "
        "monitor's risk-capital breakdown, whose `exposure` covers only the priced subset and runs "
        "about a third smaller. These rows carry no `balance` and no activity fields, which "
        "upstream does not publish, and a `reference_synced_at` naming the sync cycle they were "
        "observed at rather than implying they are current. `underlying_*` are populated when the "
        "position resolves to STL's receipt-token registry (the feed itself names no underlying) "
        "and `null`/empty otherwise."
    ),
)
async def list_allocations(
    prime_id: ProxyAddressPathParam,
    requested_provenance: Provenance | None = Depends(get_requested_provenance),
    service: AllocationService = Depends(_get_service),
    reference_services: Callable[[], ReferencePositionsService] = Depends(get_reference_positions_service_factory),
):
    """Return current allocations for ``prime_id``.

    Combines three sources:
    - Receipt-token positions (e.g. spUSDT wrapping USDT).
    - Direct asset holdings — tokens held in the proxy that are not
      registered as receipt-token wrappers (e.g. PYUSD, syrupUSDT). These
      rows have null ``receipt_token_*`` / ``protocol_name``; ``amount_usd``
      is valued from the token's oracle price when one exists, else null.
    - Off-chain Anchorage BTC custody — chain_id 0, ``protocol_name``
      ``anchorage``, null ``underlying_*`` (no token-registry row), with the
      loan drawn against the collateral as ``amount_usd``. Gated to the
      one proxy of the prime that carries its prime-scoped rows (see
      ``_custody_applies``).

    Errors:
    - 422 if ``prime_id`` is malformed.
    - 404 if ``prime_id`` is well-formed but no such prime exists.
    """
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    source = resolve_or_422(requested_provenance, available=frozenset(Provenance), default=Provenance.INDEXED)

    if source is Provenance.REFERENCE:
        return _with_position_keys(await _reference_allocations(prime_address, reference_services()))

    if source is Provenance.BOTH:
        return _with_position_keys(await _merged_allocations(prime_address, service, reference_services()))

    custody_applies = await _custody_applies(prime_address, service)
    positions, direct, custody = await asyncio.gather(
        service.list_receipt_token_positions(prime_address),
        service.list_direct_asset_holdings(prime_address),
        service.list_anchorage_custody_holdings(prime_address) if custody_applies else _no_custody(),
    )

    category_service = AllocationCategoryService()
    return _with_position_keys(
        [
            *(_receipt_token_row(position, category_service) for position in positions),
            *(_direct_asset_row(holding, category_service) for holding in direct),
            *(_anchorage_custody_row(holding, category_service) for holding in custody),
        ]
    )


def _with_position_keys(rows: list[AllocationResponse]) -> list[AllocationResponse]:
    """Publish each row's identity so a client can join it to its risk row.

    Filled here rather than at the four projections, so a new source of rows
    cannot ship without them.
    """
    return [row.model_copy(update={"position_keys": position_identities(_position_facts(row))}) for row in rows]


async def _custody_applies(prime_address: EthAddress, service: AllocationService) -> bool:
    """Whether this proxy carries the prime-scoped Anchorage custody leg.

    Serving the leg under every one of a prime's proxies would triple-count $250M
    of BTC for a consumer unioning them, so exactly one proxy carries it. The pick
    is resolved from ``allocation_position`` (see
    ``AllocationRepository.primary_proxy_address``) rather than from the
    axis-synome contract, because the contract cannot answer this safely in either
    direction: a prime whose mainnet ALM proxy has no rows yet would have the leg
    attributed to a proxy ``/v1/primes`` does not list, and a proxy the contract
    has not been told about yet — the state during a chain onboarding — would be
    treated as its own primary and serve a second copy.

    A prime with no resolvable proxy cannot happen after the ``prime_exists``
    gate above, so it is logged rather than silently dropping the leg.
    """
    primary = await service.primary_proxy_address(prime_address)
    if primary is None:
        logger.error(
            "No primary proxy resolved for a prime that exists; withholding the prime-scoped custody leg",
            extra={"prime_address": str(prime_address)},
        )
        return False
    return primary.lower() == str(prime_address).lower()


def _position_facts(row: AllocationResponse) -> PositionFacts:
    """Read a projected row back as the facts that identify its position.

    The receipt token where there is one, else the token actually held — never
    the underlying a position is priced through, which two different vaults can
    share. `sparkPrimeUSDC1` is held directly and priced through USDC: keyed on
    USDC's address it matched Sky's own plain-USDC row, which reports $0, so the
    merged row claimed Sky valued a $20.3M position at nothing while Sky's real
    row for it went unjoined.
    """
    return PositionFacts(
        chain_id=row.chain_id,
        network=row.network,
        position_address=row.receipt_token_address or row.held_token_address,
        receipt_token_id=row.receipt_token_id,
        protocol_name=row.protocol_name,
        symbol=row.symbol,
    )


async def _merged_allocations(
    prime_address: EthAddress,
    service: AllocationService,
    reference_service: ReferencePositionsService,
) -> list[AllocationResponse]:
    """Every position either provenance reports, each named once.

    The indexed half is resolved prime-wide here, unlike the proxy-scoped
    default: the reference half is reported per prime, and joining it against a
    single proxy's rows matches whatever that one chain happens to hold — for
    spark, 8 of 12 against its mainnet proxy and 0 against its Base one.

    A prime with no reference data at all leaves the indexed rows as they are.
    Every row states its own provenance, so an answer with nothing from Sky in
    it says so without an envelope to carry the notice. Every other outcome is
    an error, and surfaces as one.
    """
    indexed = await _prime_wide_indexed_allocations(prime_address, service)

    try:
        reference = await _reference_allocations(prime_address, reference_service)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        logger.info(
            "Serving indexed allocations alone; no reference cycle has reported on this prime",
            extra={"prime_address": str(prime_address)},
        )
        return indexed

    # Indexed by every key each row answers to, so a match on any one counts.
    by_identity: dict[str, AllocationResponse] = {}
    for row in indexed:
        for key in position_identities(_position_facts(row)):
            by_identity.setdefault(key, row)

    merged: list[AllocationResponse] = []
    matched: set[int] = set()
    for row in reference:
        counterpart = next(
            (by_identity[key] for key in position_identities(_position_facts(row)) if key in by_identity),
            None,
        )
        if counterpart is not None:
            matched.add(id(counterpart))
        if counterpart is None:
            merged.append(row.model_copy(update={"source": Provenance.REFERENCE}))
            continue
        # STL's own figures lead where both report a position: they are computed
        # from the chain rather than reported. The two disagree by ~1% on
        # exposure, which a consumer needs told rather than averaged away.
        #
        # Sky's value rides along rather than being dropped. STL prices only the
        # chains it indexes, so a position it holds on an unserved chain carries
        # a real balance and a null `amount_usd` — six of spark's rows, $423M —
        # and discarding Sky's figure left a consumer nothing to fall back to.
        merged.append(
            counterpart.model_copy(
                update={
                    "source": Provenance.BOTH,
                    "reference_amount_usd": row.amount_usd,
                    "reference_synced_at": row.reference_synced_at,
                }
            )
        )

    merged.extend(row for row in indexed if id(row) not in matched)
    return merged


async def _prime_wide_indexed_allocations(
    prime_address: EthAddress, service: AllocationService
) -> list[AllocationResponse]:
    """STL's rows for every proxy of the prime, not just the queried one."""
    proxies = await service.prime_proxy_addresses(prime_address)
    category_service = AllocationCategoryService()

    rows: list[AllocationResponse] = []
    for proxy in proxies:
        positions, direct = await asyncio.gather(
            service.list_receipt_token_positions(proxy),
            service.list_direct_asset_holdings(proxy),
        )
        rows.extend(_receipt_token_row(position, category_service) for position in positions)
        rows.extend(_direct_asset_row(holding, category_service) for holding in direct)

    # Prime-scoped, so it belongs to the union once however many proxies there
    # are — and ungated, unlike the proxy-scoped default. `_custody_applies`
    # answers "does *this* proxy carry the leg", which is the wrong question of a
    # response that already spans every proxy: asking it drops the leg entirely
    # whenever a non-primary proxy is the one queried. The read resolves the
    # prime from whichever proxy it is given, so any of them returns the leg.
    custody = await service.list_anchorage_custody_holdings(prime_address)
    rows.extend(_anchorage_custody_row(holding, category_service) for holding in custody)

    return rows


async def _reference_allocations(
    prime_address: EthAddress, reference_service: ReferencePositionsService
) -> list[AllocationResponse]:
    """List the positions upstream reported this prime holds.

    Sourced from Sky's balance-sheet feed rather than the Star monitor's
    risk-capital breakdown. The monitor answers a different question — the
    priced, risk-bearing subset — so serving it here put 11 rows summing to
    spark's `total_exposure` beside STL's 30 rows summing to its assets, two
    figures 1.5x apart in one column. The balance sheet is the same measurement
    STL's own rows carry, and reports every position rather than the priced ones.
    """
    snapshot = await reference_service.get(prime_address)
    if snapshot is None:
        raise HTTPException(
            status_code=404,
            detail="No reference allocations have been observed for this prime",
        )

    category_service = AllocationCategoryService()
    return [
        _reference_allocation_row(row, snapshot.synced_at, category_service).model_copy(
            update={"source": Provenance.REFERENCE}
        )
        for row in snapshot.positions
    ]


def _reference_allocation_row(
    row: ReferencePosition, synced_at: datetime, category_service: AllocationCategoryService
) -> AllocationResponse:
    """Project an observed upstream position onto the allocation model.

    A network STL has no chain id for yields a null ``chain_id``, with
    ``network`` naming it: upstream adds chains before STL indexes them, and 0
    is not available, since it already means off-chain custody.
    """
    return AllocationResponse(
        chain_id=row.chain_id,
        network=row.network,
        wallet_address=row.wallet_address,
        receipt_token_id=row.receipt_token_id,
        receipt_token_address=row.token_address if as_address(row.token_address) else None,
        # Unlike the Star monitor's breakdown, this feed names no loan token
        # itself; see ReferencePosition for where these come from instead.
        underlying_token_id=row.underlying_token_id,
        underlying_token_address=row.underlying_token_address,
        symbol=row.symbol,
        underlying_symbol=row.underlying_symbol,
        protocol_name=row.protocol_name,
        balance=None,
        # `assets`, the whole holding — the same measurement STL's own rows
        # carry, and the one the prime's `assets` total decomposes into.
        amount_usd=row.assets_usd,
        reference_synced_at=synced_at,
        latest_activity_at=None,
        latest_activity_action=None,
        latest_activity_amount=None,
        category=category_service.classify(row.protocol_name, row.symbol),
        scope="prime",
    )


def _receipt_token_row(
    position: ReceiptTokenPosition, category_service: AllocationCategoryService
) -> AllocationResponse:
    """Project a receipt-token position onto the allocation model."""
    return AllocationResponse(
        chain_id=position.chain_id,
        receipt_token_id=position.receipt_token_id,
        receipt_token_address=position.receipt_token_address,
        underlying_token_id=position.underlying_token_id,
        underlying_token_address=position.underlying_token_address,
        symbol=position.symbol,
        underlying_symbol=position.underlying_symbol,
        protocol_name=position.protocol_name,
        balance=position.balance,
        amount_usd=position.amount_usd,
        latest_activity_at=position.latest_activity_at.isoformat() if position.latest_activity_at else None,
        latest_activity_action=position.latest_activity_action,
        latest_activity_amount=position.latest_activity_amount,
        category=category_service.classify(position.protocol_name, position.symbol),
    )


def _direct_asset_row(holding: DirectAssetHolding, category_service: AllocationCategoryService) -> AllocationResponse:
    """Project a direct (unwrapped) token holding onto the allocation model.

    Underlying identity travels with the price basis; see
    ``_DIRECT_ASSET_HOLDINGS_SQL``.
    """
    underlying_id, underlying_address, underlying_symbol = _direct_underlying_identity(holding)
    return AllocationResponse(
        chain_id=holding.chain_id,
        receipt_token_id=None,
        receipt_token_address=None,
        held_token_address=holding.token_address,
        underlying_token_id=underlying_id,
        underlying_token_address=underlying_address,
        symbol=holding.symbol,
        underlying_symbol=underlying_symbol,
        protocol_name=None,
        balance=holding.balance,
        amount_usd=holding.amount_usd,
        latest_activity_at=holding.latest_activity_at.isoformat() if holding.latest_activity_at else None,
        latest_activity_action=holding.latest_activity_action,
        latest_activity_amount=holding.latest_activity_amount,
        category=category_service.classify(None, holding.symbol),
    )


# chain_id 0 is the off-chain sentinel: Anchorage BTC custody is not on any EVM
# chain, and chain_id stays non-nullable (a 0 sentinel, not NULL) so every row
# keeps an int. The protocol name drives the CUSTODY classification.
_OFFCHAIN_CHAIN_ID = 0
_ANCHORAGE_PROTOCOL_NAME = "anchorage"


def _anchorage_custody_row(
    holding: AnchorageCustodyHolding, category_service: AllocationCategoryService
) -> AllocationResponse:
    """Project an off-chain Anchorage custody holding onto the allocation model.

    BTC has no ``token`` registry row (root AGENTS: off-chain assets get none),
    so ``underlying_token_id`` / ``underlying_token_address`` are null and the
    symbol names the asset. ``amount_usd`` is the loan drawn (exposure); the
    collateral value rides on the domain entity so the surfaced figure can flip
    without a schema change. ``latest_activity_at`` is the snapshot time,
    surfaced verbatim so a frozen upstream feed reads as honestly stale.
    """
    return AllocationResponse(
        chain_id=_OFFCHAIN_CHAIN_ID,
        receipt_token_id=None,
        receipt_token_address=None,
        underlying_token_id=None,
        underlying_token_address=None,
        symbol=holding.symbol,
        underlying_symbol=holding.symbol,
        protocol_name=_ANCHORAGE_PROTOCOL_NAME,
        balance=holding.balance,
        amount_usd=holding.amount_usd,
        latest_activity_at=holding.as_of.isoformat(),
        latest_activity_action=None,
        latest_activity_amount=None,
        category=category_service.classify(_ANCHORAGE_PROTOCOL_NAME, holding.symbol),
        scope="prime",
    )


async def _no_custody() -> list[AnchorageCustodyHolding]:
    """Stand in for the custody fetch on a non-primary proxy, keeping the gather uniform."""
    return []


def _direct_underlying_identity(h: DirectAssetHolding) -> tuple[int, str, str]:
    """The holding's projected underlying identity, or the held token's own.

    Atomic on purpose: any missing piece falls back entirely, so a partial set
    (impossible from the repository's all-or-nothing projection, but cheap to
    guard) can never compose a hybrid of underlying id/address with the held
    token's symbol.
    """
    if h.underlying_token_id is None or h.underlying_token_address is None or h.underlying_symbol is None:
        return h.token_id, h.token_address, h.symbol
    return h.underlying_token_id, h.underlying_token_address, h.underlying_symbol


class AllocationActivityBucketResponse(BaseModel):
    """Allocation activity aggregated into a single time bucket."""

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    event_count: int = Field(description="Number of activity events in the bucket.", examples=[42])
    total_tx_amount: PlainDecimal = Field(
        description="Sum of `tx_amount` across the bucket's events, serialized as a JSON string.",
        examples=["1234567890000000000000"],
    )
    net_flow_usd: PlainDecimal = Field(
        description=(
            "Signed net flow valued in USD (inflows positive, outflows negative). Only receipt-token "
            "flows are valued: each is converted to underlying units at its row's share ratio "
            "(underlying_value / balance), borrowing the nearest same-token row's ratio when the "
            "row's own is unavailable and falling back to the raw tx_amount only when the token has "
            "no valued row at all, then priced at the receipt token's latest underlying oracle "
            "price. Rows whose recorded underlying diverges from the registry's are refused and "
            "contribute 0, as do direct holdings. Lets clients reconstruct a balance series by "
            "anchoring at the current total and cumulating net flows backwards."
        ),
        examples=["1234567.89"],
    )


class AllocationActivityEnvelope(BaseModel):
    """Allocation activity response: raw events or aggregated time buckets."""

    mode: Literal["raw", "aggregated"] = Field(description="`raw` for events, `aggregated` for time buckets.")
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[AllocationActivityResponse] | list[AllocationActivityBucketResponse] = Field(
        description="Events when `mode=raw`, count/sum buckets when `mode=aggregated`."
    )


@router.get(
    "/allocations/activity",
    response_model=AllocationActivityEnvelope,
    tags=["allocations"],
    summary="Allocation activity feed",
    description=(
        "Retrieve allocation activity events with optional filters, inside a `{mode, window, data}` "
        "envelope. All filters are optional and combine with logical AND. `protocol_name` and "
        "`token_symbol` use case-insensitive substring matching; the rest are exact matches. Results "
        "are time-windowed (default last 24h) and ordered newest first. Set `aggregate=true` for "
        "per-bucket event counts and tx-amount sums."
    ),
)
async def list_allocation_activity(
    response: Response,
    prime_id: Annotated[
        OptionalEthAddressParam,
        Query(
            description="Filter by prime address (0x-prefixed Ethereum address).",
            examples=["0x1234567890abcdef1234567890abcdef12345678"],
        ),
    ] = None,
    chain_id: int | None = Query(default=None, description="Filter by EVM chain id.", examples=[1]),
    protocol_name: str | None = Query(
        default=None,
        description="Filter by protocol name (case-insensitive substring).",
        examples=["aave"],
    ),
    action_type: str | None = Query(
        default=None,
        description="Filter by action type (`in`, `out`, `sweep`).",
        examples=["in"],
    ),
    token_symbol: str | None = Query(
        default=None,
        description="Filter by token symbol (case-insensitive substring).",
        examples=["USDC"],
    ),
    tx_hash: Annotated[
        OptionalTxHashParam,
        Query(
            description="Filter by transaction hash (0x-prefixed).",
            examples=["0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"],
        ),
    ] = None,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=1000, description="Max results (default 100, max 1000)."),
    service: AllocationService = Depends(_get_service),
) -> AllocationActivityEnvelope:
    """Errors:

    - 422 if ``prime_id`` is malformed (or ``limit`` is out of range).
    - 200 with an empty ``data`` list if filters match no rows — including when
      ``prime_id`` is well-formed but unknown. ``prime_id`` is treated as
      a filter here, not a path resource.
    """
    parsed_prime_id = EthAddress(prime_id) if prime_id is not None else None
    # Selective = an index-seekable exact filter. Substring filters
    # (protocol_name/token_symbol) and low-cardinality filters (chain_id,
    # action_type) do not qualify because they cannot prune chunks.
    has_selective_filter = parsed_prime_id is not None or tx_hash is not None
    try:
        enforce_filter_for_window(time_series, has_selective_filter=has_selective_filter)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    window = build_window(time_series)
    apply_cache_control(response, time_series)

    try:
        if time_series.aggregate:
            buckets = await service.list_activity_buckets(
                prime_id=parsed_prime_id,
                chain_id=chain_id,
                protocol_name=protocol_name,
                action_type=action_type,
                token_symbol=token_symbol,
                tx_hash=tx_hash,
                from_timestamp=time_series.from_timestamp,
                to_timestamp=time_series.to_timestamp,
                bucket_seconds=time_series.bucket.total_seconds(),
                limit=limit,
            )
            return AllocationActivityEnvelope(
                mode="aggregated",
                window=window,
                data=[AllocationActivityBucketResponse(**bucket.__dict__) for bucket in buckets],
            )

        events = await service.list_allocation_activity(
            prime_id=parsed_prime_id,
            chain_id=chain_id,
            protocol_name=protocol_name,
            action_type=action_type,
            token_symbol=token_symbol,
            tx_hash=tx_hash,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            limit=limit,
        )
    except ValueError as exc:
        logger.error(
            "Failed to retrieve allocation activity",
            extra={
                "prime_id": str(parsed_prime_id) if parsed_prime_id else None,
                "chain_id": chain_id,
                "protocol_name": protocol_name,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Failed to retrieve allocation activity") from exc

    return AllocationActivityEnvelope(
        mode="raw",
        window=window,
        data=[
            AllocationActivityResponse(
                chain_id=e.chain_id,
                prime_address=e.prime_address,
                prime_name=e.prime_name,
                protocol_name=e.protocol_name,
                token_id=e.token_id,
                token_symbol=e.token_symbol,
                action_type=e.action_type,
                tx_amount=e.tx_amount,
                balance=e.balance,
                tx_hash=None if e.action_type.lower() == "sweep" else e.tx_hash,
                log_index=e.log_index,
                block_number=e.block_number,
                block_version=e.block_version,
                created_at=e.created_at.isoformat(),
            )
            for e in events
        ],
    )
