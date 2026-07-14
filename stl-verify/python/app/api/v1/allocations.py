import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import EthAddressParam, OptionalEthAddressParam, OptionalTxHashParam
from app.api.deps import get_engine
from app.api.time_series import TimeSeriesWindow, apply_cache_control, build_window, get_time_series_query_params
from app.config import get_settings
from app.domain.entities.allocation import DirectAssetHolding, EthAddress
from app.domain.entities.allocation_category import AllocationCategory
from app.domain.time_series import TimeSeriesQuery, enforce_filter_for_window
from app.services.allocation_category_service import AllocationCategoryService
from app.services.allocation_service import AllocationService

logger = logging.getLogger(__name__)
router = APIRouter()


class PrimeResponse(BaseModel):
    """A prime (capital allocator) tracked by STL."""

    id: str = Field(description="Stable surrogate id for the prime.", examples=["prime-acme"])
    name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    address: str = Field(
        description="0x-prefixed Ethereum address controlled by the prime.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
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

    Two row shapes share this model:
    - Receipt-token positions (e.g. spUSDT wrapping USDT): all fields populated.
    - Direct asset holdings (e.g. PYUSD held in the proxy with no wrapper):
      ``receipt_token_id`` / ``receipt_token_address`` / ``protocol_name`` are
      null; ``symbol`` names the held asset. ``underlying_*`` usually point at
      the held asset itself, except holdings valued on the underlying-value
      basis (allowlisted, e.g. a Uni V3 pool position valued in USDC) with a
      resolvable underlying, where they point at that underlying.
      ``amount_usd`` is populated when an oracle price exists for the pricing
      basis and null otherwise (e.g. LP/curve shares with no oracle feed).
    """

    chain_id: int = Field(description="EVM chain id of the position.", examples=[1])
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
    underlying_token_id: int = Field(
        description=(
            "Surrogate id of the underlying token. For direct holdings, this is the held asset itself, "
            "unless the holding is valued on the underlying-value basis (allowlisted)."
        ),
        examples=[1],
    )
    underlying_token_address: str = Field(
        description=(
            "0x-prefixed underlying-token contract address. For direct holdings, this is the held asset itself, "
            "unless the holding is valued on the underlying-value basis (allowlisted)."
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
            "unless the holding is valued on the underlying-value basis (allowlisted)."
        ),
        examples=["USDC"],
    )
    protocol_name: str | None = Field(
        default=None,
        description="Protocol the position is held in. `null` for direct holdings (no registered wrapper).",
        examples=["aave-v3"],
    )
    balance: Decimal = Field(
        description="Balance held by the prime, in token units. Decimal serialized as a JSON string.",
        examples=["1234567.89"],
    )
    amount_usd: Decimal | None = Field(
        default=None,
        description="USD value of the position when a price is available; `null` otherwise.",
        examples=["1234567.89"],
    )
    latest_activity_at: str | None = Field(
        default=None,
        description="ISO-8601 timestamp of the most recent on-chain activity for this position, or `null`.",
        examples=["2026-05-07T12:00:00Z"],
    )
    category: AllocationCategory = Field(
        description="Allocation category derived from protocol/symbol (`allocation`, `pol`, `psm3`, `asset`).",
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
                "category": "allocation",
            }
        }
    }


class CapitalMetricsResponse(BaseModel):
    """Prime-level capital metrics for risk and alert management."""

    prime_id: str = Field(description="Stable surrogate id for the prime.", examples=["prime-acme"])
    prime_name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    exposure: Decimal = Field(
        description="Total USD exposure across the prime's allocations (upstream `exposure`).",
        examples=["1900000000"],
    )
    capital_buffer: Decimal = Field(
        description="`max(total_risk_capital - required_risk_capital, 0)` — unencumbered risk capital (USD).",
        examples=["2500000"],
    )
    required_risk_capital: Decimal = Field(
        description="Required Risk Capital (RRC) reported by upstream `financial_rrc` (USD).",
        examples=["7500000"],
    )
    total_risk_capital: Decimal = Field(
        description="Total Risk Capital reported by upstream `total_rc` (USD).",
        examples=["10000000"],
    )
    encumbrance_ratio: Decimal | None = Field(
        default=None,
        description=(
            "Required Risk Capital as a share of Total Risk Capital "
            "(upstream `risk_tolerance_ratio`). `null` when not validated."
        ),
        examples=["0.85"],
    )
    timestamp: str = Field(
        description="ISO-8601 timestamp the snapshot was assembled.",
        examples=["2026-05-07T12:00:00Z"],
    )
    benchmark_source: str | None = Field(
        default=None,
        description="URL of the upstream benchmark source used to populate the row.",
    )
    is_validated: bool = Field(default=False, description="Whether the row was validated against on-chain state.")
    validation_note: str | None = Field(
        default=None,
        description="Human-readable note about validation, e.g. why a row is missing or unmatched.",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "prime_id": "prime-acme",
                "prime_name": "Acme Prime",
                "exposure": "1900000000",
                "capital_buffer": "2500000",
                "required_risk_capital": "7500000",
                "total_risk_capital": "10000000",
                "encumbrance_ratio": "0.85",
                "timestamp": "2026-05-07T12:00:00Z",
                "benchmark_source": "https://example.com/star-rrc",
                "is_validated": False,
                "validation_note": "Sourced from Star Agents Risk Capital & Requirements Monitor.",
            }
        }
    }


class AllocationActivityResponse(BaseModel):
    """Allocation activity event record for timeline feeds."""

    chain_id: int = Field(description="EVM chain id where the event occurred.", examples=[1])
    prime_address: str = Field(
        description="Prime's 0x-prefixed Ethereum address.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    prime_name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    protocol_name: str | None = Field(
        default=None, description="Protocol the event was emitted by.", examples=["aave-v3"]
    )
    token_id: int = Field(description="Surrogate id of the receipt token involved.", examples=[42])
    token_symbol: str | None = Field(default=None, description="Receipt-token symbol, when known.", examples=["aUSDC"])
    action_type: str = Field(description="One of `in`, `out`, `sweep`.", examples=["in"])
    tx_amount: Decimal = Field(
        description="Token-unit amount moved by this event. Decimal serialized as a JSON string.",
        examples=["1000.5"],
    )
    balance: Decimal = Field(
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


class StarRiskCapitalRowResponse(BaseModel):
    """Internal upstream payload row from the Star risk-capital monitor."""

    star: str
    exposure: str
    total_rc: str
    financial_rrc: str
    exposure_share: str
    risk_tolerance_ratio: str


class StarRiskCapitalDataResponse(BaseModel):
    results: list[StarRiskCapitalRowResponse] = []


class StarRiskCapitalResponse(BaseModel):
    data: StarRiskCapitalDataResponse | None = None
    status: int | None = None
    success: bool | None = None


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(AllocationRepository(engine))


async def _fetch_star_risk_capital_payload() -> StarRiskCapitalResponse:
    settings = get_settings()
    timeout = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(settings.star_risk_capital_upstream_url)
    except httpx.HTTPError as exc:
        logger.exception(
            "Failed to fetch Star risk capital upstream",
            extra={"upstream_url": settings.star_risk_capital_upstream_url},
        )
        raise HTTPException(status_code=502, detail="Risk capital upstream request failed") from exc

    if not response.is_success:
        logger.error(
            "Star risk capital upstream returned non-success status",
            extra={
                "upstream_url": settings.star_risk_capital_upstream_url,
                "status_code": response.status_code,
                "response_preview": response.text[:500],
            },
        )
        raise HTTPException(
            status_code=502,
            detail=f"Risk capital upstream returned status {response.status_code}",
        )

    try:
        payload = response.json()
    except ValueError as exc:
        logger.exception(
            "Star risk capital upstream returned invalid JSON",
            extra={"upstream_url": settings.star_risk_capital_upstream_url},
        )
        raise HTTPException(status_code=502, detail="Risk capital upstream returned invalid JSON") from exc

    try:
        parsed = StarRiskCapitalResponse.model_validate(payload)
    except ValidationError as exc:
        logger.exception(
            "Star risk capital upstream response had unexpected shape",
            extra={
                "upstream_url": settings.star_risk_capital_upstream_url,
                "validation_errors": exc.errors(),
            },
        )
        raise HTTPException(status_code=502, detail="Risk capital upstream response shape mismatch") from exc

    if parsed.success is False or (parsed.status is not None and parsed.status >= 400):
        logger.error(
            "Star risk capital upstream reported failure",
            extra={
                "upstream_url": settings.star_risk_capital_upstream_url,
                "upstream_status": parsed.status,
                "upstream_success": parsed.success,
            },
        )
        raise HTTPException(status_code=502, detail="Risk capital upstream reported failure")

    return parsed


def _to_decimal(value: str, *, field: str, prime_name: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        logger.error(
            "Invalid numeric value in Star risk capital payload",
            extra={
                "field": field,
                "prime_name": prime_name,
                "value": value,
            },
        )
        raise HTTPException(
            status_code=502,
            detail=(
                f"Risk capital upstream returned invalid numeric value for field '{field}' and prime '{prime_name}'"
            ),
        ) from exc


@router.get(
    "/primes",
    response_model=list[PrimeResponse],
    tags=["primes"],
    summary="List all primes",
    description="Return every prime tracked by STL with its surrogate id, name, and on-chain address.",
)
async def list_primes(service: AllocationService = Depends(_get_service)):
    primes = await service.list_primes()
    return [PrimeResponse(id=p.id, name=p.name, address=p.address) for p in primes]


@router.get(
    "/capital-metrics",
    response_model=list[CapitalMetricsResponse],
    tags=["capital", "internal"],
    summary="List per-prime capital metrics",
    description=(
        "Join each tracked prime with the latest row from the upstream Star risk-capital monitor "
        "and return derived capital metrics: risk capital, first-loss capital, total capital, "
        "and the buffer between them. Primes without a matching upstream row are still returned, "
        "with zeroed metrics and a `validation_note` explaining why. A `502` is returned only when "
        "the upstream call itself fails."
    ),
)
async def list_capital_metrics(
    service: AllocationService = Depends(_get_service),
) -> list[CapitalMetricsResponse]:
    primes = await service.list_primes()
    star_payload = await _fetch_star_risk_capital_payload()
    rows = star_payload.data.results if star_payload.data else []

    settings = get_settings()
    metrics = []
    for prime in primes:
        row = next(
            (r for r in rows if r.star.strip().lower() == prime.name.strip().lower()),
            None,
        )
        if not row:
            logger.warning(
                "No upstream risk capital data found for prime",
                extra={
                    "prime_id": prime.id,
                    "prime_name": prime.name,
                    "upstream_url": settings.star_risk_capital_upstream_url,
                },
            )
            metrics.append(
                CapitalMetricsResponse(
                    prime_id=prime.id,
                    prime_name=prime.name,
                    exposure=Decimal("0"),
                    capital_buffer=Decimal("0"),
                    required_risk_capital=Decimal("0"),
                    total_risk_capital=Decimal("0"),
                    encumbrance_ratio=None,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    benchmark_source=settings.star_risk_capital_upstream_url,
                    is_validated=False,
                    validation_note="No upstream Star risk-capital row matched this prime.",
                )
            )
            continue

        total_rc = _to_decimal(row.total_rc, field="total_rc", prime_name=prime.name)
        financial_rrc = _to_decimal(row.financial_rrc, field="financial_rrc", prime_name=prime.name)
        capital_buffer = max(total_rc - financial_rrc, Decimal("0"))

        metrics.append(
            CapitalMetricsResponse(
                prime_id=prime.id,
                prime_name=prime.name,
                exposure=_to_decimal(row.exposure, field="exposure", prime_name=prime.name),
                capital_buffer=capital_buffer,
                required_risk_capital=financial_rrc,
                total_risk_capital=total_rc,
                encumbrance_ratio=_to_decimal(
                    row.risk_tolerance_ratio,
                    field="risk_tolerance_ratio",
                    prime_name=prime.name,
                ),
                timestamp=datetime.now(timezone.utc).isoformat(),
                benchmark_source=settings.star_risk_capital_upstream_url,
                is_validated=False,
                validation_note="Sourced from Star Agents Risk Capital & Requirements Monitor.",
            )
        )

    return metrics


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
        "Return every current allocation held by the given prime — both receipt-token "
        "positions (enriched with USD value when a price is available) and direct asset "
        "holdings (tokens held in the proxy with no registered receipt-token wrapper, "
        "surfaced with `receipt_token_id`, `receipt_token_address` and `protocol_name` "
        "set to `null`, and `amount_usd` valued from the token's oracle price when one "
        "exists). Each row includes the latest on-chain activity "
        "timestamp and a derived `category` (`allocation` / `pol` / `psm3` / `asset`)."
    ),
)
async def list_allocations(
    prime_id: EthAddressParam,
    service: AllocationService = Depends(_get_service),
):
    """Return current allocations for ``prime_id``.

    Combines two sources:
    - Receipt-token positions (e.g. spUSDT wrapping USDT).
    - Direct asset holdings — tokens held in the proxy that are not
      registered as receipt-token wrappers (e.g. PYUSD, syrupUSDT). These
      rows have null ``receipt_token_*`` / ``protocol_name``; ``amount_usd``
      is valued from the token's oracle price when one exists, else null.

    Errors:
    - 422 if ``prime_id`` is malformed.
    - 404 if ``prime_id`` is well-formed but no such prime exists.
    """
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    positions, direct_holdings = await asyncio.gather(
        service.list_receipt_token_positions(prime_address),
        service.list_direct_asset_holdings(prime_address),
    )
    category_service = AllocationCategoryService()

    receipt_rows = [
        AllocationResponse(
            chain_id=p.chain_id,
            receipt_token_id=p.receipt_token_id,
            receipt_token_address=p.receipt_token_address,
            underlying_token_id=p.underlying_token_id,
            underlying_token_address=p.underlying_token_address,
            symbol=p.symbol,
            underlying_symbol=p.underlying_symbol,
            protocol_name=p.protocol_name,
            balance=p.balance,
            amount_usd=p.amount_usd,
            latest_activity_at=p.latest_activity_at.isoformat() if p.latest_activity_at else None,
            category=category_service.classify(p.protocol_name, p.symbol),
        )
        for p in positions
    ]
    # Underlying identity travels with the price basis; see
    # _DIRECT_ASSET_HOLDINGS_SQL.
    direct_rows = []
    for h in direct_holdings:
        underlying_id, underlying_address, underlying_symbol = _direct_underlying_identity(h)
        direct_rows.append(
            AllocationResponse(
                chain_id=h.chain_id,
                receipt_token_id=None,
                receipt_token_address=None,
                underlying_token_id=underlying_id,
                underlying_token_address=underlying_address,
                symbol=h.symbol,
                underlying_symbol=underlying_symbol,
                protocol_name=None,
                balance=h.balance,
                amount_usd=h.amount_usd,
                latest_activity_at=h.latest_activity_at.isoformat() if h.latest_activity_at else None,
                category=category_service.classify(None, h.symbol),
            )
        )
    return receipt_rows + direct_rows


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
    total_tx_amount: Decimal = Field(
        description="Sum of `tx_amount` across the bucket's events, serialized as a JSON string.",
        examples=["1234567890000000000000"],
    )
    net_flow_usd: Decimal = Field(
        description=(
            "Signed net flow valued in USD (inflows positive, outflows negative), using the receipt "
            "token's latest underlying oracle price for wrapped positions and the token's own latest "
            "oracle price for direct holdings. Lets clients reconstruct a balance series by anchoring "
            "at the current total and cumulating net flows backwards."
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
