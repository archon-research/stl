import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.api._validators import EthAddressParam, OptionalEthAddressParam
from app.api.deps import get_engine
from app.config import get_settings
from app.domain.entities.allocation import EthAddress
from app.domain.entities.allocation_category import AllocationCategory
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
      ``receipt_token_id`` / ``receipt_token_address`` / ``protocol_name`` /
      ``amount_usd`` are null; ``symbol`` and ``underlying_symbol`` both name
      the held asset; ``underlying_token_id`` / ``underlying_token_address``
      point at it.
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
        description="Surrogate id of the underlying token. For direct holdings, this is the held asset itself.",
        examples=[1],
    )
    underlying_token_address: str = Field(
        description=(
            "0x-prefixed underlying-token contract address. For direct holdings, this is the held asset itself."
        ),
        examples=["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"],
    )
    symbol: str = Field(
        description="Display symbol: receipt-token symbol for wrapped positions, asset symbol for direct holdings.",
        examples=["aUSDC"],
    )
    underlying_symbol: str = Field(
        description="Underlying-token symbol. For direct holdings, same as ``symbol``.",
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
    risk_capital: Decimal = Field(
        description="Risk capital exposure (USD) sourced from the upstream Star monitor.",
        examples=["10000000"],
    )
    capital_buffer: Decimal = Field(
        description="`max(total_capital - first_loss_capital, 0)` — distance to first-loss exhaustion (USD).",
        examples=["2500000"],
    )
    first_loss_capital: Decimal = Field(
        description="Financial RRC (first-loss capital) reported by upstream (USD).",
        examples=["7500000"],
    )
    total_capital: Decimal = Field(
        description="Total RRC reported by upstream (USD).",
        examples=["10000000"],
    )
    risk_to_capital_ratio: Decimal | None = Field(
        default=None,
        description="Upstream `risk_tolerance_ratio`. `null` when not validated.",
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
                "risk_capital": "10000000",
                "capital_buffer": "2500000",
                "first_loss_capital": "7500000",
                "total_capital": "10000000",
                "risk_to_capital_ratio": "0.85",
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
    return AllocationService(PostgresAllocationRepository(engine))


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
                    risk_capital=Decimal("0"),
                    capital_buffer=Decimal("0"),
                    first_loss_capital=Decimal("0"),
                    total_capital=Decimal("0"),
                    risk_to_capital_ratio=None,
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
                risk_capital=_to_decimal(row.exposure, field="exposure", prime_name=prime.name),
                capital_buffer=capital_buffer,
                first_loss_capital=financial_rrc,
                total_capital=total_rc,
                risk_to_capital_ratio=_to_decimal(
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
        "surfaced with `receipt_token_id`, `receipt_token_address`, `protocol_name` and "
        "`amount_usd` set to `null`). Each row includes the latest on-chain activity "
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
      rows have null ``receipt_token_*`` / ``protocol_name`` / ``amount_usd``.

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
    direct_rows = [
        AllocationResponse(
            chain_id=h.chain_id,
            receipt_token_id=None,
            receipt_token_address=None,
            underlying_token_id=h.token_id,
            underlying_token_address=h.token_address,
            symbol=h.symbol,
            underlying_symbol=h.symbol,
            protocol_name=None,
            balance=h.balance,
            amount_usd=None,
            latest_activity_at=h.latest_activity_at.isoformat() if h.latest_activity_at else None,
            category=category_service.classify(None, h.symbol),
        )
        for h in direct_holdings
    ]
    return receipt_rows + direct_rows


@router.get(
    "/allocations/activity",
    response_model=list[AllocationActivityResponse],
    tags=["allocations"],
    summary="Allocation activity feed",
    description=(
        "Retrieve allocation activity events with optional filters. All filters are optional "
        "and combine with logical AND. `protocol_name` and `token_symbol` use case-insensitive "
        "substring matching; the rest are exact matches. Results are ordered newest first."
    ),
)
async def list_allocation_activity(
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
    tx_hash: str | None = Query(
        default=None,
        description="Filter by transaction hash (0x-prefixed).",
        examples=["0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"],
    ),
    from_timestamp: datetime | None = Query(default=None, description="Inclusive lower timestamp bound (ISO-8601)."),
    to_timestamp: datetime | None = Query(default=None, description="Inclusive upper timestamp bound (ISO-8601)."),
    limit: int = Query(100, ge=1, le=1000, description="Max results (default 100, max 1000)."),
    service: AllocationService = Depends(_get_service),
):
    """Errors:

    - 422 if ``prime_id`` is malformed (or ``limit`` is out of range).
    - 200 with an empty list if filters match no rows — including when
      ``prime_id`` is well-formed but unknown. ``prime_id`` is treated as
      a filter here, not a path resource.
    """
    parsed_prime_id = EthAddress(prime_id) if prime_id is not None else None

    try:
        events = await service.list_allocation_activity(
            prime_id=parsed_prime_id,
            chain_id=chain_id,
            protocol_name=protocol_name,
            action_type=action_type,
            token_symbol=token_symbol,
            tx_hash=tx_hash,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
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

    return [
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
    ]
