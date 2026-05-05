import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import AfterValidator, BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.api.deps import get_engine
from app.config import get_settings
from app.domain.entities.allocation import EthAddress
from app.domain.entities.allocation_category import AllocationCategory
from app.services.allocation_category_service import AllocationCategoryService
from app.services.allocation_service import AllocationService
from app.services.capital_metrics_service import CapitalMetricsService

logger = logging.getLogger(__name__)
router = APIRouter()


def _validate_eth_address(value: str) -> EthAddress:
    """Convert a raw path string to an EthAddress.

    Raises ValueError on malformed input, which FastAPI surfaces as HTTP 422.
    """
    return EthAddress(value)


EthAddressPath = Annotated[str, AfterValidator(_validate_eth_address)]


class PrimeResponse(BaseModel):
    id: str
    name: str
    address: str


class ChainResponse(BaseModel):
    chain_id: int
    name: str


class ProtocolResponse(BaseModel):
    id: int
    chain_id: int
    encode: str
    name: str


class AllocationResponse(BaseModel):
    """Enriched allocation response with category and metadata."""

    chain_id: int
    receipt_token_id: int
    receipt_token_address: str
    underlying_token_id: int
    underlying_token_address: str
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal
    amount_usd: Decimal | None = None
    latest_activity_at: str | None = None
    category: AllocationCategory  # New: allocation type (allocation/pol/psm3/asset)


class CapitalMetricsResponse(BaseModel):
    """Prime-level capital metrics for risk and alert management."""

    prime_id: str
    prime_name: str
    risk_capital: Decimal
    capital_buffer: Decimal
    first_loss_capital: Decimal
    total_capital: Decimal
    risk_to_capital_ratio: Decimal | None
    timestamp: str  # ISO format
    benchmark_source: str | None = None
    is_validated: bool = False
    validation_note: str | None = None


class AllocationActivityResponse(BaseModel):
    """Allocation activity event record for timeline feeds."""

    chain_id: int
    prime_address: str
    prime_name: str
    protocol_name: str | None
    token_id: int
    token_symbol: str | None
    action_type: str
    tx_amount: Decimal
    balance: Decimal
    tx_hash: str | None
    log_index: int
    block_number: int
    block_version: int
    created_at: str


class StarRiskCapitalRowResponse(BaseModel):
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


async def _get_capital_metrics_service(engine: AsyncEngine = Depends(get_engine)) -> CapitalMetricsService:
    return CapitalMetricsService(PostgresAllocationRepository(engine))


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


@router.get("/primes", response_model=list[PrimeResponse])
async def list_primes(service: AllocationService = Depends(_get_service)):
    primes = await service.list_primes()
    return [PrimeResponse(id=p.id, name=p.name, address=p.address) for p in primes]


@router.get("/capital-metrics", response_model=list[CapitalMetricsResponse])
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


@router.get("/chains", response_model=list[ChainResponse])
async def list_chains(service: AllocationService = Depends(_get_service)):
    chains = await service.list_chains()
    return [ChainResponse(chain_id=chain.chain_id, name=chain.name) for chain in chains]


@router.get("/protocols", response_model=list[ProtocolResponse])
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


@router.get("/primes/{prime_id}/allocations", response_model=list[AllocationResponse])
async def list_allocations(
    prime_id: EthAddressPath,
    service: AllocationService = Depends(_get_service),
):
    positions = await service.list_receipt_token_positions(prime_id)
    category_service = AllocationCategoryService()

    return [
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


@router.get("/allocations/activity", response_model=list[AllocationActivityResponse])
async def list_allocation_activity(
    prime_id: str | None = None,
    chain_id: int | None = None,
    protocol_name: str | None = None,
    action_type: str | None = None,
    token_symbol: str | None = None,
    tx_hash: str | None = None,
    from_timestamp: datetime | None = None,
    to_timestamp: datetime | None = None,
    limit: int = 100,
    service: AllocationService = Depends(_get_service),
):
    """Retrieve allocation activity events with optional URL filters.

    Query parameters:
    - prime_id: Filter by prime address (0x-prefixed Ethereum address)
    - chain_id: Filter by chain ID
    - protocol_name: Filter by protocol name (case-insensitive substring)
    - action_type: Filter by action type (`in`, `out`, `sweep`)
    - token_symbol: Filter by token symbol (case-insensitive substring)
    - tx_hash: Filter by transaction hash (0x-prefixed)
    - from_timestamp: Inclusive lower timestamp bound (ISO-8601)
    - to_timestamp: Inclusive upper timestamp bound (ISO-8601)
    - limit: Max results (default 100, max 1000)
    """
    if prime_id is None:
        parsed_prime_id = None
    else:
        try:
            parsed_prime_id = EthAddress(prime_id)
        except ValueError as exc:
            logger.warning(
                "Invalid Ethereum address provided to allocation activity endpoint",
                extra={
                    "prime_id_input": prime_id,
                    "validation_error": str(exc),
                    "endpoint": "/v1/allocations/activity",
                },
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc

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
            tx_hash=e.tx_hash,
            log_index=e.log_index,
            block_number=e.block_number,
            block_version=e.block_version,
            created_at=e.created_at.isoformat(),
        )
        for e in events
    ]


@router.get("/star-risk-capital/primes", response_model=StarRiskCapitalResponse)
async def get_star_risk_capital_requirements():
    """Proxy published Star risk capital payload through backend to avoid browser CORS issues."""
    return await _fetch_star_risk_capital_payload()
