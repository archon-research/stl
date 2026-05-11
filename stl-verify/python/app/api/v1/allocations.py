import logging
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import AfterValidator, BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.api.deps import get_engine
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
    """Enriched allocation response with category and metadata.

    Two row shapes share this model:
    - Receipt-token positions (e.g. spUSDT wrapping USDT): all fields populated.
    - Direct asset holdings (e.g. PYUSD held in the proxy with no wrapper):
      ``receipt_token_id`` / ``receipt_token_address`` / ``protocol_name`` /
      ``amount_usd`` are null; ``symbol`` and ``underlying_symbol`` both name
      the held asset; ``underlying_token_id`` / ``underlying_token_address``
      point at it.
    """

    chain_id: int
    receipt_token_id: int | None = None
    receipt_token_address: str | None = None
    underlying_token_id: int
    underlying_token_address: str
    symbol: str
    underlying_symbol: str
    protocol_name: str | None = None
    balance: Decimal
    amount_usd: Decimal | None = None
    latest_activity_at: str | None = None
    category: AllocationCategory  # allocation/pol/psm3/asset


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


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(PostgresAllocationRepository(engine))


async def _get_capital_metrics_service(engine: AsyncEngine = Depends(get_engine)) -> CapitalMetricsService:
    return CapitalMetricsService(PostgresAllocationRepository(engine))


@router.get("/primes", response_model=list[PrimeResponse])
async def list_primes(service: AllocationService = Depends(_get_service)):
    primes = await service.list_primes()
    return [PrimeResponse(id=p.id, name=p.name, address=p.address) for p in primes]


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
    if not await service.prime_exists(prime_id):
        raise HTTPException(status_code=404, detail=f"Prime not found: {prime_id}")

    positions = await service.list_receipt_token_positions(prime_id)
    direct_holdings = await service.list_direct_asset_holdings(prime_id)
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


@router.get("/primes/{prime_id}/capital-metrics", response_model=CapitalMetricsResponse)
async def get_capital_metrics(
    prime_id: EthAddressPath,
    service: CapitalMetricsService = Depends(_get_capital_metrics_service),
):
    """Retrieve capital metrics for a prime.

    Responds with:
    - risk_capital: Total risk-bearing capital
    - capital_buffer: Baseline protective capital
    - first_loss_capital: Prime-owned first-loss layer
    - total_capital: Sum of capital tiers
    - risk_to_capital_ratio: Risk / Capital (use for alert thresholds, e.g., >1.0)
    - is_validated: Whether reconciled against external benchmark
    - validation_note: Any caveats or pending work on this endpoint
    """
    metrics = await service.get_capital_metrics(prime_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Prime not found")

    return CapitalMetricsResponse(
        prime_id=metrics.prime_id,
        prime_name=metrics.prime_name,
        risk_capital=metrics.risk_capital,
        capital_buffer=metrics.capital_buffer,
        first_loss_capital=metrics.first_loss_capital,
        total_capital=metrics.total_capital,
        risk_to_capital_ratio=metrics.risk_to_capital_ratio,
        timestamp=metrics.timestamp.isoformat(),
        benchmark_source=metrics.benchmark_source,
        is_validated=metrics.is_validated,
        validation_note=metrics.validation_note,
    )
