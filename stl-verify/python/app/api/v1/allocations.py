from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine
from app.domain.entities.allocation import EthAddress
from app.domain.entities.allocation_category import AllocationCategory
from app.services.allocation_category_service import AllocationCategoryService
from app.services.allocation_service import AllocationService
from app.services.capital_metrics_service import CapitalMetricsService

router = APIRouter()


class PrimeResponse(BaseModel):
    id: str
    name: str
    address: str


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
    category: AllocationCategory  # New: allocation type (allocation/pol/psm3/asset)


class CapitalMetricsResponse(BaseModel):
    """Prime-level capital metrics for risk and alert management."""

    prime_id: str
    prime_name: str
    risk_capital: Decimal
    capital_buffer: Decimal
    first_loss_capital: Decimal
    total_capital: Decimal
    risk_to_capital_ratio: Decimal
    timestamp: str  # ISO format
    benchmark_source: str | None = None
    is_validated: bool = False
    validation_note: str | None = None


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(PostgresAllocationRepository(engine))


async def _get_capital_metrics_service(engine: AsyncEngine = Depends(get_engine)) -> CapitalMetricsService:
    return CapitalMetricsService(PostgresAllocationRepository(engine))


@router.get("/primes", response_model=list[PrimeResponse])
async def list_primes(service: AllocationService = Depends(_get_service)):
    primes = await service.list_primes()
    return [PrimeResponse(id=p.id, name=p.name, address=p.address) for p in primes]


@router.get("/primes/{prime_id}/allocations", response_model=list[AllocationResponse])
async def list_allocations(
    prime_id: EthAddressParam,
    service: AllocationService = Depends(_get_service),
):
    positions = await service.list_receipt_token_positions(EthAddress(prime_id))
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
            category=category_service.classify(p.protocol_name, p.symbol),
        )
        for p in positions
    ]


@router.get("/primes/{prime_id}/capital-metrics", response_model=CapitalMetricsResponse)
async def get_capital_metrics(
    prime_id: EthAddressParam,
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
    metrics = await service.get_capital_metrics(EthAddress(prime_id))
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
