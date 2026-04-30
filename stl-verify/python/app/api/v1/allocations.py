from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import AfterValidator, BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.api.deps import get_engine
from app.domain.entities.allocation import EthAddress
from app.domain.entities.allocation_category import AllocationCategory
from app.services.allocation_category_service import AllocationCategoryService
from app.services.allocation_service import AllocationService
from app.services.capital_metrics_service import CapitalMetricsService
from app.services.data_provenance_service import DataProvenanceService

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


class DataSourceResponse(BaseModel):
    """Data source metadata for transparency panel."""

    name: str
    host: str
    access_model: str  # open|public|closed
    role: str
    caveat: str | None = None
    attribution_required: bool = False


class DataSourcesResponse(BaseModel):
    """Aggregated data sources for methodology transparency."""

    sources: list[DataSourceResponse]
    methodology_markdown: str


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(PostgresAllocationRepository(engine))


@router.get("/primes", response_model=list[PrimeResponse])
async def list_primes(service: AllocationService = Depends(_get_service)):
    primes = await service.list_primes()
    return [PrimeResponse(id=p.id, name=p.name, address=p.address) for p in primes]


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
            category=category_service.classify(p.protocol_name, p.symbol),
        )
        for p in positions
    ]


@router.get("/primes/{prime_id}/capital-metrics", response_model=CapitalMetricsResponse | None)
async def get_capital_metrics(
    prime_id: EthAddressPath,
    engine: AsyncEngine = Depends(get_engine),
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
    async with engine.connect() as conn:
        repo = PostgresAllocationRepository(conn)
        service = CapitalMetricsService(repo)
        metrics = await service.get_capital_metrics(prime_id)

        if not metrics:
            return None

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


@router.get("/data-sources", response_model=DataSourcesResponse)
async def get_data_sources():
    """Retrieve data sources and methodology for transparency.

    Returns:
    - sources: List of data sources used across the platform
    - methodology_markdown: Formatted methodology/transparency text for UI panels
    """
    service = DataProvenanceService()
    sources = service.get_sources()
    methodology = service.get_methodology_panel_text()

    return DataSourcesResponse(
        sources=[
            DataSourceResponse(
                name=s.name,
                host=s.host,
                access_model=s.access_model.value,
                role=s.role,
                caveat=s.caveat,
                attribution_required=s.attribution_required,
            )
            for s in sources
        ],
        methodology_markdown=methodology,
    )
