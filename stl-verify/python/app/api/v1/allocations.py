from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import AfterValidator, BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.api.deps import get_engine
from app.domain.entities.allocation import EthAddress
from app.services.allocation_service import AllocationService

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
    chain_id: int
    receipt_token_id: int
    receipt_token_address: str
    underlying_token_id: int
    underlying_token_address: str
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal


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
        )
        for p in positions
    ]
