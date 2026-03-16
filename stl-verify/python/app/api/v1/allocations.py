import dataclasses
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import AfterValidator, BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_repository import PostgresAllocationRepository
from app.domain.entities.allocation import EthAddress
from app.services.allocation_service import AllocationService

router = APIRouter()


def _validate_eth_address(value: str) -> EthAddress:
    """Convert a raw path string to an EthAddress.

    Raises ValueError on malformed input, which FastAPI surfaces as HTTP 422.
    """
    return EthAddress(value)


EthAddressPath = Annotated[str, AfterValidator(_validate_eth_address)]


class StarResponse(BaseModel):
    id: str
    name: str
    address: str


class AllocationPositionResponse(BaseModel):
    id: int
    chain_id: int
    name: str
    proxy_address: str
    token_address: str
    token_symbol: str | None
    token_decimals: int | None
    balance: Decimal
    scaled_balance: Decimal | None
    block_number: int
    block_version: int
    tx_hash: str
    log_index: int
    tx_amount: Decimal
    direction: str
    created_at: datetime


def _get_engine(request: Request) -> AsyncEngine:
    return request.app.state.engine


async def _get_service(engine: AsyncEngine = Depends(_get_engine)) -> AllocationService:
    async with engine.connect() as conn:
        repo = PostgresAllocationRepository(conn)
        yield AllocationService(repo)


@router.get("/stars", response_model=list[StarResponse])
async def list_stars(service: AllocationService = Depends(_get_service)):
    stars = await service.list_stars()
    return [StarResponse(id=s.id, name=s.name, address=s.address) for s in stars]


@router.get("/stars/{star_id}/allocations", response_model=list[AllocationPositionResponse])
async def list_allocations(
    star_id: EthAddressPath,
    block_number: int | None = None,
    service: AllocationService = Depends(_get_service),
):
    positions = await service.list_allocations_by_star(star_id, block_number)
    return [AllocationPositionResponse(**dataclasses.asdict(p)) for p in positions]
