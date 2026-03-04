import dataclasses
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_repository import PostgresAllocationRepository
from app.adapters.postgres.engine import get_engine
from app.config import Settings, get_settings
from app.services.allocation_service import AllocationService

router = APIRouter()


class StarResponse(BaseModel):
    name: str


class AllocationPositionResponse(BaseModel):
    id: int
    chain_id: int
    star: str
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


def _get_engine(settings: Settings = Depends(get_settings)) -> AsyncEngine:
    return get_engine(settings)


async def _get_service(engine: AsyncEngine = Depends(_get_engine)) -> AllocationService:
    async with engine.connect() as conn:
        repo = PostgresAllocationRepository(conn)
        yield AllocationService(repo)


@router.get("/stars", response_model=list[StarResponse])
async def list_stars(service: AllocationService = Depends(_get_service)):
    stars = await service.list_stars()
    return [StarResponse(name=s.name) for s in stars]


@router.get("/stars/{star}/allocations", response_model=list[AllocationPositionResponse])
async def list_allocations(
    star: str,
    block_number: int | None = None,
    service: AllocationService = Depends(_get_service),
):
    positions = await service.list_allocations_by_star(star, block_number)
    return [AllocationPositionResponse(**dataclasses.asdict(p)) for p in positions]
