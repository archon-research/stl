import dataclasses
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
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


class StarResponse(BaseModel):
    id: str
    name: str
    address: str


class ReceiptTokenPositionResponse(BaseModel):
    receipt_token_id: int
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal
    token_address: str | None


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


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AsyncIterator[AllocationService]:
    async with engine.connect() as conn:
        repo = PostgresAllocationRepository(conn)
        yield AllocationService(repo)


@router.get("/stars", response_model=list[StarResponse])
async def list_stars(service: AllocationService = Depends(_get_service)):
    stars = await service.list_stars()
    return [StarResponse(id=s.id, name=s.name, address=s.address) for s in stars]


@router.get("/stars/{star_id}/receipt-tokens", response_model=list[ReceiptTokenPositionResponse])
async def list_receipt_tokens(
    star_id: EthAddressPath,
    service: AllocationService = Depends(_get_service),
):
    star = await service.get_star(star_id)
    if star is None:
        raise HTTPException(status_code=404, detail="star not found")

    positions = await service.list_receipt_token_positions(star_id)

    result = []
    for p in positions:
        result.append(
            ReceiptTokenPositionResponse(
                receipt_token_id=p.receipt_token_id,
                symbol=p.symbol,
                underlying_symbol=p.underlying_symbol,
                protocol_name=p.protocol_name,
                balance=p.balance,
                token_address=p.token_address,
            )
        )

    return result


@router.get("/stars/{star_id}/allocations", response_model=list[AllocationPositionResponse])
async def list_allocations(
    star_id: EthAddressPath,
    block_number: int | None = None,
    service: AllocationService = Depends(_get_service),
):
    positions = await service.list_allocations_by_star(star_id, block_number)
    return [AllocationPositionResponse(**dataclasses.asdict(p)) for p in positions]
