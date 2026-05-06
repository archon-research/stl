from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.prime_debt_repository import PostgresPrimeDebtRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine
from app.domain.entities.allocation import EthAddress
from app.services.prime_debt_service import PrimeDebtService

router = APIRouter()


class PrimeDebtSnapshotResponse(BaseModel):
    prime_address: str
    prime_name: str
    ilk_name: str
    debt_wad: Decimal
    block_number: int
    block_version: int
    synced_at: datetime


async def _get_prime_debt_service(engine: AsyncEngine = Depends(get_engine)) -> PrimeDebtService:
    return PrimeDebtService(PostgresPrimeDebtRepository(engine))


@router.get("/primes/{prime_id}/debt", response_model=list[PrimeDebtSnapshotResponse])
async def list_prime_debt_snapshots(
    prime_id: EthAddressParam,
    limit: int = Query(100, ge=1, le=500),
    service: PrimeDebtService = Depends(_get_prime_debt_service),
) -> list[PrimeDebtSnapshotResponse]:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    snapshots = await service.list_debt_snapshots(prime_address, limit=limit)
    return [PrimeDebtSnapshotResponse(**snapshot.__dict__) for snapshot in snapshots]
