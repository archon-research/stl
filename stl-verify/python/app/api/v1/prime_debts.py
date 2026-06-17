from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.prime_debt_repository import PrimeDebtRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine
from app.domain.entities.allocation import EthAddress
from app.services.prime_debt_service import PrimeDebtService

router = APIRouter(tags=["primes"])


class PrimeDebtSnapshotResponse(BaseModel):
    """A single observed prime-debt position at a point in time."""

    prime_address: str = Field(
        description="Prime's 0x-prefixed Ethereum address.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    prime_name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    ilk_name: str = Field(
        description="Maker `ilk` (collateral type) the debt is denominated against.",
        examples=["ALLOCATOR-NEXUS-A"],
    )
    debt_wad: Decimal = Field(
        description=(
            "Outstanding debt in MakerDAO `wad` units (1e18 fixed-point). "
            "Decimal serialized as a JSON string to preserve precision."
        ),
        examples=["1234567890000000000000"],
    )
    block_number: int = Field(description="Block number the snapshot was observed at.", examples=[18000000])
    block_version: int = Field(
        description="Cache-key version that increments on chain reorgs.",
        examples=[1],
    )
    synced_at: datetime = Field(description="Server-side time the snapshot was persisted.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "prime_address": "0x1234567890abcdef1234567890abcdef12345678",
                "prime_name": "Acme Prime",
                "ilk_name": "ALLOCATOR-NEXUS-A",
                "debt_wad": "1234567890000000000000",
                "block_number": 18000000,
                "block_version": 1,
                "synced_at": "2026-05-07T12:00:00Z",
            }
        }
    }


async def _get_prime_debt_service(engine: AsyncEngine = Depends(get_engine)) -> PrimeDebtService:
    return PrimeDebtService(PrimeDebtRepository(engine))


@router.get(
    "/primes/{prime_id}/debt",
    response_model=list[PrimeDebtSnapshotResponse],
    summary="List prime debt snapshots",
    description=(
        "Return recent debt snapshots for a prime, newest first. Returns `404` if the prime "
        "is unknown. Each row carries the `block_number`/`block_version` it was observed at; "
        "consumers can use `block_version` to detect reorg-driven re-emissions."
    ),
)
async def list_prime_debt_snapshots(
    prime_id: EthAddressParam,
    limit: int = Query(100, ge=1, le=500, description="Max snapshots returned (default 100, max 500)."),
    service: PrimeDebtService = Depends(_get_prime_debt_service),
) -> list[PrimeDebtSnapshotResponse]:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    snapshots = await service.list_debt_snapshots(prime_address, limit=limit)
    return [PrimeDebtSnapshotResponse(**snapshot.__dict__) for snapshot in snapshots]
