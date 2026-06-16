from datetime import datetime
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.prime_debt_repository import PostgresPrimeDebtRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine
from app.api.time_series import TimeSeriesWindow, build_window, get_time_series_query_params
from app.domain.entities.allocation import EthAddress
from app.domain.time_series import TimeSeriesQuery
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


class PrimeDebtBucketResponse(BaseModel):
    """Last observed debt within a single time bucket (LOCF gap-filled)."""

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    debt_wad: Decimal | None = Field(
        default=None,
        description=(
            "Last observed debt in `wad` units carried forward into the bucket, serialized as a "
            "JSON string. `null` for leading buckets before the first observation."
        ),
        examples=["1234567890000000000000"],
    )


class PrimeDebtEnvelope(BaseModel):
    """Prime debt response: raw snapshots or aggregated time buckets."""

    mode: Literal["raw", "aggregated"] = Field(description="`raw` for snapshots, `aggregated` for time buckets.")
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[PrimeDebtSnapshotResponse] | list[PrimeDebtBucketResponse] = Field(
        description="Snapshots when `mode=raw`, value buckets when `mode=aggregated`."
    )


async def _get_prime_debt_service(engine: AsyncEngine = Depends(get_engine)) -> PrimeDebtService:
    return PrimeDebtService(PostgresPrimeDebtRepository(engine))


@router.get(
    "/primes/{prime_id}/debt",
    response_model=PrimeDebtEnvelope,
    summary="List prime debt snapshots",
    description=(
        "Return debt snapshots for a prime, newest first, inside a `{mode, window, data}` "
        "envelope. Results are time-windowed (default last 24h). Returns `404` if the prime "
        "is unknown. Each snapshot carries the `block_number`/`block_version` it was observed "
        "at; consumers can use `block_version` to detect reorg-driven re-emissions. Set "
        "`aggregate=true` for the last debt value per time bucket (gap-filled)."
    ),
)
async def list_prime_debt_snapshots(
    prime_id: EthAddressParam,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max snapshots returned (default 100, max 500)."),
    service: PrimeDebtService = Depends(_get_prime_debt_service),
) -> PrimeDebtEnvelope:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    window = build_window(time_series)
    if time_series.aggregate:
        buckets = await service.list_debt_buckets(
            prime_address,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            bucket_seconds=time_series.bucket.total_seconds(),
            limit=limit,
        )
        return PrimeDebtEnvelope(
            mode="aggregated",
            window=window,
            data=[PrimeDebtBucketResponse(**bucket.__dict__) for bucket in buckets],
        )

    snapshots = await service.list_debt_snapshots(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        limit=limit,
    )
    return PrimeDebtEnvelope(
        mode="raw",
        window=window,
        data=[PrimeDebtSnapshotResponse(**snapshot.__dict__) for snapshot in snapshots],
    )
