from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine
from app.api.time_series import (
    TimeSeriesWindow,
    apply_cache_control,
    build_window,
    get_time_series_query_params,
)
from app.domain.entities.allocation import EthAddress
from app.domain.serialization import PlainDecimal
from app.domain.time_series import TimeSeriesQuery
from app.services.allocation_service import AllocationService

router = APIRouter(tags=["primes", "capital"])


class ExposureBucketResponse(BaseModel):
    """Priced receipt-token exposure within a single time bucket (LOCF gap-filled)."""

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    exposure_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sum across the prime's receipt-token positions of the carried-forward balance "
            "valued at the latest underlying oracle price (USD), serialized as a JSON string. "
            "`null` for leading buckets before the first observation."
        ),
        examples=["1459014561.88"],
    )


class ExposureEnvelope(BaseModel):
    """Per-prime exposure time series, gap-filled into buckets."""

    mode: Literal["aggregated"] = Field(description="Always `aggregated`: a gap-filled time series.")
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[ExposureBucketResponse] = Field(description="Priced exposure per time bucket.")


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(AllocationRepository(engine))


@router.get(
    "/primes/{prime_id}/exposure",
    response_model=ExposureEnvelope,
    summary="Prime exposure time series",
    description=(
        "Return the prime's priced receipt-token exposure over time, gap-filled (LOCF) into "
        "buckets. Per bucket, each receipt-token position's carried-forward balance is valued at "
        "the latest underlying oracle price and summed (the current `balance * price` exposure "
        "extended over time). Direct (non-receipt-token) holdings are excluded, matching "
        "the risk-capital exposure basis. Returns `404` if the prime is unknown. Defaults to the "
        "last 24h; pass a window and `resolution` for longer ranges."
    ),
)
async def list_prime_exposure(
    prime_id: EthAddressParam,
    response: Response,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max buckets returned (default 100, max 500)."),
    service: AllocationService = Depends(_get_service),
) -> ExposureEnvelope:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    # Exposure observations are immutable once written, so a fully-pinned window
    # is safely cacheable; a defaulted (now-relative) window is not.
    apply_cache_control(response, time_series)
    window = build_window(time_series)
    buckets = await service.list_exposure_buckets(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        bucket_seconds=time_series.bucket.total_seconds(),
        limit=limit,
    )
    return ExposureEnvelope(
        mode="aggregated",
        window=window,
        data=[ExposureBucketResponse(**bucket.__dict__) for bucket in buckets],
    )
