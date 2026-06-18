from datetime import datetime
from decimal import Decimal
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
from app.domain.time_series import TimeSeriesQuery
from app.services.allocation_service import AllocationService

router = APIRouter(tags=["primes", "capital"])


class TotalCapitalBucketResponse(BaseModel):
    """Last observed treasury balance within a single time bucket (LOCF gap-filled)."""

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    total_capital_usd: Decimal | None = Field(
        default=None,
        description=(
            "Last observed SubProxy treasury USDS balance carried forward into the bucket "
            "(USD; USDS is dollar-pegged), serialized as a JSON string. `null` for leading "
            "buckets before the first observation."
        ),
        examples=["36359440.25"],
    )


class TotalCapitalEnvelope(BaseModel):
    """Per-prime total-capital time series, gap-filled into buckets."""

    mode: Literal["aggregated"] = Field(description="Always `aggregated`: a gap-filled time series.")
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[TotalCapitalBucketResponse] = Field(description="Last treasury balance per time bucket.")


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(AllocationRepository(engine))


@router.get(
    "/primes/{prime_id}/total-capital",
    response_model=TotalCapitalEnvelope,
    summary="Prime total-capital (treasury) time series",
    description=(
        "Return the prime's total capital over time, gap-filled (LOCF) into buckets. Total "
        "capital is the treasury USDS held in the prime's SubProxy wallet (USDS is "
        "dollar-pegged, so the balance is the USD figure); it matches the upstream Star "
        "`total_capital`. Returns `404` if the prime is unknown. Defaults to the last 24h; "
        "pass a window and `resolution` for longer ranges."
    ),
)
async def list_prime_total_capital(
    prime_id: EthAddressParam,
    response: Response,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max buckets returned (default 100, max 500)."),
    service: AllocationService = Depends(_get_service),
) -> TotalCapitalEnvelope:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    # Treasury observations are immutable once written, so a fully-pinned window
    # is safely cacheable; a defaulted (now-relative) window is not.
    apply_cache_control(response, time_series)
    window = build_window(time_series)
    buckets = await service.list_total_capital_buckets(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        bucket_seconds=time_series.bucket.total_seconds(),
        limit=limit,
    )
    return TotalCapitalEnvelope(
        mode="aggregated",
        window=window,
        data=[TotalCapitalBucketResponse(**bucket.__dict__) for bucket in buckets],
    )
