import asyncio
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import ReferenceEffectiveAtProvider
from app.api._validators import ProxyAddressPathParam
from app.api.deps import (
    get_engine,
    get_reference_as_of,
    get_reference_capital_repository_factory,
    require_prime_view,
)
from app.api.provenance import (
    get_requested_provenance,
    resolve_or_422,
)
from app.api.time_series import (
    TimeSeriesWindow,
    apply_cache_control,
    build_window,
    get_time_series_query_params,
)
from app.domain.entities.allocation import EthAddress
from app.domain.provenance import Provenance
from app.domain.serialization import PlainDecimal
from app.domain.time_series import TimeSeriesQuery
from app.ports.reference_capital_repository import ReferenceCapitalRepository
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
    reference_exposure_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sky's reported exposure for the same bucket, populated only under `source=both`. "
            "Carried beside STL's rather than replacing it: the two are computed differently and "
            "differ by around a percent, which a reader needs shown rather than reconciled."
        ),
        examples=["1461200000.00"],
    )


class ExposureEnvelope(BaseModel):
    """Per-prime exposure time series, gap-filled into buckets."""

    mode: Literal["aggregated"] = Field(description="Always `aggregated`: a gap-filled time series.")
    source: Provenance = Field(
        default=Provenance.INDEXED,
        description=(
            "Provenance the series was answered from. `indexed` is STL's priced receipt-token "
            "exposure; `reference` is Sky's Star monitor as observed by STL's syncer; `both` fills "
            "`exposure_usd` and `reference_exposure_usd` on every bucket."
        ),
    )
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[ExposureBucketResponse] = Field(description="Priced exposure per time bucket.")


async def _get_service(
    engine: AsyncEngine = Depends(get_engine),
    reference_as_of: ReferenceEffectiveAtProvider = Depends(get_reference_as_of),
) -> AllocationService:
    return AllocationService(AllocationRepository(engine, reference_as_of))


def _merged_bucket_starts(*grids: dict) -> list:
    """Every bucket either provenance reported, newest first like each series."""
    starts = {start for grid in grids for start in grid}
    return sorted(starts, reverse=True)


async def _reference_exposure_by_bucket(
    prime_address: EthAddress,
    time_series: TimeSeriesQuery,
    limit: int,
    repository: ReferenceCapitalRepository,
) -> dict[datetime, Decimal | None]:
    """Sky's exposure keyed by bucket start.

    Both series are gap-filled over the same window and resolution, so their
    bucket grids are identical and a lookup cannot silently shift a value into a
    neighbouring bucket.
    """
    buckets = await repository.list_reference_capital_buckets(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        bucket_seconds=time_series.bucket.total_seconds(),
        limit=limit,
    )
    return {bucket.bucket_start: bucket.exposure_usd for bucket in buckets}


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
    prime_id: ProxyAddressPathParam,
    response: Response,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max buckets returned (default 100, max 500)."),
    requested_provenance: Provenance | None = Depends(get_requested_provenance),
    service: AllocationService = Depends(_get_service),
    reference_repositories: Callable[[], ReferenceCapitalRepository] = Depends(
        get_reference_capital_repository_factory
    ),
    _authz: None = Depends(require_prime_view),
) -> ExposureEnvelope:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    source = resolve_or_422(requested_provenance, available=frozenset(Provenance), default=Provenance.INDEXED)

    # Exposure observations are immutable once written, so a fully-pinned window
    # is safely cacheable; a defaulted (now-relative) window is not.
    apply_cache_control(response, time_series)
    window = build_window(time_series)

    if source is Provenance.REFERENCE:
        reference_buckets = await reference_repositories().list_reference_capital_buckets(
            prime_address,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            bucket_seconds=time_series.bucket.total_seconds(),
            limit=limit,
        )
        return ExposureEnvelope(
            mode="aggregated",
            source=source,
            window=window,
            data=[
                ExposureBucketResponse(bucket_start=bucket.bucket_start, exposure_usd=bucket.exposure_usd)
                for bucket in reference_buckets
            ],
        )

    if source is Provenance.BOTH:
        reference_by_bucket, buckets = await asyncio.gather(
            _reference_exposure_by_bucket(prime_address, time_series, limit, reference_repositories()),
            service.list_exposure_buckets(
                prime_address,
                from_timestamp=time_series.from_timestamp,
                to_timestamp=time_series.to_timestamp,
                bucket_seconds=time_series.bucket.total_seconds(),
                limit=limit,
            ),
        )
        indexed_by_bucket = {bucket.bucket_start: bucket.exposure_usd for bucket in buckets}
        return ExposureEnvelope(
            mode="aggregated",
            source=source,
            window=window,
            data=[
                ExposureBucketResponse(
                    bucket_start=bucket_start,
                    exposure_usd=indexed_by_bucket.get(bucket_start),
                    reference_exposure_usd=reference_by_bucket.get(bucket_start),
                )
                for bucket_start in _merged_bucket_starts(indexed_by_bucket, reference_by_bucket)
            ],
        )

    buckets = await service.list_exposure_buckets(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        bucket_seconds=time_series.bucket.total_seconds(),
        limit=limit,
    )
    return ExposureEnvelope(
        mode="aggregated",
        source=source,
        window=window,
        data=[ExposureBucketResponse(**bucket.__dict__) for bucket in buckets],
    )
