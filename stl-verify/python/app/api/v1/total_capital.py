from collections.abc import Callable
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import ProxyAddressPathParam
from app.api.deps import get_engine, get_reference_capital_repository_factory
from app.api.time_series import (
    TimeSeriesWindow,
    apply_cache_control,
    build_window,
    get_time_series_query_params,
)
from app.domain.entities.allocation import EthAddress
from app.domain.serialization import PlainDecimal
from app.domain.time_series import TimeSeriesQuery
from app.ports.reference_capital_repository import ReferenceCapitalRepository
from app.services.allocation_service import AllocationService

router = APIRouter(tags=["primes", "capital"])


class TotalCapitalBucketResponse(BaseModel):
    """Last observed capital figures within a single time bucket (LOCF gap-filled).

    Only ``total_capital_usd`` is served in both modes. The other two are
    reference-only and come from two different upstream feeds, so each is null
    outside the range its own feed covers.
    """

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    total_capital_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Last observed SubProxy treasury USDS balance carried forward into the bucket "
            "(USD; USDS is dollar-pegged), serialized as a JSON string. `null` for leading "
            "buckets before the first observation."
        ),
        examples=["36359440.25"],
    )
    assets_usd: PlainDecimal | None = Field(
        default=None,
        description=(
            "Total assets the prime holds, as published upstream — the figure Sky's dashboard "
            "labels PRIME COLLATERAL. Reference mode only, and `null` outside the range the "
            "balance-sheet feed covers. STL computes no equivalent: its own asset total omits "
            "sources it does not index (PSM3, Curve LP valuations), so it is not served here."
        ),
        examples=["3190000000.00"],
    )
    encumbrance_ratio: PlainDecimal | None = Field(
        default=None,
        description=(
            "`required_risk_capital / total_risk_capital` as the monitor reported it (0-1). "
            "Reference mode only, and `null` for buckets covered by backfilled history alone: "
            "the balance-sheet feed carries no encumbrance figure."
        ),
        examples=["0.9397"],
    )


class TotalCapitalEnvelope(BaseModel):
    """Per-prime total-capital time series, gap-filled into buckets."""

    mode: Literal["aggregated"] = Field(description="Always `aggregated`: a gap-filled time series.")
    source: Literal["self", "reference"] = Field(
        default="self",
        description=(
            "Provenance of the series. `self` is the on-chain SubProxy treasury; `reference` is "
            "Sky's Star monitor as observed by STL's syncer, returned when `reference=true`."
        ),
    )
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[TotalCapitalBucketResponse] = Field(
        description="Last observed capital figures per time bucket, newest first."
    )


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
        "`total_capital`. Under `reference=true` each bucket also carries `assets_usd` "
        "(the upstream PRIME COLLATERAL figure) and the monitor's `encumbrance_ratio`. "
        "Returns `404` if the prime is unknown. Defaults to the last 24h; "
        "pass a window and `resolution` for longer ranges."
    ),
)
async def list_prime_total_capital(
    prime_id: ProxyAddressPathParam,
    response: Response,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max buckets returned (default 100, max 500)."),
    reference: bool = Query(
        False,
        description=(
            "Serve Sky's Star monitor figures instead of the on-chain treasury. The shape is "
            "unchanged; `source` reports the provenance. The reference series only extends back to "
            "when STL first observed the monitor — it publishes no history of its own — so earlier "
            "buckets are `null`, meaning not yet observed rather than zero."
        ),
    ),
    service: AllocationService = Depends(_get_service),
    reference_repositories: Callable[[], ReferenceCapitalRepository] = Depends(
        get_reference_capital_repository_factory
    ),
) -> TotalCapitalEnvelope:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    # Treasury observations are immutable once written, so a fully-pinned window
    # is safely cacheable; a defaulted (now-relative) window is not.
    apply_cache_control(response, time_series)
    window = build_window(time_series)

    if reference:
        reference_buckets = await reference_repositories().list_reference_capital_buckets(
            prime_address,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            bucket_seconds=time_series.bucket.total_seconds(),
            limit=limit,
        )
        return TotalCapitalEnvelope(
            mode="aggregated",
            source="reference",
            window=window,
            data=[
                TotalCapitalBucketResponse(
                    bucket_start=bucket.bucket_start,
                    total_capital_usd=bucket.total_capital_usd,
                    assets_usd=bucket.assets_usd,
                    encumbrance_ratio=bucket.encumbrance_ratio,
                )
                for bucket in reference_buckets
            ],
        )

    buckets = await service.list_total_capital_buckets(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        bucket_seconds=time_series.bucket.total_seconds(),
        limit=limit,
    )
    return TotalCapitalEnvelope(
        mode="aggregated",
        source="self",
        window=window,
        data=[TotalCapitalBucketResponse(**bucket.__dict__) for bucket in buckets],
    )
