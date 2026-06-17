from datetime import datetime
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.capital_metrics_repository import PostgresCapitalMetricsRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine
from app.api.time_series import TimeSeriesWindow, build_window, get_time_series_query_params
from app.domain.entities.allocation import EthAddress
from app.domain.time_series import TimeSeriesQuery
from app.services.capital_metrics_history_service import CapitalMetricsHistoryService

router = APIRouter(tags=["primes", "capital"])


class CapitalMetricsSnapshotResponse(BaseModel):
    """A single observed capital-metrics position at a point in time."""

    prime_address: str = Field(
        description="Prime's 0x-prefixed Ethereum address.",
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    prime_name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    risk_capital: Decimal = Field(description="Risk capital exposure (USD).", examples=["10000000"])
    total_capital: Decimal = Field(description="Total RRC reported upstream (USD).", examples=["10000000"])
    first_loss_capital: Decimal = Field(description="First-loss capital (USD).", examples=["7500000"])
    capital_buffer: Decimal = Field(
        description="Derived `max(total_capital - first_loss_capital, 0)` (USD).",
        examples=["2500000"],
    )
    risk_to_capital_ratio: Decimal | None = Field(
        default=None,
        description="Upstream risk-tolerance ratio. `null` when not reported.",
        examples=["0.85"],
    )
    benchmark_source: str = Field(description="Upstream source the snapshot was sourced from.")
    synced_at: datetime = Field(description="Server-side time the snapshot was persisted.")


class CapitalMetricsBucketResponse(BaseModel):
    """Last observed capital metrics within a single time bucket (LOCF gap-filled)."""

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    risk_capital: Decimal | None = Field(default=None, description="Carried-forward risk capital (USD).")
    total_capital: Decimal | None = Field(default=None, description="Carried-forward total capital (USD).")
    first_loss_capital: Decimal | None = Field(default=None, description="Carried-forward first-loss capital (USD).")
    capital_buffer: Decimal | None = Field(
        default=None,
        description="Derived `max(total_capital - first_loss_capital, 0)`; `null` for leading gap-filled buckets.",
    )
    risk_to_capital_ratio: Decimal | None = Field(default=None, description="Carried-forward risk-to-capital ratio.")


class CapitalMetricsEnvelope(BaseModel):
    """Capital-metrics response: raw snapshots or aggregated time buckets."""

    mode: Literal["raw", "aggregated"] = Field(description="`raw` for snapshots, `aggregated` for time buckets.")
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[CapitalMetricsSnapshotResponse] | list[CapitalMetricsBucketResponse] = Field(
        description="Snapshots when `mode=raw`, value buckets when `mode=aggregated`."
    )


async def _get_capital_metrics_service(
    engine: AsyncEngine = Depends(get_engine),
) -> CapitalMetricsHistoryService:
    return CapitalMetricsHistoryService(PostgresCapitalMetricsRepository(engine))


@router.get(
    "/primes/{prime_id}/capital-metrics",
    response_model=CapitalMetricsEnvelope,
    summary="List prime capital-metrics snapshots",
    description=(
        "Return stored capital-metrics snapshots for a prime, newest first, inside a "
        "`{mode, window, data}` envelope. Results are time-windowed (default last 24h). "
        "Returns `404` if the prime is unknown. Set `aggregate=true` for the last value per "
        "time bucket (gap-filled). This is the historical series; the latest live values "
        "remain at `/v1/capital-metrics`."
    ),
)
async def list_prime_capital_metrics(
    prime_id: EthAddressParam,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max snapshots returned (default 100, max 500)."),
    service: CapitalMetricsHistoryService = Depends(_get_capital_metrics_service),
) -> CapitalMetricsEnvelope:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    window = build_window(time_series)
    if time_series.aggregate:
        buckets = await service.list_buckets(
            prime_address,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            bucket_seconds=time_series.bucket.total_seconds(),
            limit=limit,
        )
        return CapitalMetricsEnvelope(
            mode="aggregated",
            window=window,
            data=[CapitalMetricsBucketResponse(**bucket.__dict__) for bucket in buckets],
        )

    snapshots = await service.list_snapshots(
        prime_address,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        limit=limit,
    )
    return CapitalMetricsEnvelope(
        mode="raw",
        window=window,
        data=[CapitalMetricsSnapshotResponse(**snapshot.__dict__) for snapshot in snapshots],
    )
