import asyncio
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.prime_debt_repository import PrimeDebtRepository
from app.api._validators import PrimeOrProxyAddressPathParam
from app.api.deps import get_engine, require_prime_view
from app.api.provenance import (
    get_requested_provenance,
    resolve_or_422,
)
from app.api.time_series import TimeSeriesWindow, build_window, get_time_series_query_params
from app.domain.entities.allocation import EthAddress
from app.domain.provenance import Provenance
from app.domain.serialization import PlainDecimal
from app.domain.time_series import TimeSeriesQuery
from app.services.prime_debt_service import PrimeDebtService

router = APIRouter(tags=["primes"])


class PrimeDebtSnapshotResponse(BaseModel):
    """A single observed prime-debt position at a point in time."""

    prime_address: str = Field(
        description=(
            "The prime's on-chain vault address — the same value served as `prime_vault_address` "
            "elsewhere in this API (e.g. `/v1/primes`)."
        ),
        examples=["0x1234567890abcdef1234567890abcdef12345678"],
    )
    prime_name: str = Field(description="Human-readable prime name.", examples=["Acme Prime"])
    ilk_name: str = Field(
        description="Maker `ilk` (collateral type) the debt is denominated against.",
        examples=["ALLOCATOR-NEXUS-A"],
    )
    debt_wad: PlainDecimal = Field(
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
    debt_wad: PlainDecimal | None = Field(
        default=None,
        description=(
            "Last observed debt in `wad` units carried forward into the bucket, serialized as a "
            "JSON string. `null` for leading buckets before the first observation."
        ),
        examples=["1234567890000000000000"],
    )
    reference_debt_wad: PlainDecimal | None = Field(
        default=None,
        description=(
            "Sky's reported debt for the same bucket in the same unit, populated only under "
            "`source=both`. Beside the on-chain figure rather than replacing it."
        ),
        examples=["2645260280720000000000000000"],
    )


class PrimeDebtEnvelope(BaseModel):
    """Prime debt response: raw snapshots or aggregated time buckets."""

    mode: Literal["raw", "aggregated"] = Field(description="`raw` for snapshots, `aggregated` for time buckets.")
    source: Provenance = Field(
        default=Provenance.INDEXED,
        description=(
            "Provenance the series was answered from. `indexed` is the on-chain per-ilk debt; "
            "`reference` is Sky's own reported figure; `both` fills `debt_wad` and `reference_debt_wad` "
            "on every bucket, leaving either null where that provenance reported nothing. Raw "
            "snapshots are always `indexed`."
        ),
    )
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[PrimeDebtSnapshotResponse] | list[PrimeDebtBucketResponse] = Field(
        description="Snapshots when `mode=raw`, value buckets when `mode=aggregated`."
    )


async def _get_prime_debt_service(engine: AsyncEngine = Depends(get_engine)) -> PrimeDebtService:
    return PrimeDebtService(PrimeDebtRepository(engine))


@router.get(
    "/primes/{prime_id}/debt",
    response_model=PrimeDebtEnvelope,
    summary="List prime debt snapshots",
    description=(
        "Return debt snapshots for a prime, newest first, inside a `{mode, window, data}` "
        "envelope. Results are time-windowed (default last 24h). Returns `404` if the prime "
        "is unknown. Each snapshot carries the `block_number`/`block_version` it was observed "
        "at; consumers can use `block_version` to detect reorg-driven re-emissions. Set "
        "`aggregate=true` for the last debt value per time bucket (gap-filled). Pass "
        "`source=reference` (with `aggregate=true`) for Sky's own reported debt instead of the "
        "on-chain per-ilk figure, or `source=both` to carry each in its own field on every "
        "bucket; `source` reports which provenance answered."
    ),
)
async def list_prime_debt_snapshots(
    prime_id: PrimeOrProxyAddressPathParam,
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max snapshots returned (default 100, max 500)."),
    requested_provenance: Provenance | None = Depends(get_requested_provenance),
    service: PrimeDebtService = Depends(_get_prime_debt_service),
    _authz: None = Depends(require_prime_view),
) -> PrimeDebtEnvelope:
    resolved_prime_id = await service.resolve_prime_id(EthAddress(prime_id))
    if resolved_prime_id is None:
        raise HTTPException(status_code=404, detail="Prime not found")

    source = resolve_or_422(requested_provenance, available=frozenset(Provenance), default=Provenance.INDEXED)

    if source in (Provenance.REFERENCE, Provenance.BOTH) and not time_series.aggregate:
        raise HTTPException(
            status_code=400,
            detail=(
                "Reference debt is only available aggregated; upstream reports one figure per prime "
                "per day and carries no ilk or block identity. Retry with aggregate=true."
            ),
        )

    window = build_window(time_series)
    if source is Provenance.BOTH and time_series.aggregate:
        reference_buckets, buckets = await asyncio.gather(
            service.list_reference_debt_buckets(
                resolved_prime_id,
                from_timestamp=time_series.from_timestamp,
                to_timestamp=time_series.to_timestamp,
                bucket_seconds=time_series.bucket.total_seconds(),
                limit=limit,
            ),
            service.list_debt_buckets(
                resolved_prime_id,
                from_timestamp=time_series.from_timestamp,
                to_timestamp=time_series.to_timestamp,
                bucket_seconds=time_series.bucket.total_seconds(),
                limit=limit,
            ),
        )
        # Same window and resolution on both, so the bucket grids align.
        reference_by_bucket = {bucket.bucket_start: bucket.debt_wad for bucket in reference_buckets}
        indexed_by_bucket = {bucket.bucket_start: bucket.debt_wad for bucket in buckets}
        return PrimeDebtEnvelope(
            mode="aggregated",
            source=source,
            window=window,
            data=[
                PrimeDebtBucketResponse(
                    bucket_start=start,
                    debt_wad=indexed_by_bucket.get(start),
                    reference_debt_wad=reference_by_bucket.get(start),
                )
                for start in sorted(set(indexed_by_bucket) | set(reference_by_bucket), reverse=True)
            ],
        )

    if time_series.aggregate:
        read_buckets = (
            service.list_reference_debt_buckets if source is Provenance.REFERENCE else service.list_debt_buckets
        )
        buckets = await read_buckets(
            resolved_prime_id,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            bucket_seconds=time_series.bucket.total_seconds(),
            limit=limit,
        )
        return PrimeDebtEnvelope(
            mode="aggregated",
            source=source,
            window=window,
            data=[PrimeDebtBucketResponse(**bucket.__dict__) for bucket in buckets],
        )

    snapshots = await service.list_debt_snapshots(
        resolved_prime_id,
        from_timestamp=time_series.from_timestamp,
        to_timestamp=time_series.to_timestamp,
        limit=limit,
    )
    return PrimeDebtEnvelope(
        mode="raw",
        source=Provenance.INDEXED,
        window=window,
        data=[PrimeDebtSnapshotResponse(**snapshot.__dict__) for snapshot in snapshots],
    )
