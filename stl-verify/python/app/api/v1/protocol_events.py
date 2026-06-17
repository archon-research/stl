import logging
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.protocol_event_repository import ProtocolEventRepository
from app.api._validators import OptionalTxHashParam, TxHashParam
from app.api.deps import get_engine
from app.api.time_series import TimeSeriesWindow, apply_cache_control, build_window, get_time_series_query_params
from app.domain.time_series import TimeSeriesQuery, enforce_filter_for_window
from app.services.protocol_event_service import ProtocolEventService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["protocol events"])


class ProtocolEventResponse(BaseModel):
    """A single decoded protocol event observed on-chain."""

    tx_hash: str = Field(
        description="0x-prefixed transaction hash that emitted the event.",
        examples=["0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"],
    )
    log_index: int = Field(description="Index of the log within the transaction's receipt.", examples=[3])
    chain_id: int = Field(description="EVM chain id where the event was observed.", examples=[1])
    block_number: int = Field(description="Block number containing the event.", examples=[18000000])
    block_version: int = Field(
        description="Cache-key version that increments on chain reorgs.",
        examples=[1],
    )
    protocol_name: str = Field(description="Protocol the event was emitted by.", examples=["aave-v3"])
    event_name: str = Field(description="Decoded event name from the protocol's ABI.", examples=["Supply"])
    contract_address: str = Field(
        description="Lower-case 0x-prefixed contract address that emitted the event.",
        examples=["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
    )
    event_data: dict[str, Any] | None = Field(
        default=None,
        description="Decoded event arguments as a JSON object. Schema varies by `event_name`.",
    )
    created_at: datetime = Field(description="Server-side time the event row was persisted.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "log_index": 3,
                "chain_id": 1,
                "block_number": 18000000,
                "block_version": 1,
                "protocol_name": "aave-v3",
                "event_name": "Supply",
                "contract_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "event_data": {"reserve": "0x...", "user": "0x...", "amount": "1000000"},
                "created_at": "2026-05-07T12:00:00Z",
            }
        }
    }


class ProtocolEventBucketResponse(BaseModel):
    """Count of protocol events within a single time bucket."""

    bucket_start: datetime = Field(description="Inclusive start of the time bucket (UTC).")
    event_count: int = Field(description="Number of events in the bucket.", examples=[42])


class ProtocolEventsEnvelope(BaseModel):
    """Protocol events response: raw rows or aggregated time buckets."""

    mode: Literal["raw", "aggregated"] = Field(description="`raw` for events, `aggregated` for time buckets.")
    window: TimeSeriesWindow = Field(description="The window and resolution applied to this response.")
    data: list[ProtocolEventResponse] | list[ProtocolEventBucketResponse] = Field(
        description="Events when `mode=raw`, count buckets when `mode=aggregated`."
    )


async def _get_protocol_event_service(engine: AsyncEngine = Depends(get_engine)) -> ProtocolEventService:
    repository = ProtocolEventRepository(engine)
    return ProtocolEventService(repository)


@router.get(
    "/protocol-events",
    response_model=ProtocolEventsEnvelope,
    summary="List protocol events",
    description=(
        "List decoded protocol events with optional filters. Use `tx_hash` to fetch all "
        "events for a single transaction or `protocol_name` to scope to one protocol. "
        "Results are time-windowed (default last 24h) and returned newest first inside a "
        "`{mode, window, data}` envelope. Set `aggregate=true` to get per-bucket event counts."
    ),
)
async def list_protocol_events(
    response: Response,
    tx_hash: Annotated[
        OptionalTxHashParam,
        Query(description="Filter by transaction hash (0x-prefixed, 32 bytes)."),
    ] = None,
    protocol_name: str | None = Query(None, description="Filter by protocol name."),
    time_series: TimeSeriesQuery = Depends(get_time_series_query_params),
    limit: int = Query(100, ge=1, le=500, description="Max events returned (default 100, max 500)."),
    service: ProtocolEventService = Depends(_get_protocol_event_service),
) -> ProtocolEventsEnvelope:
    # A selective filter is one the index can seek to: tx_hash (exact byte
    # match) or protocol_name (exact equality, joins to indexed protocol_id).
    has_selective_filter = tx_hash is not None or protocol_name is not None
    try:
        enforce_filter_for_window(time_series, has_selective_filter=has_selective_filter)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    window = build_window(time_series)
    apply_cache_control(response, time_series)
    try:
        if time_series.aggregate:
            buckets = await service.list_event_buckets(
                tx_hash=tx_hash,
                protocol_name=protocol_name,
                from_timestamp=time_series.from_timestamp,
                to_timestamp=time_series.to_timestamp,
                bucket_seconds=time_series.bucket.total_seconds(),
                limit=limit,
            )
            return ProtocolEventsEnvelope(
                mode="aggregated",
                window=window,
                data=[ProtocolEventBucketResponse(**bucket.__dict__) for bucket in buckets],
            )

        events = await service.list_events(
            tx_hash=tx_hash,
            protocol_name=protocol_name,
            from_timestamp=time_series.from_timestamp,
            to_timestamp=time_series.to_timestamp,
            limit=limit,
        )
        return ProtocolEventsEnvelope(
            mode="raw",
            window=window,
            data=[ProtocolEventResponse(**event.__dict__) for event in events],
        )
    except ValueError as exc:
        logger.error(
            "Failed to retrieve protocol events",
            extra={
                "tx_hash": tx_hash,
                "protocol_name": protocol_name,
                "limit": limit,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Failed to retrieve protocol events") from exc


@router.get(
    "/tx/{tx_hash}/events",
    response_model=list[ProtocolEventResponse],
    summary="Get all events for a transaction",
    description=(
        "Return every decoded protocol event emitted within a single transaction, "
        "ordered by `log_index`. Returns an empty list if the transaction is unknown "
        "or did not emit any tracked protocol events."
    ),
)
async def get_tx_events(
    tx_hash: Annotated[
        TxHashParam,
        Path(description="0x-prefixed 32-byte transaction hash."),
    ],
    service: ProtocolEventService = Depends(_get_protocol_event_service),
) -> list[ProtocolEventResponse]:
    try:
        events = await service.get_events_by_tx(tx_hash)
        return [ProtocolEventResponse(**event.__dict__) for event in events]
    except ValueError as exc:
        logger.error(
            "Failed to retrieve transaction events",
            extra={
                "tx_hash": tx_hash,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Failed to retrieve transaction events") from exc
