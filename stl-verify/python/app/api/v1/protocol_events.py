import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.protocol_event_repository import PostgresProtocolEventRepository
from app.api._validators import TX_HASH_PATTERN
from app.api.deps import get_engine
from app.services.protocol_event_service import ProtocolEventService

logger = logging.getLogger(__name__)
router = APIRouter()


# Pydantic models for request/response serialization
class ProtocolEventResponse(BaseModel):
    """Response model for protocol event."""

    tx_hash: str
    log_index: int
    chain_id: int
    block_number: int
    block_version: int
    protocol_name: str
    event_name: str
    contract_address: str
    event_data: dict[str, Any] | None
    created_at: datetime


# Dependency injection for services
async def _get_protocol_event_service(engine: AsyncEngine = Depends(get_engine)) -> ProtocolEventService:
    """Get protocol event service with engine."""
    repository = PostgresProtocolEventRepository(engine)
    return ProtocolEventService(repository)


# Protocol Event endpoints
@router.get("/protocol-events", response_model=list[ProtocolEventResponse])
async def list_protocol_events(
    tx_hash: str | None = Query(
        None,
        pattern=TX_HASH_PATTERN,
        description="Filter by transaction hash",
    ),
    protocol_name: str | None = Query(None, description="Filter by protocol name"),
    limit: int = Query(100, ge=1, le=500, description="Limit number of results"),
    service: ProtocolEventService = Depends(_get_protocol_event_service),
) -> list[ProtocolEventResponse]:
    """List protocol events with optional filtering."""
    try:
        events = await service.list_events(tx_hash=tx_hash, protocol_name=protocol_name, limit=limit)
        return [ProtocolEventResponse(**event.__dict__) for event in events]
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


@router.get("/tx/{tx_hash}/events", response_model=list[ProtocolEventResponse])
async def get_tx_events(
    tx_hash: str = Path(..., pattern=TX_HASH_PATTERN),
    service: ProtocolEventService = Depends(_get_protocol_event_service),
) -> list[ProtocolEventResponse]:
    """Get all events for a transaction."""
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
