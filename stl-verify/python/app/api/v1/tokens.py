from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.token_catalog_repository import PostgresTokenCatalogRepository
from app.api.deps import get_engine
from app.services.token_catalog_service import TokenCatalogService

router = APIRouter()


class TokenResponse(BaseModel):
    """Token metadata entry from the token catalog."""

    id: int
    chain_id: int
    address: str
    symbol: str | None
    decimals: int | None
    updated_at: datetime
    metadata: dict[str, Any] | None = None


class TokenPriceResponse(BaseModel):
    """Latest token price state.

    The endpoint returns `200` when the token exists even if no quote is currently
    available. In that case `is_stale=true`, `staleness_reason='missing_quote'`, and
    quote fields are `null`.
    """

    token_id: int = Field(description="Token identifier")
    source_type: str | None = Field(
        default=None, description="Price source type (`onchain` or `offchain`) when available"
    )
    source_id: int | None = Field(default=None, description="Source identifier when available")
    source_name: str | None = Field(default=None, description="Source machine name when available")
    source_display_name: str | None = Field(default=None, description="Human-friendly source name when available")
    price_usd: Decimal | None = Field(default=None, description="Latest USD price; null when no quote is available")
    timestamp: datetime | None = Field(default=None, description="Timestamp of the latest quote; null when unavailable")
    staleness_seconds: int | None = Field(
        default=None, description="Age of the latest quote in seconds; null when unavailable"
    )
    is_stale: bool = Field(description="Whether quote data is stale or missing")
    staleness_reason: str | None = Field(default=None, description="Reason for stale state, e.g. `missing_quote`")


def _to_token_response(row) -> TokenResponse:
    return TokenResponse(
        id=row.id,
        chain_id=row.chain_id,
        address=row.address,
        symbol=row.symbol,
        decimals=row.decimals,
        updated_at=row.updated_at,
        metadata=row.metadata,
    )


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> TokenCatalogService:
    return TokenCatalogService(PostgresTokenCatalogRepository(engine))


@router.get("/tokens", response_model=list[TokenResponse])
async def list_tokens(
    chain_id: int | None = None,
    symbol: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    service: TokenCatalogService = Depends(_get_service),
) -> list[TokenResponse]:
    rows = await service.list_tokens(chain_id=chain_id, symbol=symbol, limit=limit)
    return [_to_token_response(row) for row in rows]


@router.get("/tokens/{token_id}", response_model=TokenResponse)
async def get_token(token_id: int, service: TokenCatalogService = Depends(_get_service)) -> TokenResponse:
    row = await service.get_token(token_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Token not found")

    return _to_token_response(row)


@router.get("/tokens/{token_id}/price", response_model=TokenPriceResponse)
async def get_token_price(token_id: int, service: TokenCatalogService = Depends(_get_service)) -> TokenPriceResponse:
    """Return latest token price state.

    Returns `404` only when the token does not exist.
    Returns `200` with stale indicators when token exists but no quote is available.
    """
    token = await service.get_token(token_id)
    if token is None:
        raise HTTPException(status_code=404, detail="Token not found")

    quote = await service.get_latest_price(token_id)
    if quote is None:
        return TokenPriceResponse(
            token_id=token_id,
            source_type=None,
            source_id=None,
            source_name=None,
            source_display_name=None,
            price_usd=None,
            timestamp=None,
            staleness_seconds=None,
            is_stale=True,
            staleness_reason="missing_quote",
        )

    return TokenPriceResponse(
        token_id=quote.token_id,
        source_type=quote.source_type,
        source_id=quote.source_id,
        source_name=quote.source_name,
        source_display_name=quote.source_display_name,
        price_usd=quote.price_usd,
        timestamp=quote.timestamp,
        staleness_seconds=quote.staleness_seconds,
        is_stale=False,
        staleness_reason=None,
    )
