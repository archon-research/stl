from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.token_catalog_repository import PostgresTokenCatalogRepository
from app.api.deps import get_engine
from app.services.token_catalog_service import TokenCatalogService

router = APIRouter()


class TokenResponse(BaseModel):
    id: int
    chain_id: int
    address: str
    symbol: str | None
    decimals: int | None
    updated_at: datetime
    metadata: dict[str, Any] | None = None


class TokenPriceResponse(BaseModel):
    token_id: int
    source_type: str
    source_id: int
    source_name: str
    source_display_name: str | None
    price_usd: Decimal
    timestamp: datetime
    staleness_seconds: int


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
    token = await service.get_token(token_id)
    if token is None:
        raise HTTPException(status_code=404, detail="Token not found")

    quote = await service.get_latest_price(token_id)
    if quote is None:
        raise HTTPException(status_code=404, detail="No price quote found for token")

    return TokenPriceResponse(
        token_id=quote.token_id,
        source_type=quote.source_type,
        source_id=quote.source_id,
        source_name=quote.source_name,
        source_display_name=quote.source_display_name,
        price_usd=quote.price_usd,
        timestamp=quote.timestamp,
        staleness_seconds=quote.staleness_seconds,
    )
