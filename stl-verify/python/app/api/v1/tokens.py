from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.token_catalog_repository import PostgresTokenCatalogRepository
from app.api._validators import ChainIdPath, TokenAddressPath
from app.api.deps import get_engine
from app.api.v1._resolvers import resolve_token
from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote
from app.services.token_catalog_service import TokenCatalogService

router = APIRouter(tags=["tokens"])


class TokenResponse(BaseModel):
    """Token metadata entry from the token catalog."""

    id: int = Field(description="Surrogate token id, stable across chain reorgs.", examples=[12345])
    chain_id: int = Field(description="EVM chain id the token lives on.", examples=[1])
    address: str = Field(
        description="Lower-case 0x-prefixed contract address.",
        examples=["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
    )
    symbol: str | None = Field(default=None, description="ERC-20 symbol, when known.", examples=["USDC"])
    decimals: int | None = Field(default=None, description="ERC-20 decimals, when known.", examples=[6])
    updated_at: datetime = Field(description="Timestamp the catalog row was last refreshed.")
    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Free-form catalog metadata (e.g. logo URL, vendor ids).",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 12345,
                "chain_id": 1,
                "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "symbol": "USDC",
                "decimals": 6,
                "updated_at": "2026-05-01T12:00:00Z",
                "metadata": {"coingecko_id": "usd-coin"},
            }
        }
    }


class TokenPriceResponse(BaseModel):
    """Latest token price state.

    The endpoint returns `200` when the token exists even if no quote is currently
    available. In that case `is_stale=true`, `staleness_reason='missing_quote'`, and
    quote fields are `null`.
    """

    token_id: int = Field(description="Token identifier", examples=[12345])
    source_type: str | None = Field(
        default=None,
        description="Price source type (`onchain` or `offchain`) when available",
        examples=["offchain"],
    )
    source_id: int | None = Field(default=None, description="Source identifier when available", examples=[7])
    source_name: str | None = Field(
        default=None,
        description="Source machine name when available",
        examples=["coingecko"],
    )
    source_display_name: str | None = Field(
        default=None,
        description="Human-friendly source name when available",
        examples=["CoinGecko"],
    )
    price_usd: Decimal | None = Field(
        default=None,
        description=(
            "Latest USD price; null when no quote is available. "
            "Decimal serialized as a JSON string to preserve precision."
        ),
        examples=["1.0001"],
    )
    timestamp: datetime | None = Field(default=None, description="Timestamp of the latest quote; null when unavailable")
    staleness_seconds: int | None = Field(
        default=None,
        description="Age of the latest quote in seconds; null when unavailable",
        examples=[42],
    )
    is_stale: bool = Field(description="Whether quote data is stale or missing")
    staleness_reason: str | None = Field(
        default=None,
        description="Reason for stale state, e.g. `missing_quote`",
        examples=["missing_quote"],
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "token_id": 12345,
                "source_type": "offchain",
                "source_id": 7,
                "source_name": "coingecko",
                "source_display_name": "CoinGecko",
                "price_usd": "1.0001",
                "timestamp": "2026-05-07T12:00:00Z",
                "staleness_seconds": 42,
                "is_stale": False,
                "staleness_reason": None,
            }
        }
    }


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


@router.get(
    "/tokens",
    response_model=list[TokenResponse],
    summary="List tokens",
    description=(
        "List entries from the token catalog with optional filters. "
        "Use `chain_id` to scope to a single chain and `symbol` for a case-insensitive "
        "substring match against the symbol. Pagination is page-less; `limit` caps the page size."
    ),
)
async def list_tokens(
    chain_id: int | None = Query(default=None, description="Filter by EVM chain id.", examples=[1]),
    symbol: str | None = Query(
        default=None,
        description="Filter by symbol (case-insensitive substring match).",
        examples=["USDC"],
    ),
    limit: int = Query(default=100, ge=1, le=500, description="Max results (default 100, max 500)."),
    service: TokenCatalogService = Depends(_get_service),
) -> list[TokenResponse]:
    rows = await service.list_tokens(chain_id=chain_id, symbol=symbol, limit=limit)
    return [_to_token_response(row) for row in rows]


def _quote_to_price_response(token_id: int, quote: TokenPriceQuote | None) -> TokenPriceResponse:
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


@router.get(
    "/tokens/{token_id}",
    response_model=TokenResponse,
    summary="Get a token by id (deprecated)",
    description=(
        "Return the token catalog entry for the given surrogate `token_id`, or `404` if unknown.\n\n"
        "**Deprecated.** Prefer `/v1/tokens/{chain_id}/{token_address}` which addresses "
        "tokens by on-chain identity and avoids a discovery round-trip."
    ),
    deprecated=True,
)
async def get_token_by_id(token_id: int, service: TokenCatalogService = Depends(_get_service)) -> TokenResponse:
    row = await service.get_token(token_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Token not found")

    return _to_token_response(row)


@router.get(
    "/tokens/{token_id}/price",
    response_model=TokenPriceResponse,
    summary="Get latest token price (deprecated)",
    description=(
        "Return the latest USD price snapshot for a token.\n\n"
        "- Returns `404` only when the token does not exist.\n"
        "- Returns `200` with `is_stale=true` and `staleness_reason='missing_quote'` "
        "when the token exists but no quote is currently available.\n"
        "- `price_usd` is a decimal serialized as a JSON string to preserve precision.\n\n"
        "**Deprecated.** Prefer `/v1/tokens/{chain_id}/{token_address}/price`."
    ),
    deprecated=True,
)
async def get_token_price_by_id(
    token_id: int, service: TokenCatalogService = Depends(_get_service)
) -> TokenPriceResponse:
    token = await service.get_token(token_id)
    if token is None:
        raise HTTPException(status_code=404, detail="Token not found")

    quote = await service.get_latest_price(token_id)
    return _quote_to_price_response(token_id, quote)


@router.get(
    "/tokens/{chain_id}/{token_address}",
    response_model=TokenResponse,
    summary="Get a token by chain id and address",
    description=(
        "Return the token catalog entry for the ERC-20 at `(chain_id, token_address)`, "
        "or `404` if unknown. `token_address` is matched case-insensitively."
    ),
)
async def get_token(
    chain_id: ChainIdPath,
    token_address: TokenAddressPath,
    service: TokenCatalogService = Depends(_get_service),
) -> TokenResponse:
    row: TokenMetadata = await resolve_token(chain_id, token_address, service)
    return _to_token_response(row)


@router.get(
    "/tokens/{chain_id}/{token_address}/price",
    response_model=TokenPriceResponse,
    summary="Get latest token price by chain id and address",
    description=(
        "Return the latest USD price snapshot for the token at `(chain_id, token_address)`.\n\n"
        "- Returns `404` only when the token does not exist.\n"
        "- Returns `200` with `is_stale=true` and `staleness_reason='missing_quote'` "
        "when the token exists but no quote is currently available.\n"
        "- `price_usd` is a decimal serialized as a JSON string to preserve precision."
    ),
)
async def get_token_price(
    chain_id: ChainIdPath,
    token_address: TokenAddressPath,
    service: TokenCatalogService = Depends(_get_service),
) -> TokenPriceResponse:
    token = await resolve_token(chain_id, token_address, service)
    quote = await service.get_latest_price(token.id)
    return _quote_to_price_response(token.id, quote)
