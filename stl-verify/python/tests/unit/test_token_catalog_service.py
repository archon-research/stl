from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote
from app.services.token_catalog_service import TokenCatalogService


def _token() -> TokenMetadata:
    return TokenMetadata(
        id=1,
        chain_id=1,
        address="0x" + "ab" * 20,
        symbol="USDC",
        decimals=6,
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={"kind": "stable"},
    )


def _quote() -> TokenPriceQuote:
    return TokenPriceQuote(
        token_id=1,
        source_type="onchain",
        source_id=1,
        source_name="chainlink",
        source_display_name="Chainlink",
        price_usd=Decimal("1.00"),
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        staleness_seconds=30,
    )


@pytest.mark.asyncio
async def test_list_tokens_delegates_with_defaults() -> None:
    repo = AsyncMock()
    token = _token()
    repo.list_tokens.return_value = [token]
    service = TokenCatalogService(repo)

    result = await service.list_tokens()

    assert result == [token]
    repo.list_tokens.assert_awaited_once_with(chain_id=None, symbol=None, limit=100)


@pytest.mark.asyncio
async def test_list_tokens_delegates_filters() -> None:
    repo = AsyncMock()
    repo.list_tokens.return_value = []
    service = TokenCatalogService(repo)

    result = await service.list_tokens(chain_id=1, symbol="US", limit=5)

    assert result == []
    repo.list_tokens.assert_awaited_once_with(chain_id=1, symbol="US", limit=5)


@pytest.mark.asyncio
async def test_get_token_and_latest_price_passthrough() -> None:
    repo = AsyncMock()
    token = _token()
    quote = _quote()
    repo.get_token.return_value = token
    repo.get_latest_price.return_value = quote
    service = TokenCatalogService(repo)

    fetched_token = await service.get_token(1)
    fetched_quote = await service.get_latest_price(1)

    assert fetched_token == token
    assert fetched_quote == quote
    repo.get_token.assert_awaited_once_with(1)
    repo.get_latest_price.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_get_latest_price_propagates_repository_error() -> None:
    repo = AsyncMock()
    repo.get_latest_price.side_effect = ValueError("db failure")
    service = TokenCatalogService(repo)

    with pytest.raises(ValueError, match="db failure"):
        await service.get_latest_price(1)
