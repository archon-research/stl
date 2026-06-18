from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.token_catalog_repository import (
    TokenCatalogRepository,
    _escape_like_pattern,
    _normalize_metadata,
    _normalize_symbol,
    _safe_decimal,
)


def _engine_with_fetchall(rows):
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchall=MagicMock(return_value=rows)))
    engine.connect.return_value = conn
    return engine, conn


def _engine_with_fetchone(row):
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=row)))
    engine.connect.return_value = conn
    return engine, conn


def test_escape_like_pattern_escapes_meta_characters() -> None:
    assert _escape_like_pattern("a%b_c\\d") == "a\\%b\\_c\\\\d"


def test_safe_decimal_valid_and_invalid() -> None:
    assert _safe_decimal("1.23", "price_usd") == Decimal("1.23")
    with pytest.raises(ValueError, match="invalid numeric value"):
        _safe_decimal("NaN", "price_usd", 1)


def test_normalize_metadata_returns_dict_or_none() -> None:
    assert _normalize_metadata({"k": "v"}) == {"k": "v"}
    assert _normalize_metadata("not-dict") is None
    assert _normalize_metadata(None) is None


def test_normalize_symbol_maps_blank_to_none() -> None:
    assert _normalize_symbol("USDC") == "USDC"
    assert _normalize_symbol("  DAI ") == "DAI"
    assert _normalize_symbol("") is None
    assert _normalize_symbol("   ") is None
    assert _normalize_symbol(None) is None


@pytest.mark.asyncio
async def test_list_tokens_tolerates_blank_symbol_rows() -> None:
    # A catalog row with an empty-string symbol must not fail the whole listing;
    # the adapter maps it to the domain's "absent" (None).
    blank = SimpleNamespace(
        id=2,
        chain_id=1,
        address="cd" * 20,
        symbol="",
        decimals=18,
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata=None,
    )
    engine, _ = _engine_with_fetchall([blank])
    repo = TokenCatalogRepository(engine)

    result = await repo.list_tokens(limit=10)

    assert len(result) == 1
    assert result[0].symbol is None


@pytest.mark.asyncio
async def test_list_tokens_escapes_symbol_and_clamps_limit() -> None:
    row = SimpleNamespace(
        id=1,
        chain_id=1,
        address="ab" * 20,
        symbol="USDC",
        decimals=6,
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={"kind": "stable"},
    )
    engine, conn = _engine_with_fetchall([row])
    repo = TokenCatalogRepository(engine)

    result = await repo.list_tokens(symbol="USD%", limit=1000)

    assert len(result) == 1
    assert result[0].address == "0x" + "ab" * 20
    params = conn.execute.await_args.args[1]
    assert params["symbol"] == "%USD\\%%"
    assert params["limit"] == 500


@pytest.mark.asyncio
async def test_get_latest_price_maps_non_negative_staleness() -> None:
    row = SimpleNamespace(
        token_id=1,
        source_type="onchain",
        source_id=1,
        source_name="chainlink",
        source_display_name="Chainlink",
        price_usd="1.0",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        staleness_seconds=-5,
    )
    engine, _ = _engine_with_fetchone(row)
    repo = TokenCatalogRepository(engine)

    quote = await repo.get_latest_price(1)

    assert quote is not None
    assert quote.price_usd == Decimal("1.0")
    assert quote.staleness_seconds == 0


@pytest.mark.asyncio
async def test_get_latest_price_raises_on_invalid_decimal() -> None:
    row = SimpleNamespace(
        token_id=1,
        source_type="onchain",
        source_id=1,
        source_name="chainlink",
        source_display_name="Chainlink",
        price_usd="NaN",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        staleness_seconds=12,
    )
    engine, _ = _engine_with_fetchone(row)
    repo = TokenCatalogRepository(engine)

    with pytest.raises(ValueError, match="fetching price for token"):
        await repo.get_latest_price(1)
