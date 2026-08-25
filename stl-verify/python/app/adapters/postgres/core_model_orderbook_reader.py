"""Live sell-side order books for the CORE model, from cex_orderbook_snapshots.

Replaces the static ``*_sell_orderbook.parquet`` snapshots. Aggregation mirrors
what BA's parquet set did: one merged multi-venue book per canonical asset,
with derivative tokens riding their underlying's book. Inspection of the
parquet inputs confirmed the LST books are the raw ETH book duplicated with
**no price rescaling**, so routing a token to its group book reproduces the
original semantics exactly.

Only the books our venues actually track can be served (see
``app/risk_engine/core_model/DATA_GAPS.md``). A market whose collateral has no
live book fails loudly rather than falling back to a stale parquet file.
"""

import json
import logging
from datetime import timedelta
from typing import cast

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# Canonical book -> the symbol each venue stores in cex_orderbook_snapshots
# (verified against staging: Coinbase BTC-USD, OKX BTC-USDT, Kraken XBT/USD).
# USDT quotes are treated as USD. New venues/symbols extend these lists.
_BOOK_SYMBOLS: dict[str, list[str]] = {
    "BTC": ["BTC-USD", "BTC-USDT", "XBT/USD"],
    "ETH": ["ETH-USD", "ETH-USDT", "ETH/USD"],
}

# Token -> canonical book, matching the routing table in the model README:
# ETH LSTs are proxied via the ETH spot book, BTC wrappers via the BTC book.
ETH_GROUP = frozenset({"ETH", "WETH", "STETH", "WSTETH", "WEETH", "RETH", "RSETH", "EZETH"})
BTC_GROUP = frozenset({"BTC", "WBTC", "LBTC", "TBTC", "CBBTC"})

# One snapshot per venue, not per (venue, symbol): a venue listing two symbols
# from the same book list (BTC-USD and BTC-USDT, say) must not count its depth
# twice.
_LATEST_FRESH_PER_VENUE = text("""
    SELECT DISTINCT ON (exchange) exchange, symbol, persisted_at, asks
    FROM cex_orderbook_snapshots
    WHERE symbol = ANY(:symbols)
      AND persisted_at > now() - CAST(:max_age AS interval)
    ORDER BY exchange, persisted_at DESC
""")


def book_for(token: str) -> str:
    """Canonical book symbol a collateral token's liquidity is read from."""
    upper = token.upper()
    if upper in ETH_GROUP:
        return "ETH"
    if upper in BTC_GROUP:
        return "BTC"
    return upper


def merge_asks(asks_per_venue: list[list[list[str]]]) -> pd.DataFrame:
    """Merge per-venue ask levels into one book: price, sz, liquidity.

    Levels stay separate rows (depth at the same price on two venues is twice
    the depth), sorted by price ascending as the liquidator consumes them.
    """
    rows: list[tuple[float, float]] = []
    for levels in asks_per_venue:
        for price_str, size_str in levels:
            price, sz = float(price_str), float(size_str)
            if price > 0 and sz > 0:
                rows.append((price, sz))
    if not rows:
        raise ValueError("no ask levels after merging venues")
    df = pd.DataFrame(rows, columns=["price", "sz"]).sort_values("price", ignore_index=True)
    df["liquidity"] = df["price"] * df["sz"]
    return df


class PostgresOrderbookReader:
    """Serves ``get_orderbooks`` from live venue snapshots.

    ``max_age`` bounds how stale the freshest snapshot per venue may be. The
    feeds tick every second, so a book older than minutes means the indexer is
    down — and a liquidity model quietly running on a dead book is exactly the
    silent-partial-data failure the repo rules forbid.
    """

    def __init__(self, engine: AsyncEngine, max_age: timedelta = timedelta(minutes=30)) -> None:
        self._engine = engine
        self._max_age = max_age

    async def get_orderbooks(self, collateral_list: list[str]) -> dict[str, pd.DataFrame]:
        books: dict[str, pd.DataFrame] = {}
        for book_symbol in sorted({book_for(token) for token in collateral_list}):
            books[book_symbol] = await self._load_book(book_symbol)
        # Distinct copies per token: the shared group book must not alias, so a
        # consumer mutating one token's frame cannot corrupt its siblings'.
        return {token: books[book_for(token)].copy() for token in collateral_list}

    async def _load_book(self, book_symbol: str) -> pd.DataFrame:
        venue_symbols = _BOOK_SYMBOLS.get(book_symbol)
        if venue_symbols is None:
            raise ValueError(
                f"no live order book for {book_symbol!r}; tracked books: {sorted(_BOOK_SYMBOLS)}. "
                "See app/risk_engine/core_model/DATA_GAPS.md for how to add it."
            )
        async with self._engine.connect() as conn:
            result = await conn.execute(_LATEST_FRESH_PER_VENUE, {"symbols": venue_symbols, "max_age": self._max_age})
            rows = result.fetchall()
        if not rows:
            raise ValueError(
                f"no fresh {book_symbol} order book: no snapshot newer than {self._max_age} "
                f"for any of {venue_symbols} — is cex-orderbook-indexer running?"
            )
        asks_per_venue = [_as_levels(row.asks) for row in rows]
        logger.info(
            "order book %s aggregated from %d venue snapshot(s): %s",
            book_symbol,
            len(rows),
            ", ".join(f"{r.exchange}:{r.symbol}@{r.persisted_at:%H:%M:%S}" for r in rows),
        )
        return merge_asks(asks_per_venue)


def _as_levels(asks: object) -> list[list[str]]:
    """JSONB arrives as a decoded list or a JSON string depending on the driver path."""
    if isinstance(asks, str):
        return json.loads(asks)
    return cast(list[list[str]], asks)
