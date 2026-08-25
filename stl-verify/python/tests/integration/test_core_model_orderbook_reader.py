"""PostgresOrderbookReader against a real migrated cex_orderbook_snapshots table."""

import datetime as dt
import json
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_orderbook_reader import PostgresOrderbookReader


@pytest.fixture()
async def engine(async_db_url: str):
    eng = create_async_engine(async_db_url, pool_pre_ping=True)
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE cex_orderbook_snapshots"))
    yield eng
    await eng.dispose()


async def _seed(engine, exchange: str, symbol: str, asks: list[list[str]], age: timedelta = timedelta(seconds=5)):
    ts = dt.datetime.now(dt.UTC) - age
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO cex_orderbook_snapshots (exchange, symbol, ingested_at, persisted_at, bids, asks)
                VALUES (:exchange, :symbol, :ts, :ts, :bids, :asks)
            """),
            {"exchange": exchange, "symbol": symbol, "ts": ts, "bids": "[]", "asks": json.dumps(asks)},
        )


async def test_aggregates_the_latest_snapshot_of_every_venue(engine):
    await _seed(engine, "coinbase", "ETH-USD", [["2000.0", "1.0"]])
    await _seed(engine, "okx", "ETH-USDT", [["2001.0", "2.0"]])
    await _seed(engine, "kraken", "ETH/USD", [["1999.0", "3.0"]])

    books = await PostgresOrderbookReader(engine).get_orderbooks(["WETH"])
    df = books["WETH"]
    assert list(df["price"]) == [1999.0, 2000.0, 2001.0]
    assert list(df["liquidity"]) == [5997.0, 2000.0, 4002.0]


async def test_only_the_newest_snapshot_per_venue_is_used(engine):
    await _seed(engine, "coinbase", "ETH-USD", [["1800.0", "9.0"]], age=timedelta(minutes=5))
    await _seed(engine, "coinbase", "ETH-USD", [["2000.0", "1.0"]], age=timedelta(seconds=1))

    books = await PostgresOrderbookReader(engine).get_orderbooks(["WETH"])
    assert list(books["WETH"]["price"]) == [2000.0]


async def test_one_venue_listing_two_book_symbols_counts_once(engine):
    await _seed(engine, "coinbase", "BTC-USDT", [["59000.0", "9.0"]], age=timedelta(minutes=5))
    await _seed(engine, "coinbase", "BTC-USD", [["60000.0", "1.0"]], age=timedelta(seconds=1))

    books = await PostgresOrderbookReader(engine).get_orderbooks(["WBTC"])
    assert list(books["WBTC"]["price"]) == [60000.0]  # newest snapshot only; depth never counted twice


async def test_every_token_of_a_group_gets_the_shared_book_without_aliasing(engine):
    await _seed(engine, "coinbase", "BTC-USD", [["60000.0", "1.0"]])

    books = await PostgresOrderbookReader(engine).get_orderbooks(["WBTC", "CBBTC"])
    assert list(books["WBTC"]["price"]) == list(books["CBBTC"]["price"]) == [60000.0]
    books["WBTC"].loc[0, "price"] = 1.0  # mutating one frame must not touch the other
    assert list(books["CBBTC"]["price"]) == [60000.0]


async def test_stale_snapshots_are_rejected(engine):
    await _seed(engine, "coinbase", "ETH-USD", [["2000.0", "1.0"]], age=timedelta(hours=2))

    with pytest.raises(ValueError, match="no fresh ETH order book"):
        await PostgresOrderbookReader(engine, max_age=timedelta(minutes=30)).get_orderbooks(["WETH"])


async def test_an_empty_table_is_rejected_not_served_as_an_empty_book(engine):
    with pytest.raises(ValueError, match="no fresh BTC order book"):
        await PostgresOrderbookReader(engine).get_orderbooks(["WBTC"])
