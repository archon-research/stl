"""PostgresPriceReader against real offchain_price_asset / offchain_token_price tables."""

import datetime as dt

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_price_reader import PostgresPriceReader

_MIN_DAYS = 5  # small window keeps the seeds readable; the logic is window-size-agnostic


@pytest.fixture()
async def engine(async_db_url: str):
    eng = create_async_engine(async_db_url, pool_pre_ping=True)
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE offchain_token_price"))
        await conn.execute(text("DELETE FROM offchain_price_asset"))
    yield eng
    await eng.dispose()


async def _seed_asset(engine, symbol: str, token_symbol: str) -> tuple[int, int]:
    """Create the asset row against a seeded token; return (token_id, source_id)."""
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text("""
                    INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, name, symbol, enabled)
                    SELECT s.id, lower(:symbol), t.id, :symbol, :symbol, true
                    FROM offchain_price_source s,
                         token t
                    WHERE t.chain_id = 1 AND t.symbol = :token_symbol
                    ORDER BY s.id LIMIT 1
                    RETURNING token_id, source_id
                """),
                {"symbol": symbol, "token_symbol": token_symbol},
            )
        ).one()
    return row.token_id, row.source_id


async def _seed_days(engine, token_id: int, source_id: int, closes: dict[dt.date, float]) -> None:
    async with engine.begin() as conn:
        for day, close in closes.items():
            # Two ticks per day: the reader must pick the later one as the close.
            for hour, price in ((9, close + 5.0), (23, close)):
                ts = dt.datetime.combine(day, dt.time(hour), tzinfo=dt.UTC)
                await conn.execute(
                    text("""
                        INSERT INTO offchain_token_price (token_id, source_id, "timestamp", price_usd)
                        VALUES (:token_id, :source_id, :ts, :price)
                    """),
                    {"token_id": token_id, "source_id": source_id, "ts": ts, "price": price},
                )


def _last_days(n: int) -> list[dt.date]:
    yesterday = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)
    return [yesterday - dt.timedelta(days=i) for i in range(n)][::-1]


async def test_daily_close_is_the_last_price_of_each_day(engine):
    token_id, source_id = await _seed_asset(engine, "WETH", "WETH")
    days = _last_days(_MIN_DAYS)
    await _seed_days(engine, token_id, source_id, {d: 2000.0 + i for i, d in enumerate(days)})

    prices = await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])
    assert list(prices["WETH"]) == [2000.0 + i for i in range(_MIN_DAYS)]  # closes, not the 09:00 ticks


async def test_symbols_resolve_case_insensitively(engine):
    token_id, source_id = await _seed_asset(engine, "cbBTC", "cbBTC")
    await _seed_days(engine, token_id, source_id, {d: 60000.0 for d in _last_days(_MIN_DAYS)})

    prices = await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["CBBTC"])
    assert list(prices.columns) == ["CBBTC"]


async def test_a_gap_inside_the_window_fails_the_run(engine):
    token_id, source_id = await _seed_asset(engine, "WETH", "WETH")
    days = _last_days(_MIN_DAYS)
    days.remove(days[2])  # hole in the middle
    await _seed_days(engine, token_id, source_id, {d: 2000.0 for d in days})

    with pytest.raises(ValueError, match="1 of the last 5 days missing"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])


async def test_an_unknown_symbol_fails_with_the_backfill_pointer(engine):
    with pytest.raises(ValueError, match="XRP: no offchain_price_asset row"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["XRP"])


async def test_short_history_fails_rather_than_feeding_garch_a_stub(engine):
    token_id, source_id = await _seed_asset(engine, "WETH", "WETH")
    await _seed_days(engine, token_id, source_id, {d: 2000.0 for d in _last_days(2)})

    with pytest.raises(ValueError, match="3 of the last 5 days missing"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])
