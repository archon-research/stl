"""PostgresPriceReader against a real onchain_token_price table."""

import datetime as dt

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_price_reader import PostgresPriceReader
from tests.integration.core_model_seed import delete_spoof_token, seed_spoof_token

_MIN_DAYS = 5  # small window keeps the seeds readable; the logic is window-size-agnostic


@pytest.fixture()
async def engine(async_db_url: str):
    eng = create_async_engine(async_db_url, pool_pre_ping=True)
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE onchain_token_price"))
        # The symbol-collision test seeds a second "WETH" token; _token_id
        # resolves by symbol, so a leaked spoof row would break sibling tests.
        await delete_spoof_token(conn)
    yield eng
    await eng.dispose()


async def _token_id(engine, token_symbol: str) -> int:
    async with engine.connect() as conn:
        return (
            await conn.execute(text("SELECT id FROM token WHERE chain_id = 1 AND symbol = :s"), {"s": token_symbol})
        ).scalar_one()


async def _seed_days(engine, token_id: int, closes: dict[dt.date, float], oracle: str = "sparklend") -> None:
    async with engine.begin() as conn:
        oracle_id = (
            await conn.execute(text("SELECT id FROM oracle WHERE name = :n AND chain_id = 1"), {"n": oracle})
        ).scalar_one()
        for i, (day, close) in enumerate(closes.items()):
            # Two updates per day: the later block must win as the close.
            for block_offset, hour, price in ((0, 9, close + 5.0), (1, 23, close)):
                ts = dt.datetime.combine(day, dt.time(hour), tzinfo=dt.UTC)
                await conn.execute(
                    text("""
                        INSERT INTO onchain_token_price (token_id, oracle_id, block_number, "timestamp", price_usd)
                        VALUES (:token_id, :oracle_id, :block, :ts, :price)
                    """),
                    {
                        "token_id": token_id,
                        "oracle_id": oracle_id,
                        "block": 1000 + i * 10 + block_offset,
                        "ts": ts,
                        "price": price,
                    },
                )


def _last_days(n: int) -> list[dt.date]:
    yesterday = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)
    return [yesterday - dt.timedelta(days=i) for i in range(n)][::-1]


async def test_daily_close_is_the_newest_block_of_each_day(engine):
    token_id = await _token_id(engine, "WETH")
    days = _last_days(_MIN_DAYS)
    await _seed_days(engine, token_id, {d: 2000.0 + i for i, d in enumerate(days)})

    prices = await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])
    assert list(prices["WETH"]) == [2000.0 + i for i in range(_MIN_DAYS)]  # closes, not the 09:00 updates


async def test_rows_from_other_oracles_never_enter_the_series(engine):
    # Chainlink writes the same days at higher blocks; the pinned oracle's
    # closes must win, never the newest block across feeds.
    token_id = await _token_id(engine, "WETH")
    days = _last_days(_MIN_DAYS)
    await _seed_days(engine, token_id, {d: 2000.0 for d in days})
    async with engine.begin() as conn:
        chainlink = (await conn.execute(text("SELECT id FROM oracle WHERE name = 'chainlink'"))).scalar_one()
        for i, day in enumerate(days):
            await conn.execute(
                text("""
                    INSERT INTO onchain_token_price (token_id, oracle_id, block_number, "timestamp", price_usd)
                    VALUES (:t, :o, :block, :ts, 9999.0)
                """),
                {
                    "t": token_id,
                    "o": chainlink,
                    "block": 1000 + i * 10 + 5,
                    "ts": dt.datetime.combine(day, dt.time(23, 30), tzinfo=dt.UTC),
                },
            )

    prices = await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])
    assert list(prices["WETH"]) == [2000.0] * _MIN_DAYS


async def test_an_unregistered_oracle_fails_loudly(engine):
    with pytest.raises(ValueError, match="not registered"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS, oracle_name="nope").get_prices(["WETH"])


async def test_columns_keep_the_callers_labels(engine):
    token_id = await _token_id(engine, "cbBTC")
    await _seed_days(engine, token_id, {d: 60000.0 for d in _last_days(_MIN_DAYS)})

    prices = await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["cbBTC"])
    assert list(prices.columns) == ["cbBTC"]


async def test_symbols_resolve_case_insensitively(engine):
    token_id = await _token_id(engine, "cbBTC")
    await _seed_days(engine, token_id, {d: 60000.0 for d in _last_days(_MIN_DAYS)})

    prices = await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["CBBTC"])
    assert list(prices.columns) == ["CBBTC"]


async def test_two_priced_tokens_sharing_a_symbol_are_refused(engine):
    token_id = await _token_id(engine, "WETH")
    await _seed_days(engine, token_id, {d: 2000.0 for d in _last_days(_MIN_DAYS)})
    async with engine.begin() as conn:
        spoof_id = await seed_spoof_token(conn, "WETH")
    await _seed_days(engine, spoof_id, {_last_days(1)[0]: 1.0})  # one priced day is enough to poison

    with pytest.raises(ValueError, match="ambiguous token symbol.*WETH"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])


async def test_a_gap_inside_the_window_fails_the_run(engine):
    token_id = await _token_id(engine, "WETH")
    days = _last_days(_MIN_DAYS)
    days.remove(days[2])  # hole in the middle
    await _seed_days(engine, token_id, {d: 2000.0 for d in days})

    with pytest.raises(ValueError, match="1 of the last 5 days missing"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])


async def test_an_unknown_symbol_fails_with_the_backfill_pointer(engine):
    with pytest.raises(ValueError, match="XRP: no on-chain oracle prices"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["XRP"])


async def test_short_history_fails_rather_than_feeding_garch_a_stub(engine):
    token_id = await _token_id(engine, "WETH")
    await _seed_days(engine, token_id, {d: 2000.0 for d in _last_days(2)})

    with pytest.raises(ValueError, match="3 of the last 5 days missing"):
        await PostgresPriceReader(engine, min_days=_MIN_DAYS).get_prices(["WETH"])
