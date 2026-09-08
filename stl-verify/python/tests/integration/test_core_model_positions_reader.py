"""PostgresPositionsReader SQL against real migrated tables.

The frame-assembly semantics are unit-tested; these cover what only the
database can: the DISTINCT ON latest-row selection, decimals scaling from the
token registry, the joins against the seeded protocol, and the protocol-oracle
pricing (``protocol_oracle`` → ``token_price_current`` by token id, feed-level
freshness).
"""

import datetime as dt

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_positions_reader import PostgresPositionsReader
from tests.integration.core_model_seed import seed_spoof_token

_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

_SPARKLEND = dict(protocol="SPARKLEND", network="ETHEREUM", morpho_market="", loan_token="USDT", galaxy_type="")
_MORPHO = dict(protocol="MORPHO", network="ETHEREUM", morpho_market="WETH", loan_token="USDT", galaxy_type="")


@pytest.fixture()
async def engine(async_db_url: str):
    eng = create_async_engine(async_db_url, pool_pre_ping=True)
    async with eng.begin() as conn:
        for table in (
            "borrower",
            "borrower_collateral",
            "sparklend_reserve_data",
            "onchain_token_price",
            # The *_current caches are trigger-fed and their upserts only take
            # newer rows, so a leaked row would outlive a re-seed at the same block.
            "token_price_current",
            "borrower_current",
            "borrower_collateral_current",
        ):
            await conn.execute(text(f"TRUNCATE {table}"))
        # Markets and their positions together: two tests seed the same
        # market_id, and the spoofed-collateral market must not leak.
        await conn.execute(text("TRUNCATE morpho_market CASCADE"))
    yield eng
    await eng.dispose()


async def _ids(conn) -> dict:
    row = (
        await conn.execute(
            text("""
            SELECT (SELECT id FROM protocol WHERE chain_id = 1 AND name = 'SparkLend') AS protocol_id,
                   (SELECT id FROM token WHERE chain_id = 1 AND address = decode(:weth, 'hex')) AS weth,
                   (SELECT id FROM token WHERE chain_id = 1 AND address = decode(:usdt, 'hex')) AS usdt,
                   (SELECT id FROM oracle WHERE name = 'sparklend') AS sparklend,
                   (SELECT id FROM oracle WHERE name = 'chainlink') AS chainlink
        """),
            {"weth": _WETH[2:], "usdt": _USDT[2:]},
        )
    ).one()
    return {
        "protocol_id": row.protocol_id,
        "weth": row.weth,
        "usdt": row.usdt,
        "sparklend": row.sparklend,
        "chainlink": row.chainlink,
    }


async def _seed_user(conn, address_hex: str) -> int:
    return (
        await conn.execute(
            text("""
            INSERT INTO "user" (chain_id, address, first_seen_block)
            VALUES (1, decode(:addr, 'hex'), 1) ON CONFLICT DO NOTHING RETURNING id
        """),
            {"addr": address_hex},
        )
    ).scalar_one()


async def _seed_price(conn, token_id: int, oracle_id: int, price: float, age: dt.timedelta, block: int = 100) -> None:
    await conn.execute(
        text("""
            INSERT INTO onchain_token_price (token_id, oracle_id, block_number, "timestamp", price_usd)
            VALUES (:t, :o, :b, :ts, :price)
        """),
        {"t": token_id, "o": oracle_id, "b": block, "ts": dt.datetime.now(dt.UTC) - age, "price": price},
    )


async def _seed_reserve(conn, ids: dict) -> None:
    """WETH as collateral: LT 86%, bonus 5%. Without it WETH supply is not collateral."""
    await conn.execute(
        text("""
            INSERT INTO sparklend_reserve_data
                (protocol_id, token_id, block_number, liquidation_threshold, liquidation_bonus)
            VALUES (:p, :t, 100, 8600, 10500)
        """),
        {"p": ids["protocol_id"], "t": ids["weth"]},
    )


async def _seed_market(conn, ids: dict, price_age: dt.timedelta = dt.timedelta(0), oracle: str = "sparklend") -> None:
    """WETH reserve params plus WETH/USDT prices from the given oracle feed."""
    await _seed_reserve(conn, ids)
    for token_id, price in ((ids["weth"], 2000), (ids["usdt"], 1)):
        await _seed_price(conn, token_id, ids[oracle], price, price_age)


async def _seed_borrow(conn, ids: dict, user_id: int, token_id: int, amount: int) -> None:
    await conn.execute(
        text("""
            INSERT INTO borrower
                (user_id, protocol_id, token_id, block_number, amount, change, event_type, tx_hash)
            VALUES (:u, :p, :t, 100, :a, 0, 'borrow', '\\x00')
        """),
        {"u": user_id, "p": ids["protocol_id"], "t": token_id, "a": amount},
    )


async def _seed_supply(conn, ids: dict, user_id: int, token_id: int, amount: int, block: int = 100) -> None:
    await conn.execute(
        text("""
            INSERT INTO borrower_collateral
                (user_id, protocol_id, token_id, block_number, amount, change, event_type,
                 tx_hash, collateral_enabled)
            VALUES (:u, :p, :t, :b, :a, 0, 'supply', '\\x00', true)
        """),
        {"u": user_id, "p": ids["protocol_id"], "t": token_id, "b": block, "a": amount},
    )


async def test_latest_row_per_user_token_wins(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "aa" * 20)
        await _seed_market(conn, ids)
        for block, amount in ((100, 5 * 10**18), (200, 3 * 10**18)):  # newer block supersedes
            await _seed_supply(conn, ids, user_id, ids["weth"], amount, block=block)
        await _seed_borrow(conn, ids, user_id, ids["usdt"], 1000 * 10**6)

    users, market = await PostgresPositionsReader(engine).get_protocol_data(**_SPARKLEND)
    row = users.iloc[0]
    assert row["weth_supply"] == 3.0  # block-200 row, decimals-scaled from 3e18
    assert row["weth_supply_usd"] == 6000.0
    assert row["usdt_borrow"] == 1000.0  # decimals-scaled from 1e9 raw (6 decimals)
    assert row["lltv"] == pytest.approx(0.86)  # bps 8600 / 10000
    assert row["liquidation_incentive"] == pytest.approx(1.05)
    assert list(market["token_symbol"]) == ["WETH"]
    assert list(market["oracle_price"]) == [2000.0]


async def test_a_silent_oracle_feed_fails_the_run(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "dd" * 20)
        await _seed_market(conn, ids, price_age=dt.timedelta(days=3))  # nothing newer than the 2-day bound
        await _seed_borrow(conn, ids, user_id, ids["usdt"], 1000 * 10**6)

    with pytest.raises(ValueError, match="wrote no price in the last"):
        await PostgresPositionsReader(engine).get_protocol_data(**_SPARKLEND)


async def test_an_unchanged_fixed_price_stays_valid_while_the_feed_is_alive(engine):
    # The worker writes a row only when a price changes, so a $1 stable can sit
    # on a weeks-old row; only the feed as a whole has to be live.
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "ab" * 20)
        await _seed_reserve(conn, ids)
        await _seed_price(conn, ids["usdt"], ids["sparklend"], 1.0, dt.timedelta(days=40), block=50)
        await _seed_price(conn, ids["weth"], ids["sparklend"], 2000.0, dt.timedelta(minutes=1))
        await _seed_supply(conn, ids, user_id, ids["weth"], 10**18)
        await _seed_borrow(conn, ids, user_id, ids["usdt"], 1000 * 10**6)

    users, _ = await PostgresPositionsReader(engine).get_protocol_data(**_SPARKLEND)
    assert users.iloc[0]["usdt_borrow_usd"] == 1000.0


async def test_only_the_protocol_oracle_prices_its_positions(engine):
    # A fresh Chainlink row for WETH is not SparkLend's price: with only that
    # feed live, SparkLend's own feed is silent and the run refuses.
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "ac" * 20)
        await _seed_market(conn, ids, oracle="chainlink")
        await _seed_supply(conn, ids, user_id, ids["weth"], 10**18)
        await _seed_borrow(conn, ids, user_id, ids["usdt"], 1000 * 10**6)

    with pytest.raises(ValueError, match="'sparklend' wrote no price"):
        await PostgresPositionsReader(engine).get_protocol_data(**_SPARKLEND)


async def test_a_second_token_with_the_same_symbol_that_nobody_holds_is_ignored(engine):
    # Prices are joined by token id, so a spoofed "WETH" priced by the same
    # oracle cannot leak into the real WETH's valuation.
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "ee" * 20)
        await _seed_market(conn, ids)
        spoof_id = await seed_spoof_token(conn, "WETH")
        await _seed_price(conn, spoof_id, ids["sparklend"], 1.0, dt.timedelta(0))
        await _seed_supply(conn, ids, user_id, ids["weth"], 10**18)
        await _seed_borrow(conn, ids, user_id, ids["usdt"], 1000 * 10**6)

    users, _ = await PostgresPositionsReader(engine).get_protocol_data(**_SPARKLEND)
    assert users.iloc[0]["weth_supply_usd"] == 2000.0


async def test_unsupported_protocol_fails_with_the_data_gaps_pointer(engine):
    with pytest.raises(ValueError, match="DATA_GAPS"):
        await PostgresPositionsReader(engine).get_protocol_data(
            protocol="SYRUP", network="ETHEREUM", morpho_market="", loan_token="USDC", galaxy_type=""
        )


async def _seed_morpho_market(
    conn, ids: dict, lltv_1e18: int, market_id_byte: str, collateral_id: int | None = None
) -> int:
    return (
        await conn.execute(
            text("""
                INSERT INTO morpho_market
                    (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
                     oracle_address, irm_address, lltv, created_at_block)
                VALUES (1, (SELECT id FROM protocol WHERE chain_id = 1 AND name = 'Morpho Blue'),
                        decode(repeat(:mb, 32), 'hex'), :loan, :collateral,
                        decode(repeat('11', 20), 'hex'), decode(repeat('22', 20), 'hex'), :lltv, 1)
                RETURNING id
            """),
            {"mb": market_id_byte, "loan": ids["usdt"], "collateral": collateral_id or ids["weth"], "lltv": lltv_1e18},
        )
    ).scalar_one()


async def _seed_morpho_position(
    conn, user_id: int, market: int, collateral: int, borrow: int, block: int = 100
) -> None:
    await conn.execute(
        text("""
            INSERT INTO morpho_market_position
                (user_id, morpho_market_id, block_number, "timestamp",
                 supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
            VALUES (:u, :m, :b, now(), 0, 0, :c, 0, :bor)
        """),
        {"u": user_id, "m": market, "b": block, "c": collateral, "bor": borrow},
    )


async def test_morpho_latest_row_decimals_and_lif(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "cc" * 20)
        await _seed_market(conn, ids, oracle="chainlink")  # Morpho is valued with Chainlink
        market = await _seed_morpho_market(conn, ids, 860000000000000000, "ab")
        for block, collateral, borrow in ((100, 2 * 10**18, 500 * 10**6), (200, 1 * 10**18, 400 * 10**6)):
            await _seed_morpho_position(conn, user_id, market, collateral, borrow, block=block)

    users, market_df = await PostgresPositionsReader(engine).get_protocol_data(**_MORPHO)
    row = users.iloc[0]
    assert row["weth_supply"] == 1.0  # block-200 row wins
    assert row["usdt_borrow"] == 400.0
    assert row["lltv"] == pytest.approx(0.86)
    assert row["liquidation_incentive"] == pytest.approx(1.04384134, abs=1e-8)
    assert list(market_df["token_symbol"]) == ["WETH"]
    assert list(market_df["oracle_price"]) == [2000.0]


async def test_morpho_refuses_when_its_oracle_feed_is_silent(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "cd" * 20)
        await _seed_market(conn, ids)  # SparkLend's feed only; nothing from Chainlink
        market = await _seed_morpho_market(conn, ids, 860000000000000000, "ab")
        await _seed_morpho_position(conn, user_id, market, 2 * 10**18, 500 * 10**6)

    with pytest.raises(ValueError, match="'chainlink' wrote no price"):
        await PostgresPositionsReader(engine).get_protocol_data(**_MORPHO)


async def test_morpho_two_tokens_sharing_the_collateral_symbol_are_refused(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "ff" * 20)
        await _seed_market(conn, ids, oracle="chainlink")
        spoof_id = await seed_spoof_token(conn, "WETH")  # permissionless market on a spoofed token
        real = await _seed_morpho_market(conn, ids, 860000000000000000, "ab")
        spoofed = await _seed_morpho_market(conn, ids, 860000000000000000, "cd", collateral_id=spoof_id)
        await _seed_morpho_position(conn, user_id, real, 2 * 10**18, 500 * 10**6)
        await _seed_morpho_position(conn, user_id, spoofed, 9 * 10**18, 900 * 10**6)

    with pytest.raises(ValueError, match="ambiguous collateral token"):
        await PostgresPositionsReader(engine).get_protocol_data(**_MORPHO)


async def test_morpho_unknown_pair_fails_loudly(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        await _seed_market(conn, ids, oracle="chainlink")  # feed alive, so the pair itself is what fails
    with pytest.raises(ValueError, match="no morpho_market rows"):
        await PostgresPositionsReader(engine).get_protocol_data(**{**_MORPHO, "loan_token": "DAI"})
