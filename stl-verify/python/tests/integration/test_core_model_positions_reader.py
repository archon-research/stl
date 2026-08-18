"""PostgresPositionsReader SQL against real migrated tables.

The frame-assembly semantics are unit-tested; these cover what only the
database can: the DISTINCT ON latest-row selection, decimals scaling from the
token registry, and the joins against the seeded protocol.
"""

import datetime as dt

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_positions_reader import PostgresPositionsReader

_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"


@pytest.fixture()
async def engine(async_db_url: str):
    eng = create_async_engine(async_db_url, pool_pre_ping=True)
    async with eng.begin() as conn:
        for table in (
            "borrower",
            "borrower_collateral",
            "sparklend_reserve_data",
            "onchain_token_price",
            "morpho_market_position",
        ):
            await conn.execute(text(f"TRUNCATE {table}"))
    yield eng
    await eng.dispose()


async def _ids(conn) -> dict:
    row = (
        await conn.execute(
            text("""
            SELECT (SELECT id FROM protocol WHERE chain_id = 1 AND name = 'SparkLend') AS protocol_id,
                   (SELECT id FROM token WHERE chain_id = 1 AND address = decode(:weth, 'hex')) AS weth,
                   (SELECT id FROM token WHERE chain_id = 1 AND address = decode(:usdt, 'hex')) AS usdt
        """),
            {"weth": _WETH[2:], "usdt": _USDT[2:]},
        )
    ).one()
    return {"protocol_id": row.protocol_id, "weth": row.weth, "usdt": row.usdt}


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


async def _seed_market(conn, ids: dict) -> None:
    now = dt.datetime.now(dt.UTC)
    await conn.execute(
        text("""
            INSERT INTO sparklend_reserve_data
                (protocol_id, token_id, block_number, liquidation_threshold, liquidation_bonus)
            VALUES (:p, :t, 100, 8600, 10500)
        """),
        {"p": ids["protocol_id"], "t": ids["weth"]},
    )
    for token_id, price in ((ids["weth"], 2000), (ids["usdt"], 1)):
        await conn.execute(
            text("""
                INSERT INTO onchain_token_price (token_id, oracle_id, block_number, "timestamp", price_usd)
                VALUES (:t, 1, 100, :ts, :price)
            """),
            {"t": token_id, "ts": now, "price": price},
        )


async def test_latest_row_per_user_token_wins(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "aa" * 20)
        await _seed_market(conn, ids)
        for block, amount in ((100, 5 * 10**18), (200, 3 * 10**18)):  # newer block supersedes
            await conn.execute(
                text("""
                    INSERT INTO borrower_collateral
                        (user_id, protocol_id, token_id, block_number, amount, change, event_type,
                         tx_hash, collateral_enabled)
                    VALUES (:u, :p, :t, :b, :a, 0, 'supply', '\\x00', true)
                """),
                {"u": user_id, "p": ids["protocol_id"], "t": ids["weth"], "b": block, "a": amount},
            )
        await conn.execute(
            text("""
                INSERT INTO borrower
                    (user_id, protocol_id, token_id, block_number, amount, change, event_type, tx_hash)
                VALUES (:u, :p, :t, 100, :a, 0, 'borrow', '\\x00')
            """),
            {"u": user_id, "p": ids["protocol_id"], "t": ids["usdt"], "a": 1000 * 10**6},
        )

    users, market = await PostgresPositionsReader(engine).get_protocol_data(
        protocol="SPARKLEND", network="ETHEREUM", morpho_market="", loan_token="USDT", galaxy_type=""
    )
    row = users.iloc[0]
    assert row["weth_supply"] == 3.0  # block-200 row, decimals-scaled from 3e18
    assert row["weth_supply_usd"] == 6000.0
    assert row["usdt_borrow"] == 1000.0  # decimals-scaled from 1e9 raw (6 decimals)
    assert row["lltv"] == pytest.approx(0.86)  # bps 8600 / 10000
    assert row["liquidation_incentive"] == pytest.approx(1.05)
    assert list(market["token_symbol"]) == ["WETH"]
    assert list(market["oracle_price"]) == [2000.0]


async def test_unsupported_protocol_fails_with_the_data_gaps_pointer(engine):
    with pytest.raises(ValueError, match="DATA_GAPS"):
        await PostgresPositionsReader(engine).get_protocol_data(
            protocol="SYRUP", network="ETHEREUM", morpho_market="", loan_token="USDC", galaxy_type=""
        )


async def _seed_morpho_market(conn, ids: dict, lltv_1e18: int, market_id_byte: str) -> int:
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
            {"mb": market_id_byte, "loan": ids["usdt"], "collateral": ids["weth"], "lltv": lltv_1e18},
        )
    ).scalar_one()


async def test_morpho_latest_row_decimals_and_lif(engine):
    async with engine.begin() as conn:
        ids = await _ids(conn)
        user_id = await _seed_user(conn, "cc" * 20)
        await _seed_market(conn, ids)  # prices for WETH/USDT
        market = await _seed_morpho_market(conn, ids, 860000000000000000, "ab")
        for block, collateral, borrow in ((100, 2 * 10**18, 500 * 10**6), (200, 1 * 10**18, 400 * 10**6)):
            await conn.execute(
                text("""
                    INSERT INTO morpho_market_position
                        (user_id, morpho_market_id, block_number, "timestamp",
                         supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
                    VALUES (:u, :m, :b, now(), 0, 0, :c, 0, :bor)
                """),
                {"u": user_id, "m": market, "b": block, "c": collateral, "bor": borrow},
            )

    users, market_df = await PostgresPositionsReader(engine).get_protocol_data(
        protocol="MORPHO", network="ETHEREUM", morpho_market="WETH", loan_token="USDT", galaxy_type=""
    )
    row = users.iloc[0]
    assert row["weth_supply"] == 1.0  # block-200 row wins
    assert row["usdt_borrow"] == 400.0
    assert row["lltv"] == pytest.approx(0.86)
    assert row["liquidation_incentive"] == pytest.approx(1.04384134, abs=1e-8)
    assert list(market_df["token_symbol"]) == ["WETH"]


async def test_morpho_unknown_pair_fails_loudly(engine):
    with pytest.raises(ValueError, match="no morpho_market rows"):
        await PostgresPositionsReader(engine).get_protocol_data(
            protocol="MORPHO", network="ETHEREUM", morpho_market="WETH", loan_token="DAI", galaxy_type=""
        )
