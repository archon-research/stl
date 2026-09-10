"""PostgresPositionsReader SQL against real migrated tables.

The frame-assembly semantics are unit-tested; these cover what only the
database can: the DISTINCT ON latest-row selection, decimals scaling from the
token registry, the joins against the seeded protocol, and the protocol-oracle
pricing (``protocol_oracle`` → ``token_price_current`` by token id, feed-level
freshness).
"""

import datetime as dt
from decimal import Decimal

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.core_model_positions_reader import PostgresPositionsReader
from tests.integration.core_model_seed import seed_spoof_token
from tests.integration.seed import (
    insert_anchorage_snapshot,
    insert_maple_loan,
    insert_maple_loan_collateral,
    insert_maple_loan_state,
    insert_maple_pool,
    insert_maple_pool_state,
    insert_user,
    maple_seed_ids,
)

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
        # The trigger-fed cache carries no FK, so the CASCADE above never
        # reaches it; a leaked row would resurrect a truncated market's borrower.
        await conn.execute(text("TRUNCATE morpho_market_position_current"))
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
            protocol="GALAXY", network="ETHEREUM", morpho_market="", loan_token="USDC", galaxy_type=""
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


# Syrup (Maple): what only the database covers — pool resolution by underlying
# symbol, the pool-cycle anchor, the is_internal filter, the collateral join on
# the loan state's exact (synced_at, processing_version), and the cycle
# staleness bound. Frame math is unit-tested.

_SYRUP = dict(protocol="SYRUP", network="ETHEREUM", morpho_market="", loan_token="USDC", galaxy_type="")

_POOL_ADDR = bytes.fromhex("f0" * 20)
_BTC_PRICE_1E8 = int(78276.425 * 10**8)


@pytest.fixture()
async def syrup_conn(db_url: str, engine):
    """asyncpg connection for the maple seed helpers, on a clean maple slate.

    Reuses the module ``engine`` fixture for the reader under test; maple
    tables are truncated here because that fixture only clears the
    SparkLend/Morpho ones.
    """
    conn = await asyncpg.connect(db_url)
    try:
        for table in ("maple_loan_state", "maple_loan_collateral", "maple_pool_state"):
            await conn.execute(f"TRUNCATE {table}")
        await conn.execute("TRUNCATE maple_pool CASCADE")
        yield conn
    finally:
        await conn.close()


async def _seed_syrup_pool(conn, synced_at: dt.datetime, *, address: bytes = _POOL_ADDR) -> tuple[int, int]:
    protocol_id, usdc_id = await maple_seed_ids(conn)
    pool_id = await insert_maple_pool(
        conn, protocol_id=protocol_id, address=address, asset_token_id=usdc_id, synced_at=synced_at
    )
    await insert_maple_pool_state(conn, pool_id=pool_id, synced_at=synced_at, liquid_assets=0)
    return protocol_id, pool_id


async def _seed_syrup_loan(
    conn,
    protocol_id: int,
    pool_id: int,
    address: bytes,
    synced_at: dt.datetime,
    *,
    principal: int = 25_000_000 * 10**6,
    meta: str | None = None,
    amount: int | None = 505 * 10**8,
    level: int | None = 1_111_111,
    build_id: int = 0,
) -> int:
    borrower_id = await insert_user(conn, address[::-1])
    loan_id = await insert_maple_loan(
        conn,
        protocol_id=protocol_id,
        pool_id=pool_id,
        borrower_user_id=borrower_id,
        address=address,
        synced_at=synced_at,
        loan_meta_type=meta,
    )
    await insert_maple_loan_state(
        conn, loan_id=loan_id, synced_at=synced_at, state="Active", principal_owed=principal, build_id=build_id
    )
    await insert_maple_loan_collateral(
        conn,
        loan_id=loan_id,
        synced_at=synced_at,
        symbol="BTC",
        amount=amount,
        decimals=8,
        value_usd=_BTC_PRICE_1E8,
        liquidation_level=level,
        build_id=build_id,
    )
    return loan_id


async def test_syrup_external_loans_of_the_current_cycle_build_the_frame(engine, syrup_conn):
    now = dt.datetime.now(dt.timezone.utc)
    old = now - dt.timedelta(hours=2)
    protocol_id, pool_id = await _seed_syrup_pool(syrup_conn, now)
    await _seed_syrup_loan(syrup_conn, protocol_id, pool_id, b"\x01" * 20, now)
    # Internal (amm) loans, loans absent from the newest cycle, and repaid
    # loans still reported Active at $0 (ltv would divide by zero) never appear.
    await _seed_syrup_loan(syrup_conn, protocol_id, pool_id, b"\x02" * 20, now, meta="amm")
    await _seed_syrup_loan(syrup_conn, protocol_id, pool_id, b"\x03" * 20, old, principal=99 * 10**6)
    await _seed_syrup_loan(syrup_conn, protocol_id, pool_id, b"\x06" * 20, now, principal=0)

    users_df, market_df = await PostgresPositionsReader(engine).get_protocol_data(**_SYRUP)
    assert len(users_df) == 1
    row = users_df.iloc[0]
    assert row["usdc_borrow"] == pytest.approx(25_000_000)
    assert row["btc_supply"] == pytest.approx(505.0)
    assert row["lltv"] == pytest.approx(0.9, abs=1e-6)
    assert list(market_df["token_symbol"]) == ["BTC"]
    assert market_df["oracle_price"].iloc[0] == pytest.approx(78276.425)


async def test_syrup_collateral_joins_the_state_rows_own_processing_version(engine, syrup_conn):
    now = dt.datetime.now(dt.timezone.utc)
    protocol_id, pool_id = await _seed_syrup_pool(syrup_conn, now)
    loan = b"\x04" * 20
    await _seed_syrup_loan(syrup_conn, protocol_id, pool_id, loan, now, amount=100 * 10**8, build_id=0)
    # A reprocess of the same cycle: the pv-1 state must pair with the pv-1
    # collateral, never with the pv-0 row of the same synced_at.
    await insert_maple_loan_state(
        syrup_conn,
        loan_id=(await syrup_conn.fetchval("SELECT id FROM maple_loan WHERE loan_address = $1", loan)),
        synced_at=now,
        state="Active",
        principal_owed=25_000_000 * 10**6,
        build_id=1,
    )
    await insert_maple_loan_collateral(
        syrup_conn,
        loan_id=(await syrup_conn.fetchval("SELECT id FROM maple_loan WHERE loan_address = $1", loan)),
        synced_at=now,
        symbol="BTC",
        amount=505 * 10**8,
        decimals=8,
        value_usd=_BTC_PRICE_1E8,
        liquidation_level=1_111_111,
        build_id=1,
    )
    users_df, _ = await PostgresPositionsReader(engine).get_protocol_data(**_SYRUP)
    assert len(users_df) == 1
    assert users_df.iloc[0]["btc_supply"] == pytest.approx(505.0)


async def test_syrup_a_stale_pool_cycle_fails_the_run(engine, syrup_conn):
    stale = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)
    protocol_id, pool_id = await _seed_syrup_pool(syrup_conn, stale)
    await _seed_syrup_loan(syrup_conn, protocol_id, pool_id, b"\x05" * 20, stale)
    with pytest.raises(ValueError, match="stale snapshot"):
        await PostgresPositionsReader(engine).get_protocol_data(**_SYRUP)


async def test_syrup_without_a_matching_pool_fails_the_run(engine, syrup_conn):
    with pytest.raises(ValueError, match="exactly one syrup pool"):
        await PostgresPositionsReader(engine).get_protocol_data(**{**_SYRUP, "loan_token": "USDT"})


# Anchorage: what only the database covers — the per-prime latest-poll cohort
# (closed packages keep active=true on their last row and must not leak), the
# processing_version correction pick, and the cohort staleness bound. Frame
# math is unit-tested.

_ANCHORAGE = dict(protocol="ANCHORAGE", network="ETHEREUM", morpho_market="", loan_token="ALL", galaxy_type="")

# The seed helper's fixed package terms: critical_ltv 0.85, asset_price 60000.
_ANCHORAGE_CRITICAL_LTV = 0.85
_ANCHORAGE_SEED_PRICE = 60_000


@pytest.fixture()
async def anchorage_conn(db_url: str, engine):
    """asyncpg connection for the anchorage seed helper, on a clean snapshot slate.

    Reuses the module ``engine`` fixture for the reader under test; the
    anchorage table is truncated here because that fixture only clears the
    SparkLend/Morpho ones. The reader scans every prime, so leftover snapshots
    from sibling tests would pollute the cohort.
    """
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute("TRUNCATE anchorage_package_snapshot")
        yield conn
    finally:
        await conn.close()


async def _seed_anchorage_prime(conn, name: str, vault_byte: bytes) -> int:
    return await conn.fetchval(
        "INSERT INTO prime (prime_key, name, vault_address) VALUES ('prm_t_' || $1, $1, $2) "
        "ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address RETURNING id",
        name,
        vault_byte * 20,
    )


async def _insert_live_package(
    conn,
    prime_id: int,
    package_id: str,
    snapshot_time: dt.datetime,
    *,
    exposure_value: Decimal = Decimal(150_000_000),
    package_value: Decimal = Decimal(187_500_000),
    asset_quantity: Decimal = Decimal(3125),
    build_id: int = 0,
) -> None:
    """The canonical live BTC package ($150M loan against 3125 BTC at the seed price)."""
    await insert_anchorage_snapshot(
        conn,
        prime_id=prime_id,
        package_id=package_id,
        active=True,
        exposure_value=exposure_value,
        package_value=package_value,
        asset_quantity=asset_quantity,
        snapshot_time=snapshot_time,
        build_id=build_id,
    )


async def test_anchorage_latest_cohort_excludes_closed_packages_last_rows(engine, anchorage_conn):
    now = dt.datetime.now(dt.timezone.utc)
    prime_id = await _seed_anchorage_prime(anchorage_conn, "anchorage_core_model", b"\xd0")
    await _insert_live_package(anchorage_conn, prime_id, "live-package", now)
    # A closed package's LAST row is older than the cohort and still says
    # active=true; taking "latest row per package" instead of "latest poll
    # cohort" would resurrect it (the $521M trap).
    await _insert_live_package(
        anchorage_conn,
        prime_id,
        "closed-package",
        now - dt.timedelta(hours=2),
        exposure_value=Decimal(99_000_000),
        package_value=Decimal(120_000_000),
        asset_quantity=Decimal(2000),
    )

    users_df, market_df = await PostgresPositionsReader(engine).get_protocol_data(**_ANCHORAGE)

    assert list(users_df["wallet_address"]) == ["live-package"]
    row = users_df.iloc[0]
    assert row["usdc_borrow"] == pytest.approx(150_000_000)
    assert row["btc_supply"] == pytest.approx(3125)
    assert row["lltv"] == pytest.approx(_ANCHORAGE_CRITICAL_LTV)
    assert row["ltv"] == pytest.approx(0.8)
    assert list(market_df["token_symbol"]) == ["BTC"]
    assert market_df["oracle_price"].iloc[0] == pytest.approx(_ANCHORAGE_SEED_PRICE)


async def test_anchorage_correction_of_the_same_poll_wins(engine, anchorage_conn):
    now = dt.datetime.now(dt.timezone.utc)
    prime_id = await _seed_anchorage_prime(anchorage_conn, "anchorage_core_model", b"\xd0")
    for build_id, quantity in ((0, 3000), (1, 3125)):
        await _insert_live_package(
            anchorage_conn, prime_id, "corrected-package", now, asset_quantity=Decimal(quantity), build_id=build_id
        )

    users_df, _ = await PostgresPositionsReader(engine).get_protocol_data(**_ANCHORAGE)

    assert len(users_df) == 1
    assert users_df.iloc[0]["btc_supply"] == pytest.approx(3125)


async def test_anchorage_prime_cohorts_are_isolated(engine, anchorage_conn):
    # A second prime polled later must not starve the first: the latest poll
    # is per prime, not a global MAX(snapshot_time).
    now = dt.datetime.now(dt.timezone.utc)
    first = await _seed_anchorage_prime(anchorage_conn, "anchorage_core_model", b"\xd0")
    second = await _seed_anchorage_prime(anchorage_conn, "anchorage_core_model_2", b"\xd1")
    await _insert_live_package(anchorage_conn, first, "first-prime-package", now - dt.timedelta(minutes=30))
    await _insert_live_package(
        anchorage_conn,
        second,
        "second-prime-package",
        now,
        exposure_value=Decimal(50_000_000),
        package_value=Decimal(62_500_000),
        asset_quantity=Decimal(1041),
    )

    users_df, _ = await PostgresPositionsReader(engine).get_protocol_data(**_ANCHORAGE)

    assert sorted(users_df["wallet_address"]) == ["first-prime-package", "second-prime-package"]


async def test_anchorage_a_stale_cohort_fails_the_run(engine, anchorage_conn):
    stale = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)
    prime_id = await _seed_anchorage_prime(anchorage_conn, "anchorage_core_model", b"\xd0")
    await _insert_live_package(anchorage_conn, prime_id, "frozen-package", stale)
    with pytest.raises(ValueError, match="frozen feed"):
        await PostgresPositionsReader(engine).get_protocol_data(**_ANCHORAGE)


async def test_anchorage_no_packages_fails_the_run(engine, anchorage_conn):
    with pytest.raises(ValueError, match="no active anchorage packages"):
        await PostgresPositionsReader(engine).get_protocol_data(**_ANCHORAGE)


async def test_anchorage_refuses_a_specific_loan_token(engine):
    # Raised before any query, so no seeded state is involved.
    with pytest.raises(ValueError, match="LOAN_TOKEN=ALL"):
        await PostgresPositionsReader(engine).get_protocol_data(**{**_ANCHORAGE, "loan_token": "USDC"})
