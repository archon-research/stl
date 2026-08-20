"""Current-position tables: trigger upkeep, and the backed-breakdown read that uses them.

Every scenario seeds its own protocol / oracle / tokens, so the module's shared
database keeps the scenarios independent of each other and of ordering.
"""

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.aave_like_backed_breakdown_repository import AaveLikeBackedBreakdownRepository
from tests.integration.seed import insert_oracle_asset, insert_token, insert_user

_BLOCK = 30_000_000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str) -> AsyncIterator[AaveLikeBackedBreakdownRepository]:
    """The Aave-like backed breakdown repository under test."""
    engine = create_async_engine(async_db_url)
    try:
        yield AaveLikeBackedBreakdownRepository(engine)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


async def _insert_protocol(conn: asyncpg.Connection, name: str, address: bytes) -> int:
    """Insert a chain_id=1 lending protocol."""
    return cast(
        int,
        await conn.fetchval(
            "INSERT INTO protocol (chain_id, address, name, protocol_type) VALUES (1, $1, $2, 'lending') RETURNING id",
            address,
            name,
        ),
    )


async def _insert_oracle(conn: asyncpg.Connection, name: str, address: bytes) -> int:
    """Insert a chain_id=1 oracle."""
    return cast(
        int,
        await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) VALUES ($1, $1, 1, $2) RETURNING id",
            name,
            address,
        ),
    )


async def _bind_oracle(conn: asyncpg.Connection, protocol_id: int, oracle_id: int) -> None:
    await conn.execute(
        "INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block) VALUES ($1, $2, 1)",
        protocol_id,
        oracle_id,
    )


async def _insert_debt(
    conn: asyncpg.Connection, *, protocol_id: int, user_id: int, token_id: int, amount: int, block: int
) -> None:
    """Insert a borrower debt snapshot (raw on-chain amount)."""
    await conn.execute(
        "INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, "
        "amount, change, event_type, tx_hash) VALUES ($1, $2, $3, $4, 0, $5, $5, 'borrow', $6)",
        user_id,
        protocol_id,
        token_id,
        block,
        Decimal(amount),
        b"\x00" * 32,
    )


async def _insert_collateral(
    conn: asyncpg.Connection,
    *,
    protocol_id: int,
    user_id: int,
    token_id: int,
    amount: int,
    block: int,
    collateral_enabled: bool = True,
) -> None:
    """Insert a borrower_collateral snapshot (raw on-chain amount)."""
    await conn.execute(
        "INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, "
        "amount, change, event_type, tx_hash, collateral_enabled) "
        "VALUES ($1, $2, $3, $4, 0, $5, $5, 'deposit', $6, $7)",
        user_id,
        protocol_id,
        token_id,
        block,
        Decimal(amount),
        b"\x00" * 32,
        collateral_enabled,
    )


async def _insert_reserve(
    conn: asyncpg.Connection, *, protocol_id: int, token_id: int, block: int, collateral_enabled: bool
) -> None:
    """Insert a sparklend_reserve_data snapshot carrying the collateral flag."""
    await conn.execute(
        "INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version, "
        "usage_as_collateral_enabled) VALUES ($1, $2, $3, 0, $4)",
        protocol_id,
        token_id,
        block,
        collateral_enabled,
    )


async def _insert_price(conn: asyncpg.Connection, *, token_id: int, oracle_id: int, price: str, block: int) -> None:
    """Insert an onchain_token_price row."""
    await conn.execute(
        "INSERT INTO onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
        "VALUES ($1, $2, $3, 0, NOW(), $4::numeric(30,18))",
        token_id,
        oracle_id,
        block,
        price,
    )


# ---------------------------------------------------------------------------
# Trigger upkeep
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_newer_debt_insert_updates_current_row(conn: asyncpg.Connection) -> None:
    """A debt snapshot at a higher block replaces the current row."""
    protocol_id = await _insert_protocol(conn, "curNewer", b"\x11" * 20)
    token_id = await insert_token(conn, "CURNEWER", 18, b"\x12" * 20)
    user_id = await insert_user(conn, b"\x13" * 20)

    await _insert_debt(conn, protocol_id=protocol_id, user_id=user_id, token_id=token_id, amount=100, block=_BLOCK)
    await _insert_debt(conn, protocol_id=protocol_id, user_id=user_id, token_id=token_id, amount=250, block=_BLOCK + 1)

    row = await conn.fetchrow(
        "SELECT amount, block_number FROM borrower_current WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3",
        protocol_id,
        user_id,
        token_id,
    )
    assert row is not None
    assert row["amount"] == Decimal(250)
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_out_of_order_debt_insert_does_not_regress_current_row(conn: asyncpg.Connection) -> None:
    """An older snapshot arriving late (backfill, retry) must not overwrite the current row."""
    protocol_id = await _insert_protocol(conn, "curReorder", b"\x21" * 20)
    token_id = await insert_token(conn, "CURREORDER", 18, b"\x22" * 20)
    user_id = await insert_user(conn, b"\x23" * 20)

    await _insert_debt(conn, protocol_id=protocol_id, user_id=user_id, token_id=token_id, amount=250, block=_BLOCK + 1)
    await _insert_debt(conn, protocol_id=protocol_id, user_id=user_id, token_id=token_id, amount=100, block=_BLOCK)

    row = await conn.fetchrow(
        "SELECT amount, block_number FROM borrower_current WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3",
        protocol_id,
        user_id,
        token_id,
    )
    assert row is not None
    assert row["amount"] == Decimal(250)
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_collateral_flag_follows_the_newest_snapshot(conn: asyncpg.Connection) -> None:
    """Disabling a deposit as collateral at a higher block flips the current row's flag."""
    protocol_id = await _insert_protocol(conn, "curCollateral", b"\x31" * 20)
    token_id = await insert_token(conn, "CURCOLL", 18, b"\x32" * 20)
    user_id = await insert_user(conn, b"\x33" * 20)

    await _insert_collateral(conn, protocol_id=protocol_id, user_id=user_id, token_id=token_id, amount=5, block=_BLOCK)
    await _insert_collateral(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=5,
        block=_BLOCK + 1,
        collateral_enabled=False,
    )

    enabled = await conn.fetchval(
        "SELECT collateral_enabled FROM borrower_collateral_current "
        "WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3",
        protocol_id,
        user_id,
        token_id,
    )
    assert enabled is False


@pytest.mark.asyncio(loop_scope="module")
async def test_out_of_order_reserve_insert_does_not_regress_current_row(conn: asyncpg.Connection) -> None:
    """A late older reserve snapshot must not resurrect the flag it already superseded."""
    protocol_id = await _insert_protocol(conn, "curReserve", b"\x41" * 20)
    token_id = await insert_token(conn, "CURRESERVE", 18, b"\x42" * 20)

    await _insert_reserve(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK + 1, collateral_enabled=False)
    await _insert_reserve(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK, collateral_enabled=True)

    row = await conn.fetchrow(
        "SELECT usage_as_collateral_enabled, block_number FROM sparklend_reserve_data_current "
        "WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
    )
    assert row is not None
    assert row["usage_as_collateral_enabled"] is False
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_price_fans_out_to_every_protocol_bound_to_the_oracle(conn: asyncpg.Connection) -> None:
    """One price row becomes one current row per protocol bound to the writing oracle."""
    oracle_id = await _insert_oracle(conn, "cur_fanout", b"\x51" * 20)
    first_protocol_id = await _insert_protocol(conn, "curFanoutA", b"\x52" * 20)
    second_protocol_id = await _insert_protocol(conn, "curFanoutB", b"\x53" * 20)
    # A third protocol, deliberately left unbound: it must get no price row.
    await _insert_protocol(conn, "curFanoutUnbound", b"\x54" * 20)
    token_id = await insert_token(conn, "CURFANOUT", 18, b"\x55" * 20)
    await _bind_oracle(conn, first_protocol_id, oracle_id)
    await _bind_oracle(conn, second_protocol_id, oracle_id)
    await insert_oracle_asset(conn, oracle_id, token_id)

    await _insert_price(conn, token_id=token_id, oracle_id=oracle_id, price="7.5", block=_BLOCK)

    rows = await conn.fetch(
        "SELECT protocol_id, price_usd FROM token_price_current WHERE token_id = $1 ORDER BY protocol_id",
        token_id,
    )
    assert [row["protocol_id"] for row in rows] == sorted([first_protocol_id, second_protocol_id])
    assert {row["price_usd"] for row in rows} == {Decimal("7.5")}


@pytest.mark.asyncio(loop_scope="module")
async def test_price_without_enabled_mapping_gets_no_current_row(conn: asyncpg.Connection) -> None:
    """A price whose (oracle, token) mapping is disabled never reaches the current table."""
    oracle_id = await _insert_oracle(conn, "cur_disabled_map", b"\x61" * 20)
    protocol_id = await _insert_protocol(conn, "curDisabledMap", b"\x62" * 20)
    token_id = await insert_token(conn, "CURDISABLEDMAP", 18, b"\x63" * 20)
    await _bind_oracle(conn, protocol_id, oracle_id)
    await insert_oracle_asset(conn, oracle_id, token_id, enabled=False)

    await _insert_price(conn, token_id=token_id, oracle_id=oracle_id, price="3.0", block=_BLOCK)

    assert await conn.fetchval("SELECT count(*) FROM token_price_current WHERE token_id = $1", token_id) == 0


# ---------------------------------------------------------------------------
# Backed breakdown over the current tables
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def attribution_ids(conn: asyncpg.Connection) -> dict[str, int]:
    """Seed a three-borrower protocol whose history exercises every current-table rule.

    Prices: COLLA = $100, COLLB = $2. Reserves: COLLA accepted from the start,
    COLLB rejected then accepted at a higher block. Borrowers:

    * user 1 -- COLLA deposit inserted out of order (20 at block+1 written before
      10 at block, so the current row must stay 20 -> $2,000) plus 500 COLLB
      ($1,000); debt 900 DEBT. COLLA gets 2000/3000 x 900 = $600, COLLB $300.
    * user 2 -- 5 COLLA ($500) + 1,000 COLLB ($2,000); debt 250 DEBT.
      COLLA gets 500/2500 x 250 = $50, COLLB $200.
    * user 3 -- 100 COLLA, disabled as collateral at a higher block, so its
      $10,000 must not back the 700 DEBT it borrows.

    Totals: COLLA $650 (56.5217%), COLLB $500 (43.4783%).
    """
    protocol_id = await _insert_protocol(conn, "curAttribution", b"\x71" * 20)
    oracle_id = await _insert_oracle(conn, "cur_attribution", b"\x72" * 20)
    await _bind_oracle(conn, protocol_id, oracle_id)

    coll_a_id = await insert_token(conn, "COLLA", 18, b"\x73" * 20)
    coll_b_id = await insert_token(conn, "COLLB", 6, b"\x74" * 20)
    debt_id = await insert_token(conn, "DEBT577", 18, b"\x75" * 20)
    for token_id, price in ((coll_a_id, "100.0"), (coll_b_id, "2.0")):
        await insert_oracle_asset(conn, oracle_id, token_id)
        await _insert_price(conn, token_id=token_id, oracle_id=oracle_id, price=price, block=_BLOCK)

    await _insert_reserve(conn, protocol_id=protocol_id, token_id=coll_a_id, block=_BLOCK, collateral_enabled=True)
    await _insert_reserve(conn, protocol_id=protocol_id, token_id=coll_b_id, block=_BLOCK, collateral_enabled=False)
    await _insert_reserve(conn, protocol_id=protocol_id, token_id=coll_b_id, block=_BLOCK + 1, collateral_enabled=True)

    user_1 = await insert_user(conn, b"\x76" * 20)
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=coll_a_id, amount=20 * 10**18, block=_BLOCK + 1
    )
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=coll_a_id, amount=10 * 10**18, block=_BLOCK
    )
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=coll_b_id, amount=500 * 10**6, block=_BLOCK
    )
    await _insert_debt(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=debt_id, amount=900 * 10**18, block=_BLOCK
    )

    user_2 = await insert_user(conn, b"\x77" * 20)
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_2, token_id=coll_a_id, amount=5 * 10**18, block=_BLOCK
    )
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_2, token_id=coll_b_id, amount=1_000 * 10**6, block=_BLOCK
    )
    await _insert_debt(
        conn, protocol_id=protocol_id, user_id=user_2, token_id=debt_id, amount=250 * 10**18, block=_BLOCK
    )

    user_3 = await insert_user(conn, b"\x78" * 20)
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_3, token_id=coll_a_id, amount=100 * 10**18, block=_BLOCK
    )
    await _insert_collateral(
        conn,
        protocol_id=protocol_id,
        user_id=user_3,
        token_id=coll_a_id,
        amount=100 * 10**18,
        block=_BLOCK + 2,
        collateral_enabled=False,
    )
    await _insert_debt(
        conn, protocol_id=protocol_id, user_id=user_3, token_id=debt_id, amount=700 * 10**18, block=_BLOCK
    )

    return {"protocol_id": protocol_id, "debt_id": debt_id}


@pytest.mark.asyncio(loop_scope="module")
async def test_backed_breakdown_matches_hand_computed_attribution(
    repository: AaveLikeBackedBreakdownRepository, attribution_ids: dict[str, int]
) -> None:
    """The breakdown over the current tables matches the figures derived in the seed docstring."""
    result = await repository.get_backed_breakdown(attribution_ids["protocol_id"], attribution_ids["debt_id"])

    by_symbol = {item.symbol: item for item in result.items}
    assert set(by_symbol) == {"COLLA", "COLLB"}

    assert by_symbol["COLLA"].backing_value == Decimal("650.00")
    assert by_symbol["COLLB"].backing_value == Decimal("500.00")
    assert by_symbol["COLLA"].backing_pct == Decimal("56.5217")
    assert by_symbol["COLLB"].backing_pct == Decimal("43.4783")
    assert by_symbol["COLLA"].price_usd == Decimal("100")
    assert by_symbol["COLLB"].price_usd == Decimal("2")


@pytest.mark.asyncio(loop_scope="module")
async def test_disabling_a_mapping_drops_the_price_at_read_time(
    conn: asyncpg.Connection, repository: AaveLikeBackedBreakdownRepository
) -> None:
    """Retiring a token's only price source takes effect on the next read.

    The current price row it already wrote stays behind, so only the read-time
    re-check of the enabled mapping can drop it: the collateral loses its USD
    value and all backing moves to the token that is still priced.
    """
    protocol_id = await _insert_protocol(conn, "curRetired", b"\x81" * 20)
    oracle_id = await _insert_oracle(conn, "cur_retired", b"\x82" * 20)
    await _bind_oracle(conn, protocol_id, oracle_id)

    keep_id = await insert_token(conn, "KEEPPRICE", 18, b"\x83" * 20)
    drop_id = await insert_token(conn, "DROPPRICE", 18, b"\x84" * 20)
    debt_id = await insert_token(conn, "RETIREDDEBT", 18, b"\x85" * 20)
    for token_id, price in ((keep_id, "3.0"), (drop_id, "4.0")):
        await insert_oracle_asset(conn, oracle_id, token_id)
        await _insert_price(conn, token_id=token_id, oracle_id=oracle_id, price=price, block=_BLOCK)
        await _insert_reserve(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK, collateral_enabled=True)

    user_id = await insert_user(conn, b"\x86" * 20)
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=keep_id, amount=10 * 10**18, block=_BLOCK
    )
    await _insert_collateral(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=drop_id, amount=5 * 10**18, block=_BLOCK
    )
    await _insert_debt(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=debt_id, amount=100 * 10**18, block=_BLOCK
    )

    before = await repository.get_backed_breakdown(protocol_id, debt_id)
    assert {item.symbol: item.backing_value for item in before.items} == {
        "KEEPPRICE": Decimal("60.00"),
        "DROPPRICE": Decimal("40.00"),
    }

    # Retiring a source is a configuration change on the mapping, not a rewrite
    # of any price history.
    await conn.execute(
        "UPDATE oracle_asset SET enabled = false WHERE oracle_id = $1 AND token_id = $2", oracle_id, drop_id
    )
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM token_price_current WHERE protocol_id = $1 AND token_id = $2", protocol_id, drop_id
        )
        == 1
    )

    after = await repository.get_backed_breakdown(protocol_id, debt_id)
    assert {item.symbol: item.backing_value for item in after.items} == {"KEEPPRICE": Decimal("100.00")}
    assert {item.symbol: item.backing_pct for item in after.items} == {"KEEPPRICE": Decimal("100.0000")}
