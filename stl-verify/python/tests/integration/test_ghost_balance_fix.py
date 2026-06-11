"""Regression tests: positions whose latest balance is zero must not appear.

The original bug applied ``balance > 0`` inside the DISTINCT ON / LIMIT 1
latest-row selection of the allocation queries, so sweep-to-zero rows were
skipped and the last non-zero balance resurfaced as an open position.
"""

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.config import Settings
from app.domain.entities.allocation import EthAddress
from app.main import create_app

# The proxy_kind classifier marks an address as ALM unless it matches a
# known sub-proxy.  Any address that is NOT the Spark sub-proxy will do.
_CLOSED_PROXY_HEX = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # all positions closed to zero
_SWEEP_PROXY_HEX = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"  # zero sweep rows newer than the non-zero row
_OPEN_PROXY_HEX = "cccccccccccccccccccccccccccccccccccccccc"  # open position with older zero rows
_MIXED_PROXY_HEX = "dddddddddddddddddddddddddddddddddddddddd"  # one swept + one open token
_TIEBREAK_PROXY_HEX = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"  # same-block rows, log_index decides

# Token addresses, not used by migrations
_SYRUP_USDT_HEX = "1111111111111111111111111111111111111111"
_USDS_HEX = "2222222222222222222222222222222222222222"
_RECEIPT_SYRUP_HEX = "3333333333333333333333333333333333333333"  # receipt token wrapping syrupUSDT

# Transaction hashes (32 bytes)
_TXA = "aa" * 32
_TXB = "bb" * 32
_TXC = "cc" * 32
_TXD = "dd" * 32
_TXE = "ee" * 32
_TXF = "ff" * 32


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


async def _insert_token(conn: AsyncConnection, addr_hex: str, symbol: str, decimals: int) -> int:
    """Insert a chain-1 token and return its id."""
    return (
        await conn.execute(
            text(
                "INSERT INTO token (chain_id, address, symbol, decimals) "
                "VALUES (1, decode(:addr, 'hex'), :symbol, :decimals) RETURNING id"
            ),
            {"addr": addr_hex, "symbol": symbol, "decimals": decimals},
        )
    ).scalar_one()


async def _insert_position(
    conn: AsyncConnection,
    *,
    token_id: int,
    prime_id: int,
    proxy_hex: str,
    balance: int,
    block: int,
    tx: str,
    direction: str,
    log_index: int = 0,
) -> None:
    """Insert one allocation_position row (chain_id=1, block_version=0, tx_amount=balance)."""
    await conn.execute(
        text(
            "INSERT INTO allocation_position "
            "(chain_id, token_id, prime_id, proxy_address, balance, "
            "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
            "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), :bal, :bn, 0, "
            "decode(:tx, 'hex'), :li, :bal, :dir)"
        ),
        {
            "tid": token_id,
            "pid": prime_id,
            "proxy": proxy_hex,
            "bal": balance,
            "bn": block,
            "tx": tx,
            "dir": direction,
            "li": log_index,
        },
    )


async def _seed_reference_rows(conn: AsyncConnection) -> tuple[int, int, int]:
    """Create the tokens, receipt-token registration, and a syrupUSDT price of 2 USD.

    Returns (prime_id, asyrup_token_id, usds_token_id).
    """
    prime_id = (await conn.execute(text("SELECT id FROM prime WHERE name = 'spark'"))).scalar_one()
    # protocol.name is not unique across chains; chain_id pins mainnet.
    protocol_id = (
        await conn.execute(text("SELECT id FROM protocol WHERE name = 'Aave V3' AND chain_id = 1"))
    ).scalar_one()
    oracle_id = (await conn.execute(text("SELECT id FROM oracle WHERE name = 'aave_v3'"))).scalar_one()

    syrup_id = await _insert_token(conn, _SYRUP_USDT_HEX, "syrupUSDT", 6)
    usds_id = await _insert_token(conn, _USDS_HEX, "USDS", 18)
    asyrup_id = await _insert_token(conn, _RECEIPT_SYRUP_HEX, "aSyrupUSDT", 6)

    await conn.execute(
        text(
            "INSERT INTO receipt_token "
            "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
            "VALUES (1, :pid, :uid, decode(:addr, 'hex'), 'aSyrupUSDT')"
        ),
        {"pid": protocol_id, "uid": syrup_id, "addr": _RECEIPT_SYRUP_HEX},
    )
    await conn.execute(
        text(
            "INSERT INTO onchain_token_price "
            "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
            "VALUES (:tid, :oid, 2000, 0, NOW(), 2)"
        ),
        {"tid": syrup_id, "oid": oracle_id},
    )
    return prime_id, asyrup_id, usds_id


async def _seed_closed_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """Both holdings closed to zero: aSyrupUSDT 100 then 0, USDS 75894 then 0.

    The latest balance is 0 for both, so neither token may appear in any query.
    """
    for token_id, rows in [
        (asyrup_id, [(1000, 100, _TXA, "in"), (2000, 0, _TXB, "out")]),
        (usds_id, [(1000, 75894, _TXC, "in"), (2000, 0, _TXD, "out")]),
    ]:
        for block, bal, tx, direction in rows:
            await _insert_position(
                conn,
                token_id=token_id,
                prime_id=prime_id,
                proxy_hex=_CLOSED_PROXY_HEX,
                balance=bal,
                block=block,
                tx=tx,
                direction=direction,
            )


async def _seed_sweep_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int) -> None:
    """The production sweep shape: five zero-balance sweep rows newer than the non-zero row."""
    await _insert_position(
        conn,
        token_id=asyrup_id,
        prime_id=prime_id,
        proxy_hex=_SWEEP_PROXY_HEX,
        balance=68231707,
        block=2000,
        tx=_TXE,
        direction="in",
    )
    for sweep_block in range(2001, 2006):
        await _insert_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=_SWEEP_PROXY_HEX,
            balance=0,
            block=sweep_block,
            tx=_TXF,
            direction="sweep",
        )


async def _seed_open_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int) -> None:
    """Open position with an older zero row; must still appear with balance=500."""
    for block, bal, tx, direction in [(999, 0, _TXA, "out"), (1000, 500, _TXB, "in")]:
        await _insert_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=_OPEN_PROXY_HEX,
            balance=bal,
            block=block,
            tx=tx,
            direction=direction,
        )


async def _seed_mixed_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """One swept receipt token (250 then 0) and one open USDS holding (1000); only USDS may appear."""
    for block, bal, tx, direction in [(3000, 250, _TXA, "in"), (3001, 0, _TXB, "out")]:
        await _insert_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=_MIXED_PROXY_HEX,
            balance=bal,
            block=block,
            tx=tx,
            direction=direction,
        )
    await _insert_position(
        conn,
        token_id=usds_id,
        prime_id=prime_id,
        proxy_hex=_MIXED_PROXY_HEX,
        balance=1000,
        block=3000,
        tx=_TXC,
        direction="in",
    )


async def _seed_tiebreak_proxy(conn: AsyncConnection, prime_id: int, asyrup_id: int, usds_id: int) -> None:
    """All rows in block 4000, so log_index alone orders them.

    aSyrupUSDT is deposited then fully withdrawn within the block and must not
    appear; USDS is the mirror shape and ends the block at 400.
    """
    for log_index, bal, direction in [(0, 300, "in"), (1, 0, "out")]:
        await _insert_position(
            conn,
            token_id=asyrup_id,
            prime_id=prime_id,
            proxy_hex=_TIEBREAK_PROXY_HEX,
            balance=bal,
            block=4000,
            tx=_TXD,
            direction=direction,
            log_index=log_index,
        )
    for log_index, bal, direction in [(0, 0, "out"), (1, 400, "in")]:
        await _insert_position(
            conn,
            token_id=usds_id,
            prime_id=prime_id,
            proxy_hex=_TIEBREAK_PROXY_HEX,
            balance=bal,
            block=4000,
            tx=_TXE,
            direction=direction,
            log_index=log_index,
        )


async def _seed(async_url: str) -> None:
    """Seed all proxy scenarios for the ghost-balance regression tests."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            prime_id, asyrup_id, usds_id = await _seed_reference_rows(conn)
            await _seed_closed_proxy(conn, prime_id, asyrup_id, usds_id)
            await _seed_sweep_proxy(conn, prime_id, asyrup_id)
            await _seed_open_proxy(conn, prime_id, asyrup_id)
            await _seed_mixed_proxy(conn, prime_id, asyrup_id, usds_id)
            await _seed_tiebreak_proxy(conn, prime_id, asyrup_id, usds_id)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed ghost-balance test data and yield the async URL."""
    asyncio.run(_seed(module_db["async_url"]))
    return module_db["async_url"]


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    """TestClient wired to the seeded test database."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare PostgresAllocationRepository for direct method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield PostgresAllocationRepository(engine)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture()
async def receipt_token_id(async_db_url: str) -> int:
    """Resolve the aSyrupUSDT receipt-token id seeded by ``_seed``."""
    engine = create_async_engine(async_db_url)
    try:
        async with engine.connect() as conn:
            return (
                await conn.execute(
                    text(
                        "SELECT id FROM receipt_token "
                        "WHERE chain_id = 1 AND receipt_token_address = decode(:addr, 'hex')"
                    ),
                    {"addr": _RECEIPT_SYRUP_HEX},
                )
            ).scalar_one()
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# /allocations endpoint
# ---------------------------------------------------------------------------


def test_closed_receipt_token_absent_from_allocations_endpoint(client: TestClient) -> None:
    """A position closed to 0 (latest balance=0) must not appear in /allocations."""
    response = client.get(f"/v1/primes/0x{_CLOSED_PROXY_HEX}/allocations")

    assert response.status_code == 200
    symbols = {row["symbol"] for row in response.json()}
    assert "aSyrupUSDT" not in symbols, "ghost receipt-token position surfaced"
    assert "USDS" not in symbols, "ghost direct-asset position surfaced"


def test_closed_direct_asset_absent_from_allocations_endpoint(client: TestClient) -> None:
    """A direct asset swept to 0 must not appear."""
    response = client.get(f"/v1/primes/0x{_CLOSED_PROXY_HEX}/allocations")

    assert response.status_code == 200
    rows = response.json()
    direct_symbols = {r["symbol"] for r in rows if r.get("category") == "asset"}
    assert "USDS" not in direct_symbols, "ghost direct-asset holding surfaced"


def test_swept_position_absent_from_allocations_endpoint(client: TestClient) -> None:
    """Zero-balance sweep rows newer than the last non-zero row close the position."""
    response = client.get(f"/v1/primes/0x{_SWEEP_PROXY_HEX}/allocations")

    assert response.status_code == 200
    symbols = {row["symbol"] for row in response.json()}
    assert "aSyrupUSDT" not in symbols, "ghost balance surfaced for swept position"


def test_open_position_with_older_zero_rows_still_returned(client: TestClient) -> None:
    """An open position whose history contains earlier zero rows must still appear."""
    response = client.get(f"/v1/primes/0x{_OPEN_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" in by_symbol, "open position incorrectly filtered out"
    assert by_symbol["aSyrupUSDT"]["balance"] == "500"


def test_mixed_proxy_drops_only_swept_token(client: TestClient) -> None:
    """Filtering is per token: the swept holding disappears, the open one stays."""
    response = client.get(f"/v1/primes/0x{_MIXED_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" not in by_symbol, "swept receipt-token position surfaced on mixed proxy"
    assert "USDS" in by_symbol, "open direct holding incorrectly filtered out on mixed proxy"
    assert by_symbol["USDS"]["balance"] == "1000"


def test_same_block_log_index_tiebreak_decides_latest_row(client: TestClient) -> None:
    """Within a single block, log_index ordering decides which row is latest."""
    response = client.get(f"/v1/primes/0x{_TIEBREAK_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" not in by_symbol, "position zeroed within its final block surfaced"
    assert "USDS" in by_symbol, "position opened within the same block incorrectly filtered out"
    assert by_symbol["USDS"]["balance"] == "400"


# ---------------------------------------------------------------------------
# Repository USD-exposure methods
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_total_usd_exposure_excludes_swept_positions(repo) -> None:
    """A fully swept prime has zero total exposure, not its stale balance × price."""
    prime_id = EthAddress(f"0x{_CLOSED_PROXY_HEX}")
    result = await repo.get_total_usd_exposure(prime_id)
    assert result == Decimal("0"), f"Expected 0 total USD exposure for fully-swept prime, got {result}"


@pytest.mark.asyncio
async def test_get_total_usd_exposure_for_open_position_is_priced(repo) -> None:
    """An open position contributes balance × price (500 × 2 = 1000) to the total."""
    prime_id = EthAddress(f"0x{_OPEN_PROXY_HEX}")
    result = await repo.get_total_usd_exposure(prime_id)
    assert result == Decimal("1000"), f"Expected priced total of 1000 for open position, got {result}"


@pytest.mark.asyncio
async def test_get_usd_exposure_raises_for_swept_position(repo, receipt_token_id: int) -> None:
    """A position swept to 0 raises instead of resurfacing a stale balance."""
    prime_id = EthAddress(f"0x{_CLOSED_PROXY_HEX}")
    with pytest.raises(ValueError, match="no position or price found"):
        await repo.get_usd_exposure(receipt_token_id, prime_id)


@pytest.mark.asyncio
async def test_get_usd_exposure_returns_priced_balance_for_open_position(repo, receipt_token_id: int) -> None:
    """An open position returns balance × price (500 × 2 = 1000)."""
    prime_id = EthAddress(f"0x{_OPEN_PROXY_HEX}")
    result = await repo.get_usd_exposure(receipt_token_id, prime_id)
    assert result == Decimal("1000"), f"Expected priced exposure of 1000, got {result}"
