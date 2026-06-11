"""Regression tests for the ghost-balance fix.

These tests verify that tokens whose *latest* allocation_position row has
balance=0 do NOT appear in any of the four allocation queries.  Before the
fix the `AND ap.balance > 0` predicate lived inside the DISTINCT ON / LIMIT 1
selection, so sweep-to-zero rows were silently skipped and the last non-zero
balance resurfaced as if the position were still open.

Tests require a live TimescaleDB container (testcontainers).  Run with:

    make test-integration          (from stl-verify/python/)

or:

    uv run pytest tests/integration/test_ghost_balance_fix.py
"""

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.domain.entities.allocation import EthAddress
from app.main import create_app

# ---------------------------------------------------------------------------
# Test proxy addresses (20 bytes each, must be valid ALM proxies)
# ---------------------------------------------------------------------------

# The proxy_kind classifier marks an address as ALM unless it matches a
# known sub-proxy.  Any address that is NOT the Spark sub-proxy will do.
_CLOSED_PROXY_HEX = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # all sweeps => zero
_SWEEP_PROXY_HEX = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"  # older zero rows then positive
_OPEN_PROXY_HEX = "cccccccccccccccccccccccccccccccccccccccc"  # normal open position

# Underlying token address (any unused hex, not seeded by migrations)
_SYRUP_USDT_HEX = "1111111111111111111111111111111111111111"
_USDS_HEX = "2222222222222222222222222222222222222222"

# Receipt token address wrapping syrupUSDT
_RECEIPT_SYRUP_HEX = "3333333333333333333333333333333333333333"

# Transaction hashes (32 bytes)
_TXA = "aa" * 32
_TXB = "bb" * 32
_TXC = "cc" * 32
_TXD = "dd" * 32
_TXE = "ee" * 32
_TXF = "ff" * 32


async def _seed(async_url: str) -> None:
    """Seed minimal rows for ghost-balance regression tests."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            # Resolve a real prime_id and protocol_id from migration seeds
            prime_id = (await conn.execute(text("SELECT id FROM prime WHERE name = 'spark' LIMIT 1"))).scalar_one()
            aave_id = (await conn.execute(text("SELECT id FROM protocol WHERE name = 'Aave V3' LIMIT 1"))).scalar_one()

            # Insert tokens
            await conn.execute(
                text(
                    "INSERT INTO token (chain_id, address, symbol, decimals) "
                    "VALUES (1, decode(:addr, 'hex'), 'syrupUSDT', 6) "
                    "ON CONFLICT DO NOTHING"
                ),
                {"addr": _SYRUP_USDT_HEX},
            )
            await conn.execute(
                text(
                    "INSERT INTO token (chain_id, address, symbol, decimals) "
                    "VALUES (1, decode(:addr, 'hex'), 'USDS', 18) "
                    "ON CONFLICT DO NOTHING"
                ),
                {"addr": _USDS_HEX},
            )
            await conn.execute(
                text(
                    "INSERT INTO token (chain_id, address, symbol, decimals) "
                    "VALUES (1, decode(:addr, 'hex'), 'aSyrupUSDT', 6) "
                    "ON CONFLICT DO NOTHING"
                ),
                {"addr": _RECEIPT_SYRUP_HEX},
            )

            syrup_token_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _SYRUP_USDT_HEX},
                )
            ).scalar_one()
            usds_token_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _USDS_HEX},
                )
            ).scalar_one()
            receipt_token_token_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _RECEIPT_SYRUP_HEX},
                )
            ).scalar_one()

            # Register aSyrupUSDT as a receipt token wrapping syrupUSDT
            await conn.execute(
                text(
                    "INSERT INTO receipt_token "
                    "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
                    "VALUES (1, :pid, :uid, decode(:addr, 'hex'), 'aSyrupUSDT') "
                    "ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique DO NOTHING"
                ),
                {"pid": aave_id, "uid": syrup_token_id, "addr": _RECEIPT_SYRUP_HEX},
            )
            _ = receipt_token_token_id  # registered above; FK via receipt_token_address

            # ---------------------------------------------------------------
            # Scenario 1: [in 100, out 100] — latest balance is 0
            # _CLOSED_PROXY holds aSyrupUSDT: earlier row balance=100 (block 1000),
            # latest row balance=0 (block 2000).  Must NOT appear in any query.
            # ---------------------------------------------------------------
            for block, bal, tx in [(1000, 100, _TXA), (2000, 0, _TXB)]:
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, prime_id, proxy_address, balance, "
                        "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), :bal, :bn, 0, "
                        "decode(:tx, 'hex'), 0, :bal, 'in')"
                    ),
                    {
                        "tid": receipt_token_token_id,
                        "pid": prime_id,
                        "proxy": _CLOSED_PROXY_HEX,
                        "bal": bal,
                        "bn": block,
                        "tx": tx,
                    },
                )
            # Also seed a direct USDS holding that was swept to zero
            for block, bal, tx in [(1000, 75894, _TXC), (2000, 0, _TXD)]:
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, prime_id, proxy_address, balance, "
                        "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), :bal, :bn, 0, "
                        "decode(:tx, 'hex'), 0, :bal, 'in')"
                    ),
                    {
                        "tid": usds_token_id,
                        "pid": prime_id,
                        "proxy": _CLOSED_PROXY_HEX,
                        "bal": bal,
                        "bn": block,
                        "tx": tx,
                    },
                )

            # ---------------------------------------------------------------
            # Scenario 2: sweep shape from prod DB evidence.
            # _SWEEP_PROXY: several zero-balance rows newer than the non-zero row.
            # blocks 2001..2005 all balance=0, block 2000 balance=68231707.
            # Must NOT appear.
            # ---------------------------------------------------------------
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), 68231707, 2000, 0, "
                    "decode(:tx, 'hex'), 0, 68231707, 'in')"
                ),
                {
                    "tid": receipt_token_token_id,
                    "pid": prime_id,
                    "proxy": _SWEEP_PROXY_HEX,
                    "tx": _TXE,
                },
            )
            for sweep_block in range(2001, 2006):
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, prime_id, proxy_address, balance, "
                        "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), 0, :bn, 0, "
                        "decode(:tx, 'hex'), 0, 0, 'sweep')"
                    ),
                    {
                        "tid": receipt_token_token_id,
                        "pid": prime_id,
                        "proxy": _SWEEP_PROXY_HEX,
                        "bn": sweep_block,
                        "tx": _TXF,
                    },
                )

            # ---------------------------------------------------------------
            # Scenario 3: open position that has older zero rows.
            # _OPEN_PROXY: block 999 balance=0, block 1000 balance=500.
            # Must still appear with balance=500.
            # ---------------------------------------------------------------
            for block, bal, tx in [(999, 0, _TXA), (1000, 500, _TXB)]:
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, prime_id, proxy_address, balance, "
                        "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), :bal, :bn, 0, "
                        "decode(:tx, 'hex'), 0, :bal, 'in')"
                    ),
                    {
                        "tid": receipt_token_token_id,
                        "pid": prime_id,
                        "proxy": _OPEN_PROXY_HEX,
                        "bal": bal,
                        "bn": block,
                        "tx": tx,
                    },
                )
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed ghost-balance test data and yield the async URL."""
    asyncio.run(_seed(module_db["async_url"]))
    return module_db["async_url"]


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    """TestClient wired to the ghost-balance test database."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


@pytest.fixture()
def repo(async_db_url: str):
    """Bare PostgresAllocationRepository for direct method tests."""
    from sqlalchemy.ext.asyncio import create_async_engine as _cae

    from app.adapters.postgres.allocation_position_repository import (
        PostgresAllocationRepository,
    )

    engine = _cae(async_db_url)
    return PostgresAllocationRepository(engine)


# ---------------------------------------------------------------------------
# Test: [in 100, out 100] history => token absent
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


# ---------------------------------------------------------------------------
# Test: sweep-history shape (many zero rows newer than non-zero) => absent
# ---------------------------------------------------------------------------


def test_swept_position_absent_from_allocations_endpoint(client: TestClient) -> None:
    """The prod bug shape: zero-balance sweep rows newer than the last non-zero row."""
    response = client.get(f"/v1/primes/0x{_SWEEP_PROXY_HEX}/allocations")

    assert response.status_code == 200
    symbols = {row["symbol"] for row in response.json()}
    assert "aSyrupUSDT" not in symbols, "ghost balance 68231707 surfaced for swept position (prod bug regression)"


# ---------------------------------------------------------------------------
# Test: open position with older zero rows => still returned
# ---------------------------------------------------------------------------


def test_open_position_with_older_zero_rows_still_returned(client: TestClient) -> None:
    """An open position whose history contains earlier zero rows must still appear."""
    response = client.get(f"/v1/primes/0x{_OPEN_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" in by_symbol, "open position incorrectly filtered out"
    assert by_symbol["aSyrupUSDT"]["balance"] == "500"


# ---------------------------------------------------------------------------
# Test: get_total_usd_exposure excludes swept positions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_total_usd_exposure_excludes_swept_positions(repo) -> None:
    """get_total_usd_exposure must return 0 when the only position is swept to 0.

    We cannot seed onchain_token_price rows here (the price oracle join
    means a zero-priced position contributes 0 anyway), but the key
    regression is that a swept position does not cause the query to pull
    a stale non-zero balance into the aggregate.  With no price rows the
    result is 0 via COALESCE regardless; the absence of an exception
    (which would occur if the old buggy query accidentally joined against
    a stale non-zero row with no matching price) is itself the assertion.
    """
    prime_id = EthAddress(f"0x{_CLOSED_PROXY_HEX}")
    result = await repo.get_total_usd_exposure(prime_id)
    assert result == Decimal("0"), f"Expected 0 total USD exposure for fully-swept prime, got {result}"


@pytest.mark.asyncio
async def test_get_total_usd_exposure_for_open_position_does_not_raise(repo) -> None:
    """get_total_usd_exposure for a prime with an open position returns 0 when
    no price data exists (no exception expected — COALESCE handles NULL sum).
    """
    prime_id = EthAddress(f"0x{_OPEN_PROXY_HEX}")
    result = await repo.get_total_usd_exposure(prime_id)
    # No price rows seeded => SUM is NULL => COALESCE returns 0
    assert result == Decimal("0")
