"""Integration tests for PostgresMorphoVaultAllocationsReader.

The reader serves the CORE model's vault→market weights, so the scenario pins
the reads the service depends on: latest-row-wins for both positions and vault
state (across blocks and across versions within one block), other users'
positions excluded, decimal scaling into loan-token units, exited markets
dropped, and the no-state / unknown-vault degradations.
"""

from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.morpho_vault_allocations_reader import PostgresMorphoVaultAllocationsReader
from tests.integration.seed import insert_token, insert_user

# The module-scoped engine binds its pool to one event loop, so every test
# must run on that loop, as the sibling repository test modules do.
pytestmark = pytest.mark.asyncio(loop_scope="module")

_SEED_BLOCK_NUMBER = 20_000_000
_OLDER_BLOCK_NUMBER = _SEED_BLOCK_NUMBER - 1

_VAULT_ADDRESS = b"\xcc" * 20
_STATELESS_VAULT_ADDRESS = b"\xdd" * 20
_USDC_ADDRESS = b"\xa0\xb8\x69\x91\xc6\x21\x8b\x36\xc1\xd1\x9d\x4a\x2e\x9e\xb0\xce\x36\x06\xeb\x48"
_WETH_ADDRESS = b"\xc0\x2a\xaa\x39\xb2\x23\xfe\x8d\x0a\x0e\x5c\x4f\x27\xea\xd9\x08\x3c\x75\x6c\xc2"
_WBTC_ADDRESS = b"\x22\x60\xfa\xc5\xe5\x54\x2a\x77\x3a\xa4\x4f\xbc\xfe\xdf\x7c\x19\x3b\xc2\xc5\x99"
_XAUT_ADDRESS = b"\x68\xd6\x91\x44\xb3\xd2\xd3\x14\x14\x62\xa1\x11\x1e\x76\x0c\x0d\xf7\x34\x12\x9a"


async def _insert_protocol(conn: asyncpg.Connection) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
        VALUES (1, $1, 'Morpho Blue', 'morpho_blue', 18883124, NOW())
        ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
            b"\xbb" * 19 + b"\x01",
        ),
    )


async def _insert_morpho_market(
    conn: asyncpg.Connection,
    protocol_id: int,
    market_id: bytes,
    loan_token_id: int,
    collateral_token_id: int,
) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO morpho_market
            (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
             oracle_address, irm_address, lltv, created_at_block)
        VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id
        """,
            protocol_id,
            market_id,
            loan_token_id,
            collateral_token_id,
            b"\x00" * 20,
            b"\x00" * 20,
            Decimal("0.86"),
            _SEED_BLOCK_NUMBER,
        ),
    )


async def _insert_morpho_vault(
    conn: asyncpg.Connection,
    protocol_id: int,
    address: bytes,
    asset_token_id: int,
    name: str,
    symbol: str,
) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO morpho_vault
            (chain_id, protocol_id, address, name, symbol,
             asset_token_id, vault_version, created_at_block)
        VALUES (1, $1, $2, $3, $4, $5, 1, $6)
        RETURNING id
        """,
            protocol_id,
            address,
            name,
            symbol,
            asset_token_id,
            _SEED_BLOCK_NUMBER,
        ),
    )


async def _insert_morpho_vault_state(
    conn: asyncpg.Connection,
    morpho_vault_id: int,
    total_assets: str,
    block_number: int,
    block_version: int = 0,
) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_vault_state
            (morpho_vault_id, block_number, block_version, timestamp,
             total_assets, total_shares)
        VALUES ($1, $2, $4, NOW(), $3, $3)
        """,
        morpho_vault_id,
        block_number,
        total_assets,
        block_version,
    )


async def _insert_morpho_market_position(
    conn: asyncpg.Connection,
    user_id: int,
    morpho_market_id: int,
    supply_assets: str,
    block_number: int,
    block_version: int = 0,
) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_market_position
            (user_id, morpho_market_id, block_number, block_version, timestamp,
             supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
        VALUES ($1, $2, $3, $5, NOW(), $4, 0, 0, $4, 0)
        """,
        user_id,
        morpho_market_id,
        block_number,
        supply_assets,
        block_version,
    )


# ---------------------------------------------------------------------------
# Scenario: a 1M-USDC vault (6 decimals) supplying two Blue markets.
#
#   WETH/USDC:  latest supply 400,000 USDC — an older-block 100,000 row and a
#               same-block lower-version 350,000 row (reorg replacement) lose;
#               another user's 900,000 position carries no weight.
#   WBTC/USDC:  supply 300,000 USDC.
#   XAUT/USDC:  fully exited — latest row has supply 0 and must not appear.
#   Vault state: an older-block 999,000 snapshot and a same-block
#               lower-version 990,000 one must lose to the 1,000,000 one.
#
# A second vault has no state row and no positions (total 0, no allocations).
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await _insert_protocol(conn)
        usdc_id = await insert_token(conn, "USDC", 6, _USDC_ADDRESS)
        weth_id = await insert_token(conn, "WETH", 18, _WETH_ADDRESS)
        wbtc_id = await insert_token(conn, "WBTC", 8, _WBTC_ADDRESS)
        xaut_id = await insert_token(conn, "XAUt", 6, _XAUT_ADDRESS)

        vault_user_id = await insert_user(conn, _VAULT_ADDRESS)
        vault_id = await _insert_morpho_vault(
            conn, protocol_id, _VAULT_ADDRESS, usdc_id, name="Morpho USDC Vault", symbol="mUSDC"
        )
        await _insert_morpho_vault_state(conn, vault_id, "999000000000", _OLDER_BLOCK_NUMBER)
        await _insert_morpho_vault_state(conn, vault_id, "990000000000", _SEED_BLOCK_NUMBER, block_version=0)
        await _insert_morpho_vault_state(conn, vault_id, "1000000000000", _SEED_BLOCK_NUMBER, block_version=1)

        weth_market_id = await _insert_morpho_market(conn, protocol_id, b"\x01" * 32, usdc_id, weth_id)
        await _insert_morpho_market_position(conn, vault_user_id, weth_market_id, "100000000000", _OLDER_BLOCK_NUMBER)
        await _insert_morpho_market_position(
            conn, vault_user_id, weth_market_id, "350000000000", _SEED_BLOCK_NUMBER, block_version=0
        )
        await _insert_morpho_market_position(
            conn, vault_user_id, weth_market_id, "400000000000", _SEED_BLOCK_NUMBER, block_version=1
        )
        rival_user_id = await insert_user(conn, b"\xab" * 20)
        await _insert_morpho_market_position(conn, rival_user_id, weth_market_id, "900000000000", _SEED_BLOCK_NUMBER)

        wbtc_market_id = await _insert_morpho_market(conn, protocol_id, b"\x02" * 32, usdc_id, wbtc_id)
        await _insert_morpho_market_position(conn, vault_user_id, wbtc_market_id, "300000000000", _SEED_BLOCK_NUMBER)

        xaut_market_id = await _insert_morpho_market(conn, protocol_id, b"\x03" * 32, usdc_id, xaut_id)
        await _insert_morpho_market_position(conn, vault_user_id, xaut_market_id, "50000000000", _OLDER_BLOCK_NUMBER)
        await _insert_morpho_market_position(conn, vault_user_id, xaut_market_id, "0", _SEED_BLOCK_NUMBER)

        await insert_user(conn, _STATELESS_VAULT_ADDRESS)
        await _insert_morpho_vault(
            conn, protocol_id, _STATELESS_VAULT_ADDRESS, usdc_id, name="Morpho Stateless Vault", symbol="mUSDCs"
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, _seed_data: None):
    engine = create_async_engine(async_db_url)
    try:
        yield PostgresMorphoVaultAllocationsReader(engine)
    finally:
        await engine.dispose()


async def test_latest_rows_win_and_are_scaled_to_loan_token_units(repository) -> None:
    # WETH 400K only if both the older block AND the same-block lower version
    # lose; total 1M only if the same tie-break holds for vault state.
    vault = await repository.get_vault_allocations(_VAULT_ADDRESS, 1)

    assert vault is not None
    assert vault.total_assets == Decimal("1000000")
    assert [(a.collateral_symbol, a.loan_symbol, a.supply_assets) for a in vault.allocations] == [
        ("WETH", "USDC", Decimal("400000")),
        ("WBTC", "USDC", Decimal("300000")),
    ]


async def test_other_users_positions_carry_no_weight(repository) -> None:
    # The rival user's 900K WETH/USDC supply must not appear as vault weight.
    vault = await repository.get_vault_allocations(_VAULT_ADDRESS, 1)

    assert vault is not None
    assert max(a.supply_assets for a in vault.allocations) == Decimal("400000")


async def test_exited_market_carries_no_allocation(repository) -> None:
    vault = await repository.get_vault_allocations(_VAULT_ADDRESS, 1)

    assert vault is not None
    assert "XAUt" not in {a.collateral_symbol for a in vault.allocations}


async def test_vault_without_state_or_positions_reports_zero_assets(repository) -> None:
    vault = await repository.get_vault_allocations(_STATELESS_VAULT_ADDRESS, 1)

    assert vault is not None
    assert vault.total_assets == Decimal("0")
    assert vault.allocations == ()


async def test_unknown_address_returns_none(repository) -> None:
    assert await repository.get_vault_allocations(b"\x99" * 20, 1) is None


async def test_wrong_chain_returns_none(repository) -> None:
    assert await repository.get_vault_allocations(_VAULT_ADDRESS, 8453) is None
