from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, Protocol, cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.domain.entities.backed_breakdown import BackedBreakdown
from tests.integration.conftest import insert_token, insert_user, store_test_ids


class ProtocolScopedBackedBreakdownRepository(Protocol):
    """Spec contract: Morpho repository is selected by protocol before invocation."""

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown: ...


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

_SEED_BLOCK_NUMBER = 20_000_000


async def _insert_protocol(conn: asyncpg.Connection) -> int:
    """Insert a Morpho Blue protocol entry and return its ID."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
        VALUES (1, $1, 'Morpho Blue', 'morpho_blue', 18883124, NOW())
        ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
            b"\xbb\xbb\xbb\xbb\xbb\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
        ),
    )


async def _insert_morpho_market(
    conn: asyncpg.Connection,
    protocol_id: int,
    market_id: bytes,
    loan_token_id: int,
    collateral_token_id: int,
) -> int:
    """Insert a Morpho market and return its ID."""
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
            b"\x00" * 20,  # oracle_address
            b"\x00" * 20,  # irm_address
            Decimal("0.86"),  # lltv
            _SEED_BLOCK_NUMBER,
        ),
    )


async def _insert_morpho_market_state(
    conn: asyncpg.Connection,
    morpho_market_id: int,
    total_supply_assets: str,
    total_borrow_assets: str,
    block_number: int,
) -> None:
    """Insert a market state snapshot."""
    await conn.execute(
        """
        INSERT INTO morpho_market_state
            (morpho_market_id, block_number, block_version, timestamp,
             total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
             last_update, fee)
        VALUES ($1, $2, 0, NOW(), $3, $3, $4, $4, $2, 0)
        """,
        morpho_market_id,
        block_number,
        total_supply_assets,
        total_borrow_assets,
    )


async def _insert_morpho_vault(
    conn: asyncpg.Connection,
    protocol_id: int,
    address: bytes,
    asset_token_id: int,
    name: str = "Test Vault",
    symbol: str = "TV",
) -> int:
    """Insert a Morpho vault and return its ID."""
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
) -> None:
    """Insert a vault state snapshot."""
    await conn.execute(
        """
        INSERT INTO morpho_vault_state
            (morpho_vault_id, block_number, block_version, timestamp,
             total_assets, total_shares)
        VALUES ($1, $2, 0, NOW(), $3, $3)
        """,
        morpho_vault_id,
        block_number,
        total_assets,
    )


async def _insert_morpho_market_position(
    conn: asyncpg.Connection,
    user_id: int,
    morpho_market_id: int,
    supply_assets: str,
    block_number: int,
) -> None:
    """Insert a market position for the vault user (supply only, no borrowing)."""
    await conn.execute(
        """
        INSERT INTO morpho_market_position
            (user_id, morpho_market_id, block_number, block_version, timestamp,
             supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
        VALUES ($1, $2, $3, 0, NOW(), $4, 0, 0, $4, 0)
        """,
        user_id,
        morpho_market_id,
        block_number,
        supply_assets,
    )


async def _insert_prime(conn: asyncpg.Connection, name: str, vault_address: bytes) -> int:
    """Insert a prime entry and return its ID."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO prime (name, vault_address)
        VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address
        RETURNING id
        """,
            name,
            vault_address,
        ),
    )


async def _insert_allocation_position(
    conn: asyncpg.Connection,
    token_id: int,
    prime_id: int,
    proxy_address: bytes,
    balance: str,
    block_number: int,
) -> None:
    """Insert an allocation_position row linking a vault share token to a prime."""
    await conn.execute(
        """
        INSERT INTO allocation_position
            (chain_id, token_id, prime_id, proxy_address, balance, scaled_balance,
             block_number, block_version, tx_hash, log_index, tx_amount, direction)
        VALUES (1, $1, $2, $3, $4, $4, $5, 0, $6, 0, $4, 'in')
        """,
        token_id,
        prime_id,
        proxy_address,
        balance,
        block_number,
        b"\x00" * 32,  # tx_hash
    )


# ---------------------------------------------------------------------------
# Seed data
#
# Scenario: A USDC vault supplies into two Morpho markets.
#
#   Vault total_assets = 1,000,000 USDC (raw: 1_000_000_000_000 with 6 decimals)
#
#   Market A (WETH/USDC):
#     vault supplies 400,000 USDC (raw: 400_000_000_000)
#     market utilization = 80% (borrow 800B / supply 1T)
#
#   Market B (WBTC/USDC):
#     vault supplies 300,000 USDC (raw: 300_000_000_000)
#     market utilization = 50% (borrow 500B / supply 1T)
#
#   Idle capital = 1M - 400K - 300K = 300K USDC
#
# Expected breakdown:
#   WETH: 400K * 0.80 / 1M = 32.00%  → 320,000.00
#   WBTC: 300K * 0.50 / 1M = 15.00%  → 150,000.00
#   USDC: loan_token_pct(A)=0.08 + loan_token_pct(B)=0.15 + idle=0.30 = 53.00% → 530,000.00
# ---------------------------------------------------------------------------


_VAULT_ADDRESS = b"\xcc" * 20
_IDLE_VAULT_ADDRESS = b"\xdd" * 20
_USDC_ADDRESS = b"\xa0\xb8\x69\x91\xc6\x21\x8b\x36\xc1\xd1\x9d\x4a\x2e\x9e\xb0\xce\x36\x06\xeb\x48"
_WETH_ADDRESS = b"\xc0\x2a\xaa\x39\xb2\x23\xfe\x8d\x0a\x0e\x5c\x4f\x27\xea\xd9\x08\x3c\x75\x6c\xc2"
_WBTC_ADDRESS = b"\x22\x60\xfa\xc5\xe5\x54\x2a\x77\x3a\xa4\x4f\xbc\xfe\xdf\x7c\x19\x3b\xc2\xc5\x99"
_MUSDC_ADDRESS = b"\xee" * 20  # vault share token for mUSDC
_MUSDCI_ADDRESS = b"\xff" * 20  # vault share token for mUSDCi


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    """Seed test data for Morpho backed breakdown tests."""
    conn = await asyncpg.connect(db_url)
    try:
        block = _SEED_BLOCK_NUMBER

        # Protocol
        protocol_id = await _insert_protocol(conn)

        # Tokens – use existing if seeded by migrations, otherwise create
        usdc_id = await insert_token(conn, "USDC", 6, _USDC_ADDRESS)
        weth_id = await insert_token(conn, "WETH", 18, _WETH_ADDRESS)
        wbtc_id = await insert_token(conn, "WBTC", 8, _WBTC_ADDRESS)

        # Vault share tokens — symbol must match morpho_vault.symbol for the CTE JOIN
        musdc_token_id = await insert_token(conn, "mUSDC", 18, _MUSDC_ADDRESS)
        musdci_token_id = await insert_token(conn, "mUSDCi", 18, _MUSDCI_ADDRESS)

        # Prime — needed as FK for allocation_position.prime_id
        prime_id = await _insert_prime(conn, "test-prime", _VAULT_ADDRESS)

        # User for the vault (vault address == user address)
        vault_user_id = await insert_user(conn, _VAULT_ADDRESS)

        # Vault
        vault_id = await _insert_morpho_vault(
            conn, protocol_id, _VAULT_ADDRESS, usdc_id, name="Morpho USDC Vault", symbol="mUSDC"
        )

        # Allocation positions — link vault share tokens to the SQL entry point
        await _insert_allocation_position(conn, musdc_token_id, prime_id, _VAULT_ADDRESS, "1000000", block)

        # Vault state: 1M USDC in raw units (1_000_000 * 10^6)
        await _insert_morpho_vault_state(conn, vault_id, "1000000000000", block)

        # Market A: WETH/USDC
        market_a_id = await _insert_morpho_market(conn, protocol_id, b"\x01" * 32, usdc_id, weth_id)
        # Market A total supply=1T, total borrow=800B → 80% utilization
        await _insert_morpho_market_state(conn, market_a_id, "1000000000000", "800000000000", block)
        # Vault supplies 400K USDC (raw) to market A
        await _insert_morpho_market_position(conn, vault_user_id, market_a_id, "400000000000", block)

        # Market B: WBTC/USDC
        market_b_id = await _insert_morpho_market(conn, protocol_id, b"\x02" * 32, usdc_id, wbtc_id)
        # Market B total supply=1T, total borrow=500B → 50% utilization
        await _insert_morpho_market_state(conn, market_b_id, "1000000000000", "500000000000", block)
        # Vault supplies 300K USDC (raw) to market B
        await _insert_morpho_market_position(conn, vault_user_id, market_b_id, "300000000000", block)

        # Idle-only vault: 500K USDC total_assets, no market positions
        # This exercises the vault_idle path when breakdown is empty.
        idle_vault_user_id = await insert_user(conn, _IDLE_VAULT_ADDRESS)
        idle_vault_id = await _insert_morpho_vault(
            conn, protocol_id, _IDLE_VAULT_ADDRESS, usdc_id, name="Morpho USDC Idle Vault", symbol="mUSDCi"
        )
        await _insert_allocation_position(conn, musdci_token_id, prime_id, _IDLE_VAULT_ADDRESS, "500000", block)
        # 500K USDC in raw units (500_000 * 10^6)
        await _insert_morpho_vault_state(conn, idle_vault_id, "500000000000", block)
        del idle_vault_user_id  # user must exist for vault_users CTE; no positions inserted

        await store_test_ids(
            conn,
            {
                "protocol_id": protocol_id,
                "vault_token_id": musdc_token_id,
                "idle_vault_token_id": musdci_token_id,
                "usdc_id": usdc_id,
                "weth_id": weth_id,
                "wbtc_id": wbtc_id,
            },
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def test_ids(db_url: str, _seed_data: None) -> dict[str, int]:
    """Retrieve seeded IDs for use in test assertions."""
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT key, val FROM _test_ids")
        return {row["key"]: row["val"] for row in rows}
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(
    async_db_url: str, _seed_data: None, test_ids: dict[str, int]
) -> AsyncIterator[ProtocolScopedBackedBreakdownRepository]:
    """Create a Morpho repository already bound to the Morpho protocol."""
    engine = create_async_engine(async_db_url)
    try:
        repository_class = cast(Any, MorphoBackedBreakdownRepository)
        yield cast(
            ProtocolScopedBackedBreakdownRepository,
            repository_class(engine, protocol_id=test_ids["protocol_id"]),
        )
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_vault_backed_breakdown(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Vault with two market allocations produces correct collateral and loan token breakdown.

    Vault: 1M USDC total assets
      Market A (WETH/USDC): 400K supply, 80% utilization
      Market B (WBTC/USDC): 300K supply, 50% utilization

    Expected:
      USDC: 530,000 (53%)  — borrowed-but-not-utilized + idle
      WETH: 320,000 (32%)  — 400K * 0.80
      WBTC: 150,000 (15%)  — 300K * 0.50
    """
    result = await repository.get_backed_breakdown(test_ids["vault_token_id"])

    assert result.backed_asset_id == test_ids["vault_token_id"]
    assert result.protocol_id == test_ids["protocol_id"]
    assert len(result.items) == 3

    by_symbol = {item.symbol: item for item in result.items}

    assert "USDC" in by_symbol
    assert "WETH" in by_symbol
    assert "WBTC" in by_symbol

    assert by_symbol["USDC"].backing_usd == Decimal("530000.00")
    assert by_symbol["USDC"].backing_pct == Decimal("53.00")

    assert by_symbol["WETH"].backing_usd == Decimal("320000.00")
    assert by_symbol["WETH"].backing_pct == Decimal("32.00")

    assert by_symbol["WBTC"].backing_usd == Decimal("150000.00")
    assert by_symbol["WBTC"].backing_pct == Decimal("15.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_nonexistent_vault_returns_empty(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """A non-existent vault ID returns an empty breakdown."""
    result = await repository.get_backed_breakdown(99999)

    assert result.items == ()


@pytest.mark.asyncio(loop_scope="module")
async def test_repository_preserves_protocol_binding_in_result(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Morpho result should reflect repository-bound protocol context, not call-site pass-through."""
    result = await repository.get_backed_breakdown(test_ids["vault_token_id"])

    assert result.protocol_id == test_ids["protocol_id"]


@pytest.mark.asyncio(loop_scope="module")
async def test_items_ordered_by_backed_amount_desc(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Results should be ordered by backed_amount descending."""
    result = await repository.get_backed_breakdown(test_ids["vault_token_id"])

    amounts = [item.backing_usd for item in result.items]
    assert amounts == sorted(amounts, reverse=True)


@pytest.mark.asyncio(loop_scope="module")
async def test_percentages_sum_to_100(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """All backing percentages should sum to 100%."""
    result = await repository.get_backed_breakdown(test_ids["vault_token_id"])

    total_pct = sum(item.backing_pct for item in result.items)
    assert total_pct == Decimal("100.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_token_ids_are_populated(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Each item should have the correct token_id."""
    result = await repository.get_backed_breakdown(test_ids["vault_token_id"])

    by_symbol = {item.symbol: item for item in result.items}

    assert by_symbol["USDC"].token_id == test_ids["usdc_id"]
    assert by_symbol["WETH"].token_id == test_ids["weth_id"]
    assert by_symbol["WBTC"].token_id == test_ids["wbtc_id"]


@pytest.mark.asyncio(loop_scope="module")
async def test_vault_with_no_market_positions_is_fully_idle(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Vault with total_assets > 0 but no market positions should be 100% the asset token (idle).

    Idle-only vault: 500,000 USDC total_assets, zero market positions.

    Expected:
      vault_idle = total_assets - 0 = 500,000 USDC
      breakdown and all_backing second-branch are both empty.
      Result: USDC 100% at 500,000.00
    """
    result = await repository.get_backed_breakdown(test_ids["idle_vault_token_id"])

    assert len(result.items) == 1

    item = result.items[0]
    assert item.symbol == "USDC"
    assert item.backing_usd == Decimal("500000.00")
    assert item.backing_pct == Decimal("100.00")
    assert item.token_id == test_ids["usdc_id"]
