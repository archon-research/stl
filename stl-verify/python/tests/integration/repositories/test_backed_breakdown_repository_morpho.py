from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, Protocol, cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.domain.entities.backed_breakdown import BackedBreakdown
from tests.integration.seed import insert_oracle_asset, insert_token, insert_user, store_test_ids


class ProtocolScopedBackedBreakdownRepository(Protocol):
    """Spec contract: Morpho repository is selected by protocol before invocation."""

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown: ...


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

_SEED_BLOCK_NUMBER = 20_000_000

# Own-token USD prices seeded via the Morpho Blue -> chainlink oracle binding. USDC
# is deliberately off $1 so assertions prove items carry the real fetched price, not
# a hardcoded 1; WETH/WBTC give the main vault's collateral rows their own non-$1
# prices (each row exposes its OWN token's price, as the Aave repo does).
_USDC_PRICE_USD = Decimal("1.0001")
_WETH_PRICE_USD = Decimal("2000")
_WBTC_PRICE_USD = Decimal("60000")

# Two prices for one loan token at different blocks: the latest snapshot must win.
_LATEST_OLD_PRICE_USD = Decimal("9")
_LATEST_NEW_PRICE_USD = Decimal("11")
_OLDER_BLOCK_NUMBER = _SEED_BLOCK_NUMBER - 1

# A loan-token price reachable only through a disabled oracle_asset mapping: the
# enabled-gate must exclude it, leaving the vault unpriced.
_DISABLED_PRICE_USD = Decimal("7")


def _price_str(price: Decimal) -> str:
    """Render a price as the 18-decimal string onchain_token_price expects."""
    return f"{price:.18f}"


async def _insert_token_price(
    conn: asyncpg.Connection,
    token_id: int,
    oracle_id: int,
    price: Decimal,
    *,
    block_number: int = _SEED_BLOCK_NUMBER,
    block_version: int = 0,
    enabled: bool = True,
) -> None:
    """Insert an onchain price for a token + its oracle_asset mapping.

    ``enabled=False`` models a retired source: the mapping exists but the breakdown
    query's enabled-gate must exclude the price.
    """
    await conn.execute(
        """
        INSERT INTO onchain_token_price
            (token_id, oracle_id, block_number, block_version, timestamp, price_usd)
        VALUES ($1, $2, $3, $4, NOW(), $5::numeric(30,18))
        """,
        token_id,
        oracle_id,
        block_number,
        block_version,
        _price_str(price),
    )
    await insert_oracle_asset(conn, oracle_id, token_id, enabled=enabled)


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


async def _seed_idle_vault(
    conn: asyncpg.Connection,
    *,
    protocol_id: int,
    prime_id: int,
    vault_address: bytes,
    share_symbol: str,
    share_address: bytes,
    loan_token_id: int,
    total_assets_raw: str,
    block: int,
    name: str,
) -> int:
    """Seed an idle-only Morpho vault (no market positions) and return its id.

    An idle-only vault holds its whole balance as the loan token, so the breakdown
    is a single loan-token row — the shape every non-stablecoin / pricing scenario
    below reuses. A user row must exist for the vault_users CTE even with no
    positions.
    """
    await insert_user(conn, vault_address)
    share_token_id = await insert_token(conn, share_symbol, 18, share_address)
    vault_id = await _insert_morpho_vault(
        conn, protocol_id, vault_address, loan_token_id, name=name, symbol=share_symbol
    )
    await _insert_allocation_position(conn, share_token_id, prime_id, vault_address, "100", block)
    await _insert_morpho_vault_state(conn, vault_id, total_assets_raw, block)
    return vault_id


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
_WETH_VAULT_ADDRESS = b"\x1a" * 20  # idle-only vault whose loan token is WETH (non-stablecoin)
_MWETHI_ADDRESS = b"\x1b" * 20  # vault share token for mWETHi
_UNPRICED_VAULT_ADDRESS = b"\x2a" * 20  # idle-only vault whose loan token has no price
_MUNPXI_ADDRESS = b"\x2b" * 20  # vault share token for mUNPXi
_UNPX_ADDRESS = b"\x2c" * 20  # loan token with no onchain price
_LATEST_VAULT_ADDRESS = b"\x3a" * 20  # idle-only vault whose loan token has two price snapshots
_MLATEI_ADDRESS = b"\x3b" * 20  # vault share token for mLATEi
_LATE_ADDRESS = b"\x3c" * 20  # loan token priced twice at different blocks
_DISABLED_VAULT_ADDRESS = b"\x4a" * 20  # idle-only vault whose loan-token price is via a disabled oracle
_MDISI_ADDRESS = b"\x4b" * 20  # vault share token for mDISi
_DIS_ADDRESS = b"\x4c" * 20  # loan token priced only through a disabled oracle_asset mapping


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

        # Main vault's share token — symbol must match morpho_vault.symbol for the CTE JOIN
        musdc_token_id = await insert_token(conn, "mUSDC", 18, _MUSDC_ADDRESS)

        # Own-token prices via the Morpho Blue -> chainlink binding, so every row carries
        # its own token's price_usd. chainlink is migration-seeded; the breakdown query
        # reaches it through protocol_oracle.
        chainlink_oracle_id = cast(int, await conn.fetchval("SELECT id FROM oracle WHERE name = 'chainlink'"))
        await conn.execute(
            """
            INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
            VALUES ($1, $2, $3)
            ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING
            """,
            protocol_id,
            chainlink_oracle_id,
            _SEED_BLOCK_NUMBER,
        )
        await _insert_token_price(conn, usdc_id, chainlink_oracle_id, _USDC_PRICE_USD)
        await _insert_token_price(conn, weth_id, chainlink_oracle_id, _WETH_PRICE_USD)
        await _insert_token_price(conn, wbtc_id, chainlink_oracle_id, _WBTC_PRICE_USD)

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

        # Idle-only vault: 500K USDC total_assets, no market positions — exercises the
        # vault_idle path when the breakdown is empty.
        idle_vault_id = await _seed_idle_vault(
            conn,
            protocol_id=protocol_id,
            prime_id=prime_id,
            vault_address=_IDLE_VAULT_ADDRESS,
            share_symbol="mUSDCi",
            share_address=_MUSDCI_ADDRESS,
            loan_token_id=usdc_id,
            total_assets_raw="500000000000",  # 500K USDC (6 dec)
            block=block,
            name="Morpho USDC Idle Vault",
        )

        # Non-stablecoin loan-token vault: 100 WETH idle, WETH priced at $2,000. Exercises
        # the loan-token -> USD scaling (100 * 2000 = 200,000) that a stablecoin vault at
        # ~$1 leaves visually indistinguishable.
        weth_vault_id = await _seed_idle_vault(
            conn,
            protocol_id=protocol_id,
            prime_id=prime_id,
            vault_address=_WETH_VAULT_ADDRESS,
            share_symbol="mWETHi",
            share_address=_MWETHI_ADDRESS,
            loan_token_id=weth_id,
            total_assets_raw="100000000000000000000",  # 100 WETH (18 dec)
            block=block,
            name="Morpho WETH Idle Vault",
        )

        # Unpriced loan-token vault: 100 UNPX idle, no price seeded for UNPX. The loan
        # token has no USD price, so backing_value stays raw and price_usd is None on
        # every row; the risk service treats that as price_data_missing.
        unpx_id = await insert_token(conn, "UNPX", 18, _UNPX_ADDRESS)
        unpriced_vault_id = await _seed_idle_vault(
            conn,
            protocol_id=protocol_id,
            prime_id=prime_id,
            vault_address=_UNPRICED_VAULT_ADDRESS,
            share_symbol="mUNPXi",
            share_address=_MUNPXI_ADDRESS,
            loan_token_id=unpx_id,
            total_assets_raw="100000000000000000000",  # 100 UNPX (18 dec)
            block=block,
            name="Morpho Unpriced Vault",
        )

        # Two-price vault: 100 LATE idle, LATE priced at two blocks. The latest snapshot
        # ($11 at the newer block) must win over the stale $9 — proving the CTE's ordering
        # and DISTINCT ON select the current price (and never fan out the join).
        late_id = await insert_token(conn, "LATE", 18, _LATE_ADDRESS)
        await _insert_token_price(
            conn, late_id, chainlink_oracle_id, _LATEST_OLD_PRICE_USD, block_number=_OLDER_BLOCK_NUMBER
        )
        await _insert_token_price(
            conn, late_id, chainlink_oracle_id, _LATEST_NEW_PRICE_USD, block_number=_SEED_BLOCK_NUMBER
        )
        latest_vault_id = await _seed_idle_vault(
            conn,
            protocol_id=protocol_id,
            prime_id=prime_id,
            vault_address=_LATEST_VAULT_ADDRESS,
            share_symbol="mLATEi",
            share_address=_MLATEI_ADDRESS,
            loan_token_id=late_id,
            total_assets_raw="100000000000000000000",  # 100 LATE (18 dec)
            block=block,
            name="Morpho Latest-Price Vault",
        )

        # Disabled-oracle vault: 100 DIS idle, DIS priced only through a disabled
        # oracle_asset mapping. The enabled-gate must exclude it, so the vault reads as
        # unpriced exactly like a token with no price row at all.
        dis_id = await insert_token(conn, "DIS", 18, _DIS_ADDRESS)
        await _insert_token_price(conn, dis_id, chainlink_oracle_id, _DISABLED_PRICE_USD, enabled=False)
        disabled_vault_id = await _seed_idle_vault(
            conn,
            protocol_id=protocol_id,
            prime_id=prime_id,
            vault_address=_DISABLED_VAULT_ADDRESS,
            share_symbol="mDISi",
            share_address=_MDISI_ADDRESS,
            loan_token_id=dis_id,
            total_assets_raw="100000000000000000000",  # 100 DIS (18 dec)
            block=block,
            name="Morpho Disabled-Oracle Vault",
        )

        await store_test_ids(
            conn,
            {
                "protocol_id": protocol_id,
                "vault_id": vault_id,
                "idle_vault_id": idle_vault_id,
                "weth_idle_vault_id": weth_vault_id,
                "unpriced_vault_id": unpriced_vault_id,
                "latest_vault_id": latest_vault_id,
                "disabled_vault_id": disabled_vault_id,
                "usdc_id": usdc_id,
                "weth_id": weth_id,
                "wbtc_id": wbtc_id,
                "unpx_id": unpx_id,
                "late_id": late_id,
                "dis_id": dis_id,
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
    """Create the Morpho backed breakdown repository."""
    engine = create_async_engine(async_db_url)
    try:
        repository_class = cast(Any, MorphoBackedBreakdownRepository)
        yield cast(
            ProtocolScopedBackedBreakdownRepository,
            repository_class(engine),
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

    Vault: 1M USDC total assets, loan token priced at $1.0001.
      Market A (WETH/USDC): 400K supply, 80% utilization
      Market B (WBTC/USDC): 300K supply, 50% utilization

    backing_value is USD (loan-token units * loan-token price):
      USDC: 530,000 * 1.0001 = 530,053.00 (53%)  — borrowed-but-not-utilized + idle
      WETH: 320,000 * 1.0001 = 320,032.00 (32%)  — 400K * 0.80
      WBTC: 150,000 * 1.0001 = 150,015.00 (15%)  — 300K * 0.50
    """
    result = await repository.get_backed_breakdown(test_ids["vault_id"])

    assert result.backed_asset_id == test_ids["vault_id"]
    assert len(result.items) == 3

    by_symbol = {item.symbol: item for item in result.items}

    assert "USDC" in by_symbol
    assert "WETH" in by_symbol
    assert "WBTC" in by_symbol

    assert by_symbol["USDC"].backing_value == Decimal("530053.00")
    assert by_symbol["USDC"].backing_pct == Decimal("53.00")

    assert by_symbol["WETH"].backing_value == Decimal("320032.00")
    assert by_symbol["WETH"].backing_pct == Decimal("32.00")

    assert by_symbol["WBTC"].backing_value == Decimal("150015.00")
    assert by_symbol["WBTC"].backing_pct == Decimal("15.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_nonexistent_vault_returns_empty(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """A non-existent vault ID returns an empty breakdown."""
    result = await repository.get_backed_breakdown(99999)

    assert result.items == ()


@pytest.mark.asyncio(loop_scope="module")
async def test_items_ordered_by_backed_amount_desc(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Results should be ordered by backed_amount descending."""
    result = await repository.get_backed_breakdown(test_ids["vault_id"])

    amounts = [item.backing_value for item in result.items]
    assert amounts == sorted(amounts, reverse=True)


@pytest.mark.asyncio(loop_scope="module")
async def test_percentages_sum_to_100(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """All backing percentages should sum to 100%."""
    result = await repository.get_backed_breakdown(test_ids["vault_id"])

    total_pct = sum(item.backing_pct for item in result.items)
    assert total_pct == Decimal("100.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_token_ids_are_populated(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Each item should have the correct token_id."""
    result = await repository.get_backed_breakdown(test_ids["vault_id"])

    by_symbol = {item.symbol: item for item in result.items}

    assert by_symbol["USDC"].token_id == test_ids["usdc_id"]
    assert by_symbol["WETH"].token_id == test_ids["weth_id"]
    assert by_symbol["WBTC"].token_id == test_ids["wbtc_id"]


async def _assert_single_idle_row(
    repository: ProtocolScopedBackedBreakdownRepository,
    vault_id: int,
    *,
    symbol: str,
    token_id: int,
    price_usd: Decimal | None,
    backing_value: Decimal,
) -> None:
    """Assert an idle-only vault yields exactly one loan-token row with the given values."""
    result = await repository.get_backed_breakdown(vault_id)
    assert len(result.items) == 1
    item = result.items[0]
    assert item.symbol == symbol
    assert item.token_id == token_id
    assert item.price_usd == price_usd
    assert item.backing_value == backing_value
    assert item.backing_pct == Decimal("100.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_items_carry_own_token_price(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Each row carries its OWN token's USD price, not the loan token's.

    The main vault's loan token is USDC ($1.0001); its collateral rows expose the
    collateral token's own price (WETH $2,000, WBTC $60,000) so amount and price stay
    denominated in the row's symbol, as the Aave repo does. A non-null price_usd is
    also what keeps CryptoLendingRiskService from dropping the row at enrichment.
    """
    result = await repository.get_backed_breakdown(test_ids["vault_id"])

    by_symbol = {item.symbol: item for item in result.items}
    assert by_symbol["USDC"].price_usd == _USDC_PRICE_USD
    assert by_symbol["WETH"].price_usd == _WETH_PRICE_USD
    assert by_symbol["WBTC"].price_usd == _WBTC_PRICE_USD


@pytest.mark.asyncio(loop_scope="module")
async def test_nonstablecoin_loan_token_scales_backing_value_to_usd(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """A vault whose loan token is not ~$1 has backing_value scaled to USD.

    100 WETH idle, WETH @ $2,000 -> 100% WETH at 200,000.00 USD (not 100 units).
    """
    await _assert_single_idle_row(
        repository,
        test_ids["weth_idle_vault_id"],
        symbol="WETH",
        token_id=test_ids["weth_id"],
        price_usd=_WETH_PRICE_USD,
        backing_value=Decimal("200000.00"),
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_unpriced_loan_token_keeps_raw_units_and_null_price(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """A loan token with no price yields price_usd None and raw (unscaled) backing_value.

    100 UNPX idle, no price -> price_usd None, backing_value 100.00 (loan-token units,
    NOT USD). The risk service treats an all-unpriced breakdown as price_data_missing,
    so the raw units never leak as USD.
    """
    await _assert_single_idle_row(
        repository,
        test_ids["unpriced_vault_id"],
        symbol="UNPX",
        token_id=test_ids["unpx_id"],
        price_usd=None,
        backing_value=Decimal("100.00"),
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_latest_price_snapshot_wins(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Among two price snapshots for the loan token, the latest block wins.

    LATE is priced $9 at an older block then $11 at the newer block: the row must use
    $11 (and backing_value scales by it, 100 * 11 = 1,100.00), never the stale $9. This
    exercises the CTE's snapshot ordering and DISTINCT ON — and that the price join
    never fans the breakdown out across the two rows.
    """
    await _assert_single_idle_row(
        repository,
        test_ids["latest_vault_id"],
        symbol="LATE",
        token_id=test_ids["late_id"],
        price_usd=_LATEST_NEW_PRICE_USD,
        backing_value=Decimal("1100.00"),
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_disabled_oracle_mapping_leaves_vault_unpriced(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """A loan-token price reachable only via a disabled oracle_asset is excluded.

    DIS has a price row but its oracle_asset mapping is disabled, so the enabled-gate
    drops it: the vault reads unpriced (price_usd None, raw backing_value 100.00),
    identical to a token with no price row at all. Distinct from the no-row case, this
    proves the gate filters on the mapping, not merely on row existence.
    """
    await _assert_single_idle_row(
        repository,
        test_ids["disabled_vault_id"],
        symbol="DIS",
        token_id=test_ids["dis_id"],
        price_usd=None,
        backing_value=Decimal("100.00"),
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_vault_with_no_market_positions_is_fully_idle(
    repository: ProtocolScopedBackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Vault with total_assets > 0 but no market positions is 100% the loan token (idle).

    Idle-only vault: 500,000 USDC total_assets, zero market positions. The whole
    balance is idle loan token, priced at its own $1.0001:
    500,000 * 1.0001 = 500,050.00 USD.
    """
    await _assert_single_idle_row(
        repository,
        test_ids["idle_vault_id"],
        symbol="USDC",
        token_id=test_ids["usdc_id"],
        price_usd=_USDC_PRICE_USD,
        backing_value=Decimal("500050.00"),
    )
