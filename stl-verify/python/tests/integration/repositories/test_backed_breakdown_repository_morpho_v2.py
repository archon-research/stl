from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, Protocol, cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.backed_breakdown_repository_morpho_v2 import MorphoV2BackedBreakdownRepository
from app.domain.entities.backed_breakdown import BackedBreakdown
from tests.integration.seed import insert_oracle_asset, insert_token, insert_user, store_test_ids


class BackedBreakdownRepository(Protocol):
    """Spec contract: the V2 repository is selected by vault_version before invocation."""

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown: ...


# ---------------------------------------------------------------------------
# Seed helpers (mirrors test_backed_breakdown_repository_morpho.py; the V2 walk
# reads morpho_vault by id directly, so no allocation_position / prime seed).
# ---------------------------------------------------------------------------

_SEED_BLOCK = 20_000_000
_STALE_BLOCK = 19_999_999  # lower block: every stale row lives here to prove latest-selection

# Adapter types (mirrors morpho_adapter.adapter_type in migration 20260721_120000).
_ADAPTER_TYPE_BLUE_MARKET = 1  # MorphoMarketV1AdapterV2 → Morpho Blue markets
_ADAPTER_TYPE_V1_VAULT = 2  # MorphoVaultV1Adapter → nested MetaMorpho V1 vault
_ADAPTER_TYPE_UNKNOWN = 99  # unrecognised adapter kind, surfaced as the vault asset

_USDC_ADDRESS = b"\xa0\xb8\x69\x91\xc6\x21\x8b\x36\xc1\xd1\x9d\x4a\x2e\x9e\xb0\xce\x36\x06\xeb\x48"
_WETH_ADDRESS = b"\xc0\x2a\xaa\x39\xb2\x23\xfe\x8d\x0a\x0e\x5c\x4f\x27\xea\xd9\x08\x3c\x75\x6c\xc2"
_WBTC_ADDRESS = b"\x22\x60\xfa\xc5\xe5\x54\x2a\x77\x3a\xa4\x4f\xbc\xfe\xdf\x7c\x19\x3b\xc2\xc5\x99"

# Own-token USD prices via the Morpho Blue → chainlink protocol_oracle binding, so
# every row carries its OWN token's price (as the V1 repo does post-VEC-511). USDC is
# deliberately off $1 so the loan-token USD-scaling of backing_value is observable.
_USDC_PRICE_USD = Decimal("1.0001")
_WETH_PRICE_USD = Decimal("2000")
_WBTC_PRICE_USD = Decimal("60000")


def _price_str(price: Decimal) -> str:
    """Render a price as the 18-decimal string onchain_token_price expects."""
    return f"{price:.18f}"


async def _insert_token_price(conn: asyncpg.Connection, token_id: int, oracle_id: int, price: Decimal) -> None:
    """Insert an onchain price for a token plus its enabled oracle_asset mapping."""
    await conn.execute(
        """
        INSERT INTO onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, price_usd)
        VALUES ($1, $2, $3, 0, NOW(), $4)
        """,
        token_id,
        oracle_id,
        _SEED_BLOCK,
        _price_str(price),
    )
    await insert_oracle_asset(conn, oracle_id, token_id, enabled=True)


_V2_VAULT_ADDRESS = b"\xcc" * 20
_V1_VAULT_ADDRESS = b"\xdd" * 20
_ADAPTER1_ADDRESS = b"\xa1" * 20  # type 1 (Blue markets)
_ADAPTER2_ADDRESS = b"\xa2" * 20  # type 2 (nested V1 vault)
_ADAPTER3_ADDRESS = b"\xa3" * 20  # type 1 but REMOVED (must be excluded)

# Second, isolated V2 vault exercising the type-99 unknown-adapter line.
_V2B_VAULT_ADDRESS = b"\xc9" * 20
_ADAPTER99_ADDRESS = b"\xa9" * 20


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
            b"\xbb\xbb\xbb\xbb\xbb\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
        ),
    )


async def _insert_morpho_vault(
    conn: asyncpg.Connection, protocol_id: int, address: bytes, asset_token_id: int, vault_version: int, symbol: str
) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO morpho_vault
            (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
        VALUES (1, $1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """,
            protocol_id,
            address,
            symbol,
            symbol,
            asset_token_id,
            vault_version,
            _SEED_BLOCK,
        ),
    )


async def _insert_morpho_vault_state(conn: asyncpg.Connection, vault_id: int, total_assets: str, block: int) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_vault_state
            (morpho_vault_id, block_number, block_version, timestamp, total_assets, total_shares)
        VALUES ($1, $2, 0, NOW(), $3, $3)
        """,
        vault_id,
        block,
        total_assets,
    )


async def _insert_morpho_vault_position(
    conn: asyncpg.Connection, user_id: int, vault_id: int, assets: str, block: int
) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_vault_position
            (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
        VALUES ($1, $2, $3, 0, NOW(), $4, $4)
        """,
        user_id,
        vault_id,
        block,
        assets,
    )


async def _insert_morpho_market(
    conn: asyncpg.Connection, protocol_id: int, market_id: bytes, loan_token_id: int, collateral_token_id: int
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
            _SEED_BLOCK,
        ),
    )


async def _insert_morpho_market_state(
    conn: asyncpg.Connection, market_id: int, total_supply_assets: str, total_borrow_assets: str, block: int
) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_market_state
            (morpho_market_id, block_number, block_version, timestamp,
             total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares, last_update, fee)
        VALUES ($1, $2, 0, NOW(), $3, $3, $4, $4, $2, 0)
        """,
        market_id,
        block,
        total_supply_assets,
        total_borrow_assets,
    )


async def _insert_morpho_market_position(
    conn: asyncpg.Connection, user_id: int, market_id: int, supply_assets: str, block: int
) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_market_position
            (user_id, morpho_market_id, block_number, block_version, timestamp,
             supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
        VALUES ($1, $2, $3, 0, NOW(), $4, 0, 0, $4, 0)
        """,
        user_id,
        market_id,
        block,
        supply_assets,
    )


async def _insert_morpho_adapter(
    conn: asyncpg.Connection,
    vault_id: int,
    address: bytes,
    asset_token_id: int,
    adapter_type: int,
    added_at_block: int,
    removed_at_block: int | None = None,
) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO morpho_adapter
            (morpho_vault_id, address, asset_token_id, adapter_type, added_at_block, removed_at_block)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
        """,
            vault_id,
            address,
            asset_token_id,
            adapter_type,
            added_at_block,
            removed_at_block,
        ),
    )


async def _insert_morpho_adapter_state(conn: asyncpg.Connection, adapter_id: int, real_assets: str, block: int) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_adapter_state (morpho_adapter_id, block_number, block_version, timestamp, real_assets)
        VALUES ($1, $2, 0, NOW(), $3)
        """,
        adapter_id,
        block,
        real_assets,
    )


# ---------------------------------------------------------------------------
# Seed data — arithmetic is documented inline like the V1 test.
#
# All amounts are in the vault's underlying asset (USDC, 6 decimals). USDC is the
# loan token of every Blue market here, so per V1 methodology every backing amount
# is USDC-denominated and the collateral token_id only labels the row.
#
# V2 vault "A" (vault_version=3), total_assets = 1,000,000 USDC:
#   adapter1 (type 1, Blue markets), real_assets = 500,000:
#       Market A (WETH/USDC): supply 300,000, util 80%  → WETH 240,000 | USDC idle-loan 60,000
#       Market B (WBTC/USDC): supply 200,000, util 50%  → WBTC 100,000 | USDC idle-loan 100,000
#       (300,000 + 200,000 = 500,000 = real_assets)
#   adapter2 (type 2, nested V1 vault), real_assets = 300,000, holds 300,000 of the V1 vault:
#       nested V1 vault total_assets = 600,000; adapter share = 300,000/600,000 = 0.5
#       Market C (WETH/USDC): supply 400,000, util 75%  → WETH 300,000 | USDC idle-loan 100,000
#       V1 vault idle = 600,000 - 400,000 = 200,000 USDC
#       scaled to adapter (×0.5): WETH 150,000 | USDC idle-loan 50,000 | USDC vault-idle 100,000
#   adapter3 (type 1, REMOVED): a 999,999 WETH-market position that must NOT appear.
#   V2 vault idle = total_assets 1,000,000 - Σ real_assets (500,000 + 300,000) = 200,000 USDC.
#
# Aggregated:
#   WETH = 240,000 (a1) + 150,000 (a2)                         = 390,000  (39.00%)
#   WBTC = 100,000 (a1)                                        = 100,000  (10.00%)
#   USDC = 60,000 + 100,000 (a1 idle-loan)
#          + 50,000 (a2 idle-loan) + 100,000 (a2 vault-idle)
#          + 200,000 (V2 vault idle)                            = 510,000  (51.00%)
#   Total = 1,000,000 USDC.
#
# Stale rows (all at _STALE_BLOCK, i.e. lower block) prove latest-selection:
#   adapter1 state 999,999,999,999 | V2 vault_state 123 | V1 vault_position 777...
#   Market A state 10% util | adapter1 Market A position 111... — none may win.
#
# V2 vault "B" (vault_version=3), total_assets = 400,000 USDC:
#   adapter99 (type 99, unknown), real_assets = 250,000 → surfaced as USDC.
#   V2 vault idle = 400,000 - 250,000 = 150,000 USDC.
#   Aggregated USDC = 250,000 + 150,000 = 400,000 (100.00%). Dropping the unknown
#   adapter would leave only 150,000 — the amount distinguishes surfaced vs dropped.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await _insert_protocol(conn)

        usdc_id = await insert_token(conn, "USDC", 6, _USDC_ADDRESS)
        weth_id = await insert_token(conn, "WETH", 18, _WETH_ADDRESS)
        wbtc_id = await insert_token(conn, "WBTC", 8, _WBTC_ADDRESS)

        # Own-token prices via the Morpho Blue → chainlink binding (chainlink is
        # migration-seeded; the breakdown query reaches it through protocol_oracle),
        # so every row carries its own token's price and backing_value is USD-scaled.
        chainlink_oracle_id = cast(int, await conn.fetchval("SELECT id FROM oracle WHERE name = 'chainlink'"))
        await conn.execute(
            """
            INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
            VALUES ($1, $2, 1)
            ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING
            """,
            protocol_id,
            chainlink_oracle_id,
        )
        await _insert_token_price(conn, usdc_id, chainlink_oracle_id, _USDC_PRICE_USD)
        await _insert_token_price(conn, weth_id, chainlink_oracle_id, _WETH_PRICE_USD)
        await _insert_token_price(conn, wbtc_id, chainlink_oracle_id, _WBTC_PRICE_USD)

        # Users keyed by the address that appears as "user" on positions.
        adapter1_user = await insert_user(conn, _ADAPTER1_ADDRESS)
        adapter2_user = await insert_user(conn, _ADAPTER2_ADDRESS)
        adapter3_user = await insert_user(conn, _ADAPTER3_ADDRESS)
        v1_vault_user = await insert_user(conn, _V1_VAULT_ADDRESS)

        # --- V2 vault A + nested V1 vault -----------------------------------
        v2_vault_id = await _insert_morpho_vault(conn, protocol_id, _V2_VAULT_ADDRESS, usdc_id, 3, "sV2A")
        v1_vault_id = await _insert_morpho_vault(conn, protocol_id, _V1_VAULT_ADDRESS, usdc_id, 1, "sV1")

        await _insert_morpho_vault_state(conn, v2_vault_id, "1000000000000", _SEED_BLOCK)
        await _insert_morpho_vault_state(conn, v2_vault_id, "123", _STALE_BLOCK)  # stale, must lose
        await _insert_morpho_vault_state(conn, v1_vault_id, "600000000000", _SEED_BLOCK)

        # Adapters
        adapter1_id = await _insert_morpho_adapter(
            conn, v2_vault_id, _ADAPTER1_ADDRESS, usdc_id, _ADAPTER_TYPE_BLUE_MARKET, _SEED_BLOCK
        )
        adapter2_id = await _insert_morpho_adapter(
            conn, v2_vault_id, _ADAPTER2_ADDRESS, usdc_id, _ADAPTER_TYPE_V1_VAULT, _SEED_BLOCK
        )
        adapter3_id = await _insert_morpho_adapter(
            conn,
            v2_vault_id,
            _ADAPTER3_ADDRESS,
            usdc_id,
            _ADAPTER_TYPE_BLUE_MARKET,
            _SEED_BLOCK - 10,
            removed_at_block=_SEED_BLOCK - 1,
        )

        await _insert_morpho_adapter_state(conn, adapter1_id, "500000000000", _SEED_BLOCK)
        await _insert_morpho_adapter_state(conn, adapter1_id, "999999999999", _STALE_BLOCK)  # stale, must lose
        await _insert_morpho_adapter_state(conn, adapter2_id, "300000000000", _SEED_BLOCK)
        await _insert_morpho_adapter_state(conn, adapter3_id, "888888888888", _SEED_BLOCK)  # removed → excluded

        # Blue markets
        market_a = await _insert_morpho_market(conn, protocol_id, b"\x01" * 32, usdc_id, weth_id)
        market_b = await _insert_morpho_market(conn, protocol_id, b"\x02" * 32, usdc_id, wbtc_id)
        market_c = await _insert_morpho_market(conn, protocol_id, b"\x03" * 32, usdc_id, weth_id)

        await _insert_morpho_market_state(conn, market_a, "1000000000000", "800000000000", _SEED_BLOCK)  # 80%
        await _insert_morpho_market_state(
            conn, market_a, "1000000000000", "100000000000", _STALE_BLOCK
        )  # 10%, must lose
        await _insert_morpho_market_state(conn, market_b, "1000000000000", "500000000000", _SEED_BLOCK)  # 50%
        await _insert_morpho_market_state(conn, market_c, "1000000000000", "750000000000", _SEED_BLOCK)  # 75%

        # adapter1 (type 1) Blue positions
        await _insert_morpho_market_position(conn, adapter1_user, market_a, "300000000000", _SEED_BLOCK)
        await _insert_morpho_market_position(
            conn, adapter1_user, market_a, "111111111111", _STALE_BLOCK
        )  # stale, must lose
        await _insert_morpho_market_position(conn, adapter1_user, market_b, "200000000000", _SEED_BLOCK)
        # adapter3 (removed) Blue position — must be excluded entirely.
        await _insert_morpho_market_position(conn, adapter3_user, market_a, "999999999999", _SEED_BLOCK)

        # adapter2 (type 2) holds shares in the nested V1 vault.
        await _insert_morpho_vault_position(conn, adapter2_user, v1_vault_id, "300000000000", _SEED_BLOCK)
        await _insert_morpho_vault_position(
            conn, adapter2_user, v1_vault_id, "777777777777", _STALE_BLOCK
        )  # stale, must lose
        # The nested V1 vault's own allocation (user = V1 vault address).
        await _insert_morpho_market_position(conn, v1_vault_user, market_c, "400000000000", _SEED_BLOCK)

        # --- V2 vault B (type-99 unknown adapter) ---------------------------
        v2b_vault_id = await _insert_morpho_vault(conn, protocol_id, _V2B_VAULT_ADDRESS, usdc_id, 3, "sV2B")
        await _insert_morpho_vault_state(conn, v2b_vault_id, "400000000000", _SEED_BLOCK)
        adapter99_id = await _insert_morpho_adapter(
            conn, v2b_vault_id, _ADAPTER99_ADDRESS, usdc_id, _ADAPTER_TYPE_UNKNOWN, _SEED_BLOCK
        )
        await _insert_morpho_adapter_state(conn, adapter99_id, "250000000000", _SEED_BLOCK)

        await store_test_ids(
            conn,
            {
                "v2_vault_id": v2_vault_id,
                "v1_vault_id": v1_vault_id,
                "v2b_vault_id": v2b_vault_id,
                "usdc_id": usdc_id,
                "weth_id": weth_id,
                "wbtc_id": wbtc_id,
            },
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def test_ids(db_url: str, _seed_data: None) -> dict[str, int]:
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT key, val FROM _test_ids")
        return {row["key"]: row["val"] for row in rows}
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(
    async_db_url: str, _seed_data: None, test_ids: dict[str, int]
) -> AsyncIterator[BackedBreakdownRepository]:
    engine = create_async_engine(async_db_url)
    try:
        repository_class = cast(Any, MorphoV2BackedBreakdownRepository)
        yield cast(BackedBreakdownRepository, repository_class(engine))
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_v2_backed_breakdown_aggregates_adapters(
    repository: BackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    """Type-1 (Blue) + type-2 (nested V1) adapters + vault idle aggregate correctly.

    Raw loan-token amounts (USDC): USDC 510,000 (51%), WETH 390,000 (39%), WBTC 100,000
    (10%). backing_value is USD = raw × loan-token price (USDC 1.0001), so:
      USDC 510,051.00  WETH 390,039.00  WBTC 100,010.00
    price_usd is each row's OWN token price. The removed adapter3 (999,999 WETH) must be
    absent: WETH is exactly 390,039 (i.e. raw 390,000), not inflated.
    """
    result = await repository.get_backed_breakdown(test_ids["v2_vault_id"])

    assert result.backed_asset_id == test_ids["v2_vault_id"]
    assert len(result.items) == 3

    by_symbol = {item.symbol: item for item in result.items}
    assert set(by_symbol) == {"USDC", "WETH", "WBTC"}

    assert by_symbol["USDC"].backing_value == Decimal("510051.00")
    assert by_symbol["USDC"].backing_pct == Decimal("51.00")
    assert by_symbol["USDC"].token_id == test_ids["usdc_id"]
    assert by_symbol["USDC"].price_usd == _USDC_PRICE_USD

    assert by_symbol["WETH"].backing_value == Decimal("390039.00")
    assert by_symbol["WETH"].backing_pct == Decimal("39.00")
    assert by_symbol["WETH"].token_id == test_ids["weth_id"]
    assert by_symbol["WETH"].price_usd == _WETH_PRICE_USD

    assert by_symbol["WBTC"].backing_value == Decimal("100010.00")
    assert by_symbol["WBTC"].backing_pct == Decimal("10.00")
    assert by_symbol["WBTC"].token_id == test_ids["wbtc_id"]
    assert by_symbol["WBTC"].price_usd == _WBTC_PRICE_USD


@pytest.mark.asyncio(loop_scope="module")
async def test_v2_items_ordered_by_backed_amount_desc(
    repository: BackedBreakdownRepository, test_ids: dict[str, int]
) -> None:
    result = await repository.get_backed_breakdown(test_ids["v2_vault_id"])
    amounts = [item.backing_value for item in result.items]
    assert amounts == sorted(amounts, reverse=True)


@pytest.mark.asyncio(loop_scope="module")
async def test_v2_percentages_sum_to_100(repository: BackedBreakdownRepository, test_ids: dict[str, int]) -> None:
    result = await repository.get_backed_breakdown(test_ids["v2_vault_id"])
    assert sum(item.backing_pct for item in result.items) == Decimal("100.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_v2_unknown_adapter_is_surfaced(repository: BackedBreakdownRepository, test_ids: dict[str, int]) -> None:
    """A type-99 adapter contributes its real_assets as the vault asset (never dropped).

    Raw USDC = 250,000 (unknown adapter) + 150,000 (vault idle) = 400,000; USD-scaled by
    the USDC price 1.0001 = 400,040.00. If the unknown adapter were silently dropped, only
    the 150,000 idle (150,015.00 USD) would remain.
    """
    result = await repository.get_backed_breakdown(test_ids["v2b_vault_id"])

    assert len(result.items) == 1
    item = result.items[0]
    assert item.symbol == "USDC"
    assert item.token_id == test_ids["usdc_id"]
    assert item.backing_value == Decimal("400040.00")
    assert item.backing_pct == Decimal("100.00")
    assert item.price_usd == _USDC_PRICE_USD


@pytest.mark.asyncio(loop_scope="module")
async def test_v2_nonexistent_vault_returns_empty(repository: BackedBreakdownRepository) -> None:
    result = await repository.get_backed_breakdown(999_999)
    assert result.items == ()
