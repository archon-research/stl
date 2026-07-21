import datetime
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.morpho_liquidation_params_repository_v2 import MorphoV2LiquidationParamsRepository
from app.risk_engine.crypto_lending.lif import compute_lif
from tests.integration.seed import insert_token, insert_user, store_test_ids

# lltv is stored WAD (18-decimal) like the Go indexer; the repo divides by 1e18.
_LLTV_086 = Decimal("860000000000000000")
_LLTV_080 = Decimal("800000000000000000")
_LLTV_077 = Decimal("770000000000000000")
_LLTV_070 = Decimal("700000000000000000")
_LLTV_050 = Decimal("500000000000000000")

_BLOCK = 20_000_000

_ADAPTER_TYPE_BLUE = 1
_ADAPTER_TYPE_V1 = 2

_USDC = b"\xa0\xb8\x69\x91\xc6\x21\x8b\x36\xc1\xd1\x9d\x4a\x2e\x9e\xb0\xce\x36\x06\xeb\x48"
_WETH = b"\xc0\x2a\xaa\x39\xb2\x23\xfe\x8d\x0a\x0e\x5c\x4f\x27\xea\xd9\x08\x3c\x75\x6c\xc2"
_WBTC = b"\x22\x60\xfa\xc5\xe5\x54\x2a\x77\x3a\xa4\x4f\xbc\xfe\xdf\x7c\x19\x3b\xc2\xc5\x99"
_WSTETH = b"\x70\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07"
_XCOLL = b"\x50\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05"

_V2_VAULT = b"\xcc" * 20
_V1_VAULT = b"\xdd" * 20
_ADAPTER1 = b"\xa1" * 20  # type 1
_ADAPTER2 = b"\xa2" * 20  # type 2 (nested V1)
_ADAPTER3 = b"\xa3" * 20  # type 1 but REMOVED
_EMPTY_V2_VAULT = b"\xc9" * 20  # v3 vault with no adapters


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
            b"\xbb\xbb\xbb\xbb\xbb\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        ),
    )


async def _insert_vault(
    conn: asyncpg.Connection, protocol_id: int, address: bytes, asset_token_id: int, version: int
) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO morpho_vault
            (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
        VALUES (1, $1, $2, 'V', 'V', $3, $4, $5)
        RETURNING id
        """,
            protocol_id,
            address,
            asset_token_id,
            version,
            _BLOCK,
        ),
    )


async def _insert_market(
    conn: asyncpg.Connection, protocol_id: int, index: int, loan_token_id: int, collateral_token_id: int, lltv: Decimal
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
            bytes([index]) + b"\x00" * 31,
            loan_token_id,
            collateral_token_id,
            b"\x00" * 20,
            b"\x00" * 20,
            lltv,
            _BLOCK,
        ),
    )


async def _insert_market_position(conn: asyncpg.Connection, user_id: int, market_id: int) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_market_position
            (user_id, morpho_market_id, block_number, block_version, timestamp,
             supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
        VALUES ($1, $2, $3, 0, $4, 100, 0, 0, 1000, 0)
        """,
        user_id,
        market_id,
        _BLOCK,
        datetime.datetime.now(tz=datetime.timezone.utc),
    )


async def _insert_vault_position(conn: asyncpg.Connection, user_id: int, vault_id: int) -> None:
    await conn.execute(
        """
        INSERT INTO morpho_vault_position
            (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets)
        VALUES ($1, $2, $3, 0, $4, 1000, 1000)
        """,
        user_id,
        vault_id,
        _BLOCK,
        datetime.datetime.now(tz=datetime.timezone.utc),
    )


async def _insert_adapter(
    conn: asyncpg.Connection,
    vault_id: int,
    address: bytes,
    asset_token_id: int,
    adapter_type: int,
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
            _BLOCK,
            removed_at_block,
        ),
    )


# ---------------------------------------------------------------------------
# Scenario (all lltv WAD): the V2 vault's liquidatable collateral is reachable
# only through its ACTIVE adapters.
#   adapter1 (type 1): markets WETH@0.86, WETH@0.80, WBTC@0.77 → MIN(WETH)=0.80
#   adapter2 (type 2): nested V1 vault → market WSTETH@0.70
#   adapter3 (REMOVED, type 1): market XCOLL@0.50 → must be EXCLUDED
# A second v3 vault has no adapters at all → has_active_adapters() is False.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await _insert_protocol(conn)

        usdc = await insert_token(conn, "USDC", 6, _USDC)
        weth = await insert_token(conn, "WETH", 18, _WETH)
        wbtc = await insert_token(conn, "WBTC", 8, _WBTC)
        wsteth = await insert_token(conn, "wstETH", 18, _WSTETH)
        xcoll = await insert_token(conn, "XCOLL", 18, _XCOLL)

        adapter1_user = await insert_user(conn, _ADAPTER1)
        adapter2_user = await insert_user(conn, _ADAPTER2)
        adapter3_user = await insert_user(conn, _ADAPTER3)
        v1_vault_user = await insert_user(conn, _V1_VAULT)

        v2_vault = await _insert_vault(conn, protocol_id, _V2_VAULT, usdc, 3)
        v1_vault = await _insert_vault(conn, protocol_id, _V1_VAULT, usdc, 1)
        empty_v2_vault = await _insert_vault(conn, protocol_id, _EMPTY_V2_VAULT, usdc, 3)

        # Adapters of the V2 vault.
        await _insert_adapter(conn, v2_vault, _ADAPTER1, usdc, _ADAPTER_TYPE_BLUE)
        await _insert_adapter(conn, v2_vault, _ADAPTER2, usdc, _ADAPTER_TYPE_V1)
        await _insert_adapter(conn, v2_vault, _ADAPTER3, usdc, _ADAPTER_TYPE_BLUE, removed_at_block=_BLOCK - 1)

        # type 1: adapter1 supplies three Blue markets (WETH twice for MIN, WBTC once).
        market_weth_a = await _insert_market(conn, protocol_id, 1, usdc, weth, _LLTV_086)
        market_weth_b = await _insert_market(conn, protocol_id, 2, usdc, weth, _LLTV_080)
        market_wbtc = await _insert_market(conn, protocol_id, 3, usdc, wbtc, _LLTV_077)
        await _insert_market_position(conn, adapter1_user, market_weth_a)
        await _insert_market_position(conn, adapter1_user, market_weth_b)
        await _insert_market_position(conn, adapter1_user, market_wbtc)

        # type 2: adapter2 holds the nested V1 vault, whose own user supplies WSTETH@0.70.
        market_wsteth = await _insert_market(conn, protocol_id, 4, usdc, wsteth, _LLTV_070)
        await _insert_vault_position(conn, adapter2_user, v1_vault)
        await _insert_market_position(conn, v1_vault_user, market_wsteth)

        # removed adapter3 supplies XCOLL@0.50 — must never surface.
        market_xcoll = await _insert_market(conn, protocol_id, 5, usdc, xcoll, _LLTV_050)
        await _insert_market_position(conn, adapter3_user, market_xcoll)

        await store_test_ids(
            conn,
            {
                "v2_vault": v2_vault,
                "empty_v2_vault": empty_v2_vault,
                "weth": weth,
                "wbtc": wbtc,
                "wsteth": wsteth,
                "xcoll": xcoll,
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
async def repository(async_db_url: str, _seed_data: None, test_ids: dict[str, int]):
    engine = create_async_engine(async_db_url)
    try:
        yield MorphoV2LiquidationParamsRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_resolves_lltv_across_adapter_graph(repository, test_ids: dict[str, int]) -> None:
    """type-1 (direct) + type-2 (nested V1) markets resolve; MIN(lltv) per collateral;
    the removed adapter's XCOLL market is excluded."""
    result = await repository.get_params(
        backed_asset_id=test_ids["v2_vault"],
        token_ids=[test_ids["weth"], test_ids["wbtc"], test_ids["wsteth"], test_ids["xcoll"]],
    )

    assert set(result) == {test_ids["weth"], test_ids["wbtc"], test_ids["wsteth"]}

    weth = result[test_ids["weth"]]
    assert weth.liquidation_threshold == Decimal("0.80")  # MIN(0.86, 0.80)
    assert abs(weth.liquidation_bonus - compute_lif(Decimal("0.80"))) < Decimal("0.0001")

    assert result[test_ids["wbtc"]].liquidation_threshold == Decimal("0.77")
    assert result[test_ids["wsteth"]].liquidation_threshold == Decimal("0.70")

    # The removed adapter's collateral must be absent even when requested.
    assert test_ids["xcoll"] not in result


@pytest.mark.asyncio(loop_scope="module")
async def test_has_active_adapters_true_when_adapters_present(repository, test_ids: dict[str, int]) -> None:
    assert await repository.has_active_adapters(test_ids["v2_vault"]) is True


@pytest.mark.asyncio(loop_scope="module")
async def test_has_active_adapters_false_when_none(repository, test_ids: dict[str, int]) -> None:
    assert await repository.has_active_adapters(test_ids["empty_v2_vault"]) is False


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_adapter_vault_returns_no_params(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(
        backed_asset_id=test_ids["empty_v2_vault"],
        token_ids=[test_ids["weth"], test_ids["wbtc"]],
    )
    assert result == {}


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_token_ids_returns_empty(repository, test_ids: dict[str, int]) -> None:
    assert await repository.get_params(backed_asset_id=test_ids["v2_vault"], token_ids=[]) == {}
