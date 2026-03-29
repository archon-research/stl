# tests/integration/test_morpho_liquidation_params_repository.py
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.morpho_liquidation_params_repository import (
    MorphoLiquidationParamsRepository,
)
from app.risk_engine.crypto_lending.lif import compute_lif
from tests.integration.conftest import insert_user, store_test_ids


async def _insert_morpho_vault(conn: asyncpg.Connection, protocol_id: int, chain_id: int, asset_token_id: int) -> int:
    address = b"\xde\xad\xbe\xef" + b"\x00" * 16
    await insert_user(conn, address)
    vault_id = cast(
        int,
        await conn.fetchval(
            """
            INSERT INTO morpho_vault
                (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
            VALUES ($1, $2, $3, 'Test Vault', 'TV', $4, 1, 1000)
            ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            chain_id,
            protocol_id,
            address,
            asset_token_id,
        ),
    )
    return vault_id


async def _insert_morpho_market(
    conn: asyncpg.Connection,
    protocol_id: int,
    chain_id: int,
    loan_token_id: int,
    collateral_token_id: int,
    lltv: Decimal,
    market_index: int,
) -> int:
    market_id = bytes([market_index]) + b"\x00" * 31
    return cast(
        int,
        await conn.fetchval(
            """
            INSERT INTO morpho_market
                (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
                 oracle_address, irm_address, lltv, created_at_block)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1000)
            ON CONFLICT (chain_id, market_id) DO UPDATE SET lltv = EXCLUDED.lltv
            RETURNING id
            """,
            chain_id,
            protocol_id,
            market_id,
            loan_token_id,
            collateral_token_id,
            b"\x00" * 20,
            b"\x00" * 20,
            lltv,
        ),
    )


async def _insert_market_position(conn: asyncpg.Connection, user_id: int, market_id: int) -> None:
    import datetime

    await conn.execute(
        """
        INSERT INTO morpho_market_position
            (user_id, morpho_market_id, block_number, block_version, timestamp,
             supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
        VALUES ($1, $2, 1000, 0, $3, 100, 0, 0, 1000, 0)
        ON CONFLICT DO NOTHING
        """,
        user_id,
        market_id,
        datetime.datetime.now(tz=datetime.timezone.utc),
    )


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
            b"\xbb\xbb\xbb\xbb\xbb\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02",
        ),
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await _insert_protocol(conn)

        usdc_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'USDC' AND chain_id = 1"))
        weth_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
        cbbtc_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'cbBTC' AND chain_id = 1"))

        vault_id = await _insert_morpho_vault(conn, protocol_id, chain_id=1, asset_token_id=usdc_id)

        vault_address = b"\xde\xad\xbe\xef" + b"\x00" * 16
        user_id = cast(
            int, await conn.fetchval('SELECT id FROM "user" WHERE address = $1 AND chain_id = 1', vault_address)
        )

        weth_market_id = await _insert_morpho_market(
            conn,
            protocol_id,
            chain_id=1,
            loan_token_id=usdc_id,
            collateral_token_id=weth_id,
            lltv=Decimal("0.86"),
            market_index=1,
        )
        cbbtc_market_id = await _insert_morpho_market(
            conn,
            protocol_id,
            chain_id=1,
            loan_token_id=usdc_id,
            collateral_token_id=cbbtc_id,
            lltv=Decimal("0.77"),
            market_index=2,
        )

        await _insert_market_position(conn, user_id, weth_market_id)
        await _insert_market_position(conn, user_id, cbbtc_market_id)

        await store_test_ids(
            conn,
            {
                "protocol_id": protocol_id,
                "vault_id": vault_id,
                "weth_id": weth_id,
                "cbbtc_id": cbbtc_id,
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
        yield MorphoLiquidationParamsRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_lltv_as_threshold_and_lif_as_bonus(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(
        backed_asset_id=test_ids["vault_id"],
        token_ids=[test_ids["weth_id"], test_ids["cbbtc_id"]],
    )

    assert test_ids["weth_id"] in result
    weth = result[test_ids["weth_id"]]
    assert weth.liquidation_threshold == Decimal("0.86")
    expected_lif = compute_lif(Decimal("0.86"))
    assert abs(weth.liquidation_bonus - expected_lif) < Decimal("0.0001")

    assert test_ids["cbbtc_id"] in result
    cbbtc = result[test_ids["cbbtc_id"]]
    assert cbbtc.liquidation_threshold == Decimal("0.77")
    expected_cbbtc_lif = compute_lif(Decimal("0.77"))
    assert abs(cbbtc.liquidation_bonus - expected_cbbtc_lif) < Decimal("0.0001")


@pytest.mark.asyncio(loop_scope="module")
async def test_token_not_in_vault_markets_is_absent(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(
        backed_asset_id=test_ids["vault_id"],
        token_ids=[99999],
    )
    assert result == {}


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_token_ids_returns_empty_dict(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(backed_asset_id=test_ids["vault_id"], token_ids=[])
    assert result == {}
