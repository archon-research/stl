from collections.abc import AsyncIterator
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.risk_position_resolver import PostgresRiskPositionResolver
from app.domain.entities.allocation import EthAddress
from app.domain.entities.risk import RiskPositionNotFoundError
from tests.integration.conftest import insert_token, store_test_ids

_SPARK_PROXY = bytes.fromhex("1234567890abcdef1234567890abcdef12345678")
_OBEX_PROXY = bytes.fromhex("fedcba9876543210fedcba9876543210fedcba98")
_GROVE_PROXY = bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
_SPUSDC_ADDRESS = bytes.fromhex("98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c")
_SPDAI_ADDRESS = bytes.fromhex("59cd1c87501baa753d0b5b5ab5d8416a45cd71db")
_TX1 = b"\xaa" * 32
_TX2 = b"\xbb" * 32
_TX3 = b"\xcc" * 32


async def _insert_receipt_token(
    conn: asyncpg.Connection,
    *,
    protocol_id: int,
    underlying_token_id: int,
    chain_id: int,
    address: bytes,
    symbol: str,
) -> int:
    return cast(
        int,
        await conn.fetchval(
            """
            INSERT INTO receipt_token
                (protocol_id, underlying_token_id, receipt_token_address, symbol, chain_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                DO UPDATE SET symbol = EXCLUDED.symbol
            RETURNING id
            """,
            protocol_id,
            underlying_token_id,
            address,
            symbol,
            chain_id,
        ),
    )


async def _insert_allocation_position(
    conn: asyncpg.Connection,
    *,
    token_id: int,
    prime_id: int,
    proxy_address: bytes,
    balance: str,
    block_number: int,
    tx_hash: bytes,
) -> None:
    await conn.execute(
        """
        INSERT INTO allocation_position
            (chain_id, token_id, prime_id, proxy_address, balance,
             block_number, block_version, tx_hash, log_index, tx_amount, direction)
        VALUES (1, $1, $2, $3, $4, $5, 0, $6, 0, $4, 'in')
        """,
        token_id,
        prime_id,
        proxy_address,
        balance,
        block_number,
        tx_hash,
    )


async def _insert_price(
    conn: asyncpg.Connection,
    *,
    token_id: int,
    oracle_id: int,
    block_number: int,
    price_usd: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO onchain_token_price
            (token_id, oracle_id, block_number, block_version, timestamp, price_usd)
        VALUES ($1, $2, $3, 0, NOW(), $4::numeric(30,18))
        """,
        token_id,
        oracle_id,
        block_number,
        price_usd,
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        sparklend_id = cast(
            int,
            await conn.fetchval("SELECT id FROM protocol WHERE name = 'SparkLend' AND chain_id = 1"),
        )
        spark_prime_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'spark'"))
        obex_prime_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'obex'"))
        grove_prime_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'grove'"))
        oracle_id = cast(int, await conn.fetchval("SELECT id FROM oracle WHERE name = 'sparklend' AND chain_id = 1"))
        usdc_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'USDC' AND chain_id = 1"))
        dai_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'DAI' AND chain_id = 1"))

        spusdc_token_id = await insert_token(conn, "spUSDC", 6, _SPUSDC_ADDRESS)
        spdai_token_id = await insert_token(conn, "spDAI", 18, _SPDAI_ADDRESS)

        spusdc_receipt_token_id = await _insert_receipt_token(
            conn,
            protocol_id=sparklend_id,
            underlying_token_id=usdc_id,
            chain_id=1,
            address=_SPUSDC_ADDRESS,
            symbol="spUSDC",
        )
        spdai_receipt_token_id = await _insert_receipt_token(
            conn,
            protocol_id=sparklend_id,
            underlying_token_id=dai_id,
            chain_id=1,
            address=_SPDAI_ADDRESS,
            symbol="spDAI",
        )

        await _insert_allocation_position(
            conn,
            token_id=spusdc_token_id,
            prime_id=spark_prime_id,
            proxy_address=_SPARK_PROXY,
            balance="250",
            block_number=2000,
            tx_hash=_TX1,
        )
        await _insert_allocation_position(
            conn,
            token_id=usdc_id,
            prime_id=obex_prime_id,
            proxy_address=_OBEX_PROXY,
            balance="125",
            block_number=2001,
            tx_hash=_TX2,
        )
        await _insert_allocation_position(
            conn,
            token_id=spdai_token_id,
            prime_id=grove_prime_id,
            proxy_address=_GROVE_PROXY,
            balance="75",
            block_number=2002,
            tx_hash=_TX3,
        )

        await _insert_price(conn, token_id=usdc_id, oracle_id=oracle_id, block_number=2000, price_usd="1.02")

        await store_test_ids(
            conn,
            {
                "spusdc_receipt_token_id": spusdc_receipt_token_id,
                "spdai_receipt_token_id": spdai_receipt_token_id,
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
async def resolver(async_db_url: str, _seed_data: None) -> AsyncIterator[PostgresRiskPositionResolver]:
    engine = create_async_engine(async_db_url)
    try:
        yield PostgresRiskPositionResolver(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_resolves_receipt_token_position_and_derives_usd_exposure(
    resolver: PostgresRiskPositionResolver,
    test_ids: dict[str, int],
) -> None:
    result = await resolver.resolve(test_ids["spusdc_receipt_token_id"], EthAddress("0x" + _SPARK_PROXY.hex()))

    assert result.chain_id == 1
    assert result.symbol == "spUSDC"
    assert result.underlying_symbol == "USDC"
    assert result.balance == Decimal("250")
    assert result.usd_exposure == Decimal("255.00")


@pytest.mark.asyncio(loop_scope="module")
async def test_resolves_underlying_mapped_position_and_derives_usd_exposure(
    resolver: PostgresRiskPositionResolver,
    test_ids: dict[str, int],
) -> None:
    result = await resolver.resolve(test_ids["spusdc_receipt_token_id"], EthAddress("0x" + _OBEX_PROXY.hex()))

    assert result.symbol == "spUSDC"
    assert result.underlying_symbol == "USDC"
    assert result.balance == Decimal("125")
    assert result.usd_exposure == Decimal("127.50")


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_position_with_missing_usd_exposure_when_price_is_unavailable(
    resolver: PostgresRiskPositionResolver,
    test_ids: dict[str, int],
) -> None:
    result = await resolver.resolve(test_ids["spdai_receipt_token_id"], EthAddress("0x" + _GROVE_PROXY.hex()))

    assert result.symbol == "spDAI"
    assert result.underlying_symbol == "DAI"
    assert result.balance == Decimal("75")
    assert result.usd_exposure is None


@pytest.mark.asyncio(loop_scope="module")
async def test_raises_not_found_for_missing_position(
    resolver: PostgresRiskPositionResolver,
    test_ids: dict[str, int],
) -> None:
    with pytest.raises(RiskPositionNotFoundError):
        await resolver.resolve(test_ids["spusdc_receipt_token_id"], EthAddress("0x" + "9" * 40))
