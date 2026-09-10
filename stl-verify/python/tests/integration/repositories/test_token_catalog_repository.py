"""Integration tests for ``TokenCatalogRepository`` chain+address lookups.

Exercises the SQL path that backs the
``/v1/tokens/{chain_id}/{token_address}`` endpoint. Reuses the shared seed
data (WETH on chain 1) provided by the integration conftest.
"""

import datetime as dt
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.reference_as_of import utc_now
from app.adapters.postgres.token_catalog_repository import TokenCatalogRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import insert_oracle_asset, insert_token


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield TokenCatalogRepository(engine, utc_now)
    finally:
        await engine.dispose()


_WETH_ADDRESS = EthAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")


@pytest.mark.asyncio(loop_scope="module")
async def test_get_token_by_chain_and_address_returns_known_token(repository) -> None:
    token = await repository.get_token_by_chain_and_address(1, _WETH_ADDRESS)

    assert token is not None
    assert token.chain_id == 1
    assert token.symbol == "WETH"


@pytest.mark.asyncio(loop_scope="module")
async def test_get_token_by_chain_and_address_is_case_insensitive(repository) -> None:
    """Address bytes are compared by value, not hex case."""
    lower = EthAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
    token = await repository.get_token_by_chain_and_address(1, lower)
    assert token is not None
    assert token.symbol == "WETH"


@pytest.mark.asyncio(loop_scope="module")
async def test_get_token_by_chain_and_address_returns_none_when_missing(repository) -> None:
    missing = EthAddress("0x" + "ff" * 20)
    result = await repository.get_token_by_chain_and_address(1, missing)
    assert result is None


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_price_tie_resolves_to_highest_oracle_id(repository, db_url) -> None:
    """Onchain rows tied on all four ordering keys resolve to the higher oracle_id.

    Same-block rows from two oracles share the block timestamp, so the
    frozen-source replant shape (a retired oracle re-emitting on a republished
    block next to a live one) ties (timestamp, block_number, block_version,
    processing_version); oracle_id DESC keeps the quote stable across calls.
    The stale lower-oracle_id row is inserted first to bias an un-tiebroken
    top-1 sort toward the wrong answer.
    """
    conn = await asyncpg.connect(db_url)
    try:
        token_id = await insert_token(conn, "tieCat", 6, b"\x7a" * 20)
        stale_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_tie_stale', 'Catalog tie stale oracle', 1, $1) RETURNING id",
            b"\x7b" * 20,
        )
        fresh_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_tie_fresh', 'Catalog tie fresh oracle', 1, $1) RETURNING id",
            b"\x7c" * 20,
        )
        assert stale_id < fresh_id, "seed premise broken: stale oracle must have the lower id"
        tied_at = dt.datetime(2026, 3, 1, tzinfo=dt.UTC)
        for oracle_id, price in ((stale_id, Decimal("1.00")), (fresh_id, Decimal("1.25"))):
            await conn.execute(
                "INSERT INTO onchain_token_price "
                "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
                "VALUES ($1, $2, 5000, 1, $3, $4)",
                token_id,
                oracle_id,
                tied_at,
                price,
            )
            # Both sources enabled: the tie is genuine (oracle_id breaks it),
            # not resolved by the disabled-mapping filter.
            await insert_oracle_asset(conn, oracle_id, token_id)
    finally:
        await conn.close()

    quote = await repository.get_latest_price(token_id)
    assert quote is not None
    assert quote.source_name == "cat_tie_fresh"
    assert quote.price_usd == Decimal("1.25")


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_price_excludes_disabled_higher_block_source(repository, db_url) -> None:
    """A retired oracle (disabled oracle_asset mapping) is excluded even when it holds the newer row.

    The disabled source carries the higher block, later timestamp and higher
    oracle_id, so every ordering signal favours it; only excluding its disabled
    mapping keeps get_latest_price on the live lower-block source.
    """
    conn = await asyncpg.connect(db_url)
    try:
        token_id = await insert_token(conn, "disCat", 6, b"\x7d" * 20)
        enabled_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_dis_enabled', 'Catalog disabled-source live oracle', 1, $1) RETURNING id",
            b"\x7e" * 20,
        )
        disabled_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_dis_disabled', 'Catalog disabled-source retired oracle', 1, $1) RETURNING id",
            b"\x7f" * 20,
        )
        assert enabled_id < disabled_id, "seed premise broken: disabled oracle must have the higher id"
        base_ts = dt.datetime(2026, 3, 2, tzinfo=dt.UTC)
        for oracle_id, block, offset, price, enabled in (
            (enabled_id, 5000, dt.timedelta(0), Decimal("1.25"), True),
            (disabled_id, 5001, dt.timedelta(minutes=1), Decimal("1.00"), False),
        ):
            await conn.execute(
                "INSERT INTO onchain_token_price "
                "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
                "VALUES ($1, $2, $3, 0, $4, $5)",
                token_id,
                oracle_id,
                block,
                base_ts + offset,
                price,
            )
            await insert_oracle_asset(conn, oracle_id, token_id, enabled=enabled)
    finally:
        await conn.close()

    quote = await repository.get_latest_price(token_id)
    assert quote is not None
    assert quote.source_name == "cat_dis_enabled"
    assert quote.price_usd == Decimal("1.25")


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_price_serves_the_newer_of_onchain_and_offchain(repository, db_url) -> None:
    """The on-chain and off-chain caches compete on observation time; the newer one serves the quote.

    Both halves of the read moved from the price histories to their *_current
    caches (VEC-672); the cross-source choice and the reported timestamp must be
    unchanged by that.
    """
    conn = await asyncpg.connect(db_url)
    try:
        token_id = await insert_token(conn, "mixCat", 6, b"\x8a" * 20)
        oracle_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_mix_oracle', 'Catalog mixed-source oracle', 1, $1) RETURNING id",
            b"\x8b" * 20,
        )
        source_id = await conn.fetchval("SELECT id FROM offchain_price_source WHERE name = 'coingecko'")
        onchain_at = dt.datetime(2026, 3, 3, 0, 0, tzinfo=dt.UTC)
        offchain_at = onchain_at + dt.timedelta(hours=1)
        await conn.execute(
            "INSERT INTO onchain_token_price "
            "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
            "VALUES ($1, $2, 6000, 0, $3, 2.00)",
            token_id,
            oracle_id,
            onchain_at,
        )
        await insert_oracle_asset(conn, oracle_id, token_id)
        await conn.execute(
            'INSERT INTO offchain_token_price (token_id, source_id, "timestamp", price_usd) VALUES ($1, $2, $3, 2.10)',
            token_id,
            source_id,
            offchain_at,
        )

        quote = await repository.get_latest_price(token_id)
        assert quote is not None
        assert (quote.source_name, quote.price_usd, quote.timestamp) == ("coingecko", Decimal("2.10"), offchain_at)

        newer_onchain_at = offchain_at + dt.timedelta(hours=1)
        await conn.execute(
            "INSERT INTO onchain_token_price "
            "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
            "VALUES ($1, $2, 6001, 0, $3, 2.20)",
            token_id,
            oracle_id,
            newer_onchain_at,
        )
    finally:
        await conn.close()

    quote = await repository.get_latest_price(token_id)
    assert quote is not None
    assert (quote.source_name, quote.price_usd) == ("cat_mix_oracle", Decimal("2.20"))
    assert quote.timestamp == newer_onchain_at
