"""The reads that resolve a prime through ``prime_proxy``.

``prime_proxy`` is static reference data: the migration transcribes the declared
proxy universe from the axis-synome contract, and nothing writes to it at runtime.
So these tests seed no rows of their own for the declared addresses — the migration
already did — and what they cover is that the reads answer from that list, and that
an address absent from it resolves to nothing.
"""

from collections.abc import AsyncIterator
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.domain.entities.allocation import EthAddress

# Declared in the migration. The ALM/SubProxy split matters: the reads classify by
# address, so a made-up one would be treated as ALM and the SubProxy exclusions
# would go untested.
_SPARK_MAINNET_ALM = "1601843c5e9bc251a3272907010afa41fa18347e"
_SPARK_BASE_ALM = "2917956eff0b5eaf030abdb4ef4296df775009ca"
_SPARK_SUB_PROXY = "3300f198988e4c9c63f75df86de36421f06af8c4"
_UNDECLARED = "ab" * 20


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str) -> AsyncIterator[AllocationRepository]:
    """The repository whose reads resolve a proxy to its prime."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_the_migration_declares_the_full_proxy_list(conn: asyncpg.Connection) -> None:
    """Twelve rows, and every one resolves to a real prime.

    A prime name in the migration that does not match ``prime.name`` would drop its
    proxies silently, and every endpoint for them would return empty rather than fail.
    """
    rows = await conn.fetch(
        "SELECT p.name, pp.chain_id, encode(pp.proxy_address, 'hex') AS address "
        "FROM prime_proxy pp JOIN prime p ON p.id = pp.prime_id"
    )

    assert len(rows) == 12
    assert {row["name"] for row in rows} == {"spark", "grove"}


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("declared", [True, False])
async def test_prime_exists_answers_from_the_declared_list(repository: AllocationRepository, declared: bool) -> None:
    """A declared proxy exists whether or not it has positions; anything else does not."""
    address = EthAddress("0x" + (_SPARK_MAINNET_ALM if declared else _UNDECLARED))

    assert await repository.prime_exists(address) is declared


@pytest.mark.asyncio(loop_scope="module")
async def test_prime_exists_holds_for_a_declared_proxy_with_no_positions(
    conn: asyncpg.Connection, repository: AllocationRepository
) -> None:
    """The semantics this table chose: declared is enough, data need not have arrived.

    spark's arbitrum ALM proxy is declared and has no allocation_position rows in
    this database, so it exercises the case that previously 404'd.
    """
    arbitrum_alm = "92afd6f2385a90e44da3a8b60fe36f6cbe1d8709"
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM allocation_position WHERE proxy_address = $1",
            bytes.fromhex(arbitrum_alm),
        )
        == 0
    )

    assert await repository.prime_exists(EthAddress("0x" + arbitrum_alm)) is True


@pytest.mark.asyncio(loop_scope="module")
async def test_proxy_list_widens_to_every_declared_alm_proxy(repository: AllocationRepository) -> None:
    """Asking with one proxy returns the prime's declared ALM set, minus SubProxies."""
    proxies = {
        str(proxy).removeprefix("0x")
        for proxy in await repository.list_prime_proxy_addresses(EthAddress("0x" + _SPARK_MAINNET_ALM))
    }

    assert _SPARK_MAINNET_ALM in proxies
    assert _SPARK_BASE_ALM in proxies
    assert _SPARK_SUB_PROXY not in proxies


@pytest.mark.asyncio(loop_scope="module")
async def test_an_undeclared_address_widens_only_to_itself(repository: AllocationRepository) -> None:
    """Never empty — downstream an empty filter is indistinguishable from no filter."""
    proxies = await repository.list_prime_proxy_addresses(EthAddress("0x" + _UNDECLARED))

    assert [str(proxy) for proxy in proxies] == ["0x" + _UNDECLARED]


@pytest.mark.asyncio(loop_scope="module")
async def test_primary_proxy_prefers_mainnet(repository: AllocationRepository) -> None:
    """The prime-scoped rows attach to the mainnet ALM proxy when the prime has one."""
    assert await repository.primary_proxy_address(EthAddress("0x" + _SPARK_MAINNET_ALM)) == "0x" + _SPARK_MAINNET_ALM


@pytest.mark.asyncio(loop_scope="module")
async def test_list_primes_reports_every_declared_alm_proxy(repository: AllocationRepository) -> None:
    """/v1/primes lists the declared ALM proxies, one row per (proxy, chain)."""
    primes = await repository.list_primes()

    spark = {prime.address.removeprefix("0x"): prime for prime in primes if prime.name == "spark"}
    assert _SPARK_MAINNET_ALM in spark
    assert _SPARK_BASE_ALM in spark
    assert _SPARK_SUB_PROXY not in spark
    assert spark[_SPARK_MAINNET_ALM].chain_id == 1
    assert spark[_SPARK_BASE_ALM].chain_id == 8453
    assert all(prime.prime_vault_address is not None for prime in spark.values())


@pytest.mark.asyncio(loop_scope="module")
async def test_an_address_cannot_be_declared_on_two_chains(conn: asyncpg.Connection) -> None:
    """Every read resolves by address alone, so the address has to be unique on its own.

    Without this the primary key would still permit one address on two chains, and
    ``WHERE proxy_address = ... LIMIT 1`` could return either prime — serving one
    prime's capital or custody data under another's address.
    """
    spark_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'spark'"))

    with pytest.raises(asyncpg.UniqueViolationError):
        await conn.execute(
            "INSERT INTO prime_proxy (chain_id, proxy_address, prime_id) VALUES (10, $1, $2)",
            bytes.fromhex(_SPARK_MAINNET_ALM),
            spark_id,
        )
