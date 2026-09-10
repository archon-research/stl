"""Multi-form prime resolution against a real database.

The risk is in the SQL and in the migration-seeded topology it reads: which forms match,
and that they all reach one prime. Mocking the engine exercises neither.
"""

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.prime_resolver_repository import PrimeResolverRepository

_SPARK_KEY = "prm_2d3ceee8415e59f3"
_SPARK_VAULT = "0x691a6c29e9e96dd897718305427ad5d534db16ba"
_SPARK_MAINNET_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_SPARK_MAINNET_SUBPROXY = "0x3300f198988e4c9c63f75df86de36421f06af8c4"
_SPARK_AVALANCHE_ALM = "0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec"
# The form a human pastes from a block explorer.
_SPARK_VAULT_CHECKSUMMED = "0x691A6c29E9e96dd897718305427Ad5D534db16BA"

# obex has a vault and no proxies at all — the form that must still resolve.
_OBEX_VAULT = "0xf275110dfe7b80df66a762f968f59b70babe2b29"
_OBEX_KEY = "prm_11db7b73f1333a63"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def resolver(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield PrimeResolverRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    "identifier",
    [
        "spark",
        _SPARK_VAULT,
        _SPARK_VAULT_CHECKSUMMED,
        _SPARK_MAINNET_ALM,
        _SPARK_MAINNET_SUBPROXY,
        _SPARK_AVALANCHE_ALM,
    ],
)
async def test_every_accepted_form_resolves_to_the_same_prime(resolver, identifier: str):
    prime = await resolver.resolve(identifier)

    assert prime is not None
    assert (prime.name, prime.prime_key, prime.vault_address) == ("spark", _SPARK_KEY, _SPARK_VAULT)


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("identifier", ["obex", _OBEX_VAULT])
async def test_a_prime_with_no_proxies_still_resolves(resolver, identifier: str):
    prime = await resolver.resolve(identifier)

    assert prime is not None
    assert (prime.name, prime.prime_key) == ("obex", _OBEX_KEY)


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("identifier", ["nosuchprime", "0x" + "ab" * 20])
async def test_an_unknown_identifier_resolves_to_none(resolver, identifier: str):
    assert await resolver.resolve(identifier) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_an_address_that_is_one_primes_vault_and_anothers_proxy_resolves_to_the_vault(resolver, db_url: str):
    address = bytes.fromhex("7b" * 20)
    conn = await asyncpg.connect(db_url)
    try:
        vault_owner = await conn.fetchval(
            "INSERT INTO prime (prime_key, name, vault_address) "
            "VALUES ('prm_t_resolver_vault_match', 'resolver_vault_match', $1) RETURNING id",
            address,
        )
        proxy_owner = await conn.fetchval(
            "INSERT INTO prime (prime_key, name, vault_address) "
            "VALUES ('prm_t_resolver_proxy_match', 'resolver_proxy_match', $1) RETURNING id",
            bytes.fromhex("7d" * 20),
        )
        await conn.execute(
            "INSERT INTO prime_proxy (chain_id, proxy_address, prime_id) VALUES (1, $1, $2)", address, proxy_owner
        )

        prime = await resolver.resolve("0x" + address.hex())

        assert prime is not None
        assert prime.id == vault_owner
    finally:
        await conn.execute(
            "DELETE FROM prime_proxy WHERE prime_id IN"
            " (SELECT id FROM prime WHERE name IN ('resolver_vault_match', 'resolver_proxy_match'))"
        )
        await conn.execute("DELETE FROM prime WHERE name IN ('resolver_vault_match', 'resolver_proxy_match')")
        await conn.close()
