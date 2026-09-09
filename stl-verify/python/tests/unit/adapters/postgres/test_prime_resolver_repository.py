from types import SimpleNamespace

import pytest

from app.adapters.postgres.prime_resolver_repository import PrimeResolverRepository
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity

_VAULT_HEX = "ab" * 20
_ROW = SimpleNamespace(id=7, name="spark", prime_key="prm_2d3ceee8415e59f3", vault_hex=_VAULT_HEX)


@pytest.mark.asyncio
async def test_resolve_returns_the_prime_identity(stub_engine) -> None:
    engine, _ = stub_engine({"fetchone.return_value": _ROW})

    assert await PrimeResolverRepository(engine).resolve("spark") == PrimeIdentity(
        id=7, name="spark", prime_key="prm_2d3ceee8415e59f3", vault_address=EthAddress("0x" + _VAULT_HEX)
    )


@pytest.mark.asyncio
async def test_resolve_returns_none_for_an_unknown_identifier(stub_engine) -> None:
    engine, _ = stub_engine({"fetchone.return_value": None})

    assert await PrimeResolverRepository(engine).resolve("nope") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("spark", {"name": "spark", "address_hex": None}),
        ("0x" + _VAULT_HEX, {"name": None, "address_hex": _VAULT_HEX}),
    ],
)
async def test_resolve_binds_the_identifier_to_the_matching_form(stub_engine, identifier, expected) -> None:
    engine, conn = stub_engine({"fetchone.return_value": _ROW})

    await PrimeResolverRepository(engine).resolve(identifier)

    assert conn.execute.await_args.args[1] == expected


@pytest.mark.asyncio
async def test_resolve_rejects_a_malformed_address_rather_than_reading_it_as_a_name(stub_engine) -> None:
    engine, _ = stub_engine({"fetchone.return_value": None})

    with pytest.raises(ValueError, match="Invalid Ethereum address"):
        await PrimeResolverRepository(engine).resolve("0xdeadbeef")


@pytest.mark.asyncio
async def test_resolve_wraps_a_database_failure(stub_engine) -> None:
    engine, _ = stub_engine(error=RuntimeError("boom"))

    with pytest.raises(ValueError, match="Database query failed while resolving prime"):
        await PrimeResolverRepository(engine).resolve("spark")
