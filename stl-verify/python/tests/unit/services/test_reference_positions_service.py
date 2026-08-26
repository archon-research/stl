"""Star resolution and pass-through for the stored balance sheet."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress, Prime
from app.domain.entities.reference_position import ReferencePosition, ReferencePositionSnapshot
from app.services.reference_positions_service import ReferencePositionsService

_PROXY = EthAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
_UNKNOWN_PROXY = EthAddress("0x" + "ab" * 20)
_VAULT = EthAddress("0x" + "ba" * 20)
_TOKEN = "0x" + "cd" * 20
_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _position() -> ReferencePosition:
    return ReferencePosition(
        protocol_name="sparklend",
        network="ethereum",
        symbol="spUSDS",
        name="Spark USDS",
        token_address=_TOKEN,
        assets_usd=Decimal("787379142.91"),
        allocated_assets_usd=None,
        idle_assets_usd=None,
        receipt_token_id=41,
        chain_id=1,
        chain="mainnet",
    )


def _prime(name: str, address: EthAddress, vault: EthAddress | None = None) -> Prime:
    return Prime(
        id=str(address),
        name=name,
        address=str(address),
        chain_id=1,
        chain="mainnet",
        role="alm",
        prime_vault_address=str(vault) if vault else None,
    )


def _service(
    *,
    snapshot: ReferencePositionSnapshot | None = None,
    primes: list[Prime] | None = None,
):
    provider = AsyncMock()
    provider.get_positions.return_value = snapshot
    directory = AsyncMock()
    directory.list_primes.return_value = primes or []
    return ReferencePositionsService(provider, directory), provider


def _snapshot(*positions: ReferencePosition) -> ReferencePositionSnapshot:
    return ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=positions)


@pytest.mark.asyncio
async def test_returns_none_when_no_cycle_has_reported_on_the_prime(monkeypatch):
    # Coverage is the reader's answer, not this service's: a prime the indexer
    # has never landed a cycle for has no reference data at all.
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "obex")
    service, _ = _service(snapshot=None)

    assert await service.get(_PROXY) is None


@pytest.mark.asyncio
async def test_an_empty_snapshot_for_a_covered_prime_is_not_none(monkeypatch):
    # A covered prime reporting no positions is an empty list, which is a claim;
    # `None` means "no reference data at all", which is a different one.
    # Collapsing them would turn a genuinely empty balance sheet into a 404.
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _ = _service(snapshot=_snapshot())

    result = await service.get(_PROXY)

    assert result is not None
    assert result.positions == ()


@pytest.mark.asyncio
async def test_returns_none_when_the_address_names_no_prime(monkeypatch):
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: None)
    service, provider = _service(snapshot=_snapshot(_position()))

    assert await service.get(_PROXY) is None
    provider.get_positions.assert_not_awaited()


@pytest.mark.asyncio
async def test_serves_the_rows_and_the_cycle_they_were_observed_at(monkeypatch):
    # The registry join is the reader's SQL now, so the service must pass rows
    # through untouched rather than re-resolving them.
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _ = _service(snapshot=_snapshot(_position()))

    result = await service.get(_PROXY)

    assert result is not None
    assert result.synced_at == _SYNCED_AT
    assert [row.receipt_token_id for row in result.positions] == [41]


# Self mode answers for a prime's vault address, so reference mode must too:
# the same URL differs only in which figures it returns.
@pytest.mark.asyncio
async def test_resolves_a_prime_by_its_vault_address(monkeypatch):
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: None)
    service, provider = _service(
        snapshot=_snapshot(),
        primes=[_prime("spark", _UNKNOWN_PROXY, vault=_VAULT)],
    )

    assert await service.get(_VAULT) is not None
    provider.get_positions.assert_awaited_once_with("spark")


# A proxy holds positions before the contract is told about it during a chain
# onboarding; reference mode must not go dark for it.
@pytest.mark.asyncio
async def test_resolves_a_proxy_the_contract_does_not_index_yet(monkeypatch):
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: None)
    service, provider = _service(
        snapshot=_snapshot(),
        primes=[_prime("spark", _UNKNOWN_PROXY)],
    )

    assert await service.get(_UNKNOWN_PROXY) is not None
    provider.get_positions.assert_awaited_once_with("spark")
