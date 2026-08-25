"""Coverage gating and registry resolution for the upstream balance sheet."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_position import ReferencePosition
from app.services.reference_positions_service import (
    _RESOLVE_CONCURRENCY,
    ReferencePositionsService,
)

_PROXY = EthAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
_TOKEN = "0x" + "cd" * 20
_V4_POOL_ID = "0x" + "ef" * 32


def _position(
    *,
    network: str = "ethereum",
    token_address: str = _TOKEN,
    chain_id: int | None = 1,
    chain: str | None = "mainnet",
) -> ReferencePosition:
    return ReferencePosition(
        protocol_name="sparklend",
        network=network,
        symbol="spUSDS",
        name="Spark USDS",
        token_address=token_address,
        assets_usd=Decimal("787379142.91"),
        allocated_assets_usd=None,
        idle_assets_usd=None,
        chain_id=chain_id,
        chain=chain,
    )


def _service(
    *,
    positions: tuple[ReferencePosition, ...] = (),
    tracked: frozenset[str] = frozenset({"spark"}),
    receipt_token_id: int | None = 41,
    primes: list | None = None,
):
    position_provider = AsyncMock()
    position_provider.get_positions.return_value = positions

    coverage = AsyncMock()
    coverage.tracked_stars.return_value = tracked

    receipt_tokens = AsyncMock()
    receipt_tokens.get_by_chain_and_address.return_value = (
        None if receipt_token_id is None else AsyncMock(receipt_token_id=receipt_token_id)
    )

    directory = AsyncMock()
    directory.list_primes.return_value = primes or []

    service = ReferencePositionsService(position_provider, coverage, receipt_tokens, directory)
    return service, position_provider, receipt_tokens


@pytest.mark.asyncio
async def test_returns_none_when_the_monitor_does_not_cover_the_prime(monkeypatch):
    # Coverage is the Star monitor's answer, not this feed's: the feed serves an
    # unknown star 200-with-no-rows, which is indistinguishable from a prime that
    # holds nothing. Asking the wrong source would publish "holds nothing".
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "obex")
    service, positions, _ = _service(tracked=frozenset({"spark"}))

    assert await service.get(_PROXY) is None
    positions.get_positions.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_empty_feed_for_a_covered_prime_is_not_none(monkeypatch):
    # The distinction the gate exists to preserve: a covered prime reporting no
    # positions is an empty list, which is a claim; `None` means "no reference
    # data at all", which is a different one. Collapsing them would turn a
    # genuinely empty balance sheet into a 404.
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _, _ = _service(positions=(), tracked=frozenset({"spark"}))

    assert await service.get(_PROXY) == ()


@pytest.mark.asyncio
async def test_returns_none_when_the_address_names_no_prime(monkeypatch):
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: None)
    service, positions, _ = _service()

    assert await service.get(_PROXY) is None
    positions.get_positions.assert_not_awaited()


@pytest.mark.asyncio
async def test_attaches_stls_receipt_token_id(monkeypatch):
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _, _ = _service(positions=(_position(),), receipt_token_id=41)

    (row,) = await service.get(_PROXY)

    assert row.receipt_token_id == 41


@pytest.mark.asyncio
async def test_keeps_a_row_stl_does_not_index(monkeypatch):
    # Most of this feed is positions STL has no registry entry for — that is why
    # it carries 59 rows against the monitor's 11. An unresolved id must not drop
    # the row.
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _, _ = _service(positions=(_position(),), receipt_token_id=None)

    (row,) = await service.get(_PROXY)

    assert row.receipt_token_id is None
    assert row.symbol == "spUSDS"


@pytest.mark.asyncio
async def test_skips_the_registry_lookup_where_it_cannot_succeed(monkeypatch):
    # A pool id is not an address and an unmapped chain has no id to look up, so
    # the query is not issued rather than issued and allowed to miss.
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _, receipt_tokens = _service(
        positions=(
            _position(token_address=_V4_POOL_ID),
            _position(network="plume", chain_id=None, chain=None),
        )
    )

    rows = await service.get(_PROXY)

    assert [row.receipt_token_id for row in rows] == [None, None]
    receipt_tokens.get_by_chain_and_address.assert_not_awaited()


@pytest.mark.asyncio
async def test_registry_lookups_stay_inside_the_connection_pool(monkeypatch):
    """The enrichment fan-out is capped, not left to the row count.

    Each lookup opens its own connection, and this feed carries an order of
    magnitude more rows than the breakdown it replaced. Unbounded, one request
    can queue for more connections than the pool holds and then time out on a
    queue it created itself.
    """
    monkeypatch.setattr("app.services.star_resolution.prime_name_for", lambda _: "spark")
    service, _, receipt_tokens = _service(
        positions=tuple(_position() for _ in range(_RESOLVE_CONCURRENCY * 4)),
    )

    live = 0
    peak = 0

    async def counting_lookup(*_args, **_kwargs):
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        # Yields, so every task that is allowed to start has started before any
        # finishes — without this the peak reads 1 whatever the limit is.
        await asyncio.sleep(0)
        live -= 1
        return None

    receipt_tokens.get_by_chain_and_address.side_effect = counting_lookup

    rows = await service.get(_PROXY)

    assert len(rows) == _RESOLVE_CONCURRENCY * 4
    assert peak <= _RESOLVE_CONCURRENCY
