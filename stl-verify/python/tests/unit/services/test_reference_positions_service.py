"""Address resolution and pass-through for the stored balance sheet.

The resolution scenarios themselves live in ``test_star_resolution.py``; these
cover what this service adds on top of them.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_position import ReferencePosition, ReferencePositionSnapshot
from app.services.reference_positions_service import ReferencePositionsService

# A real axis-synome spark ALM proxy: the service resolves the star through the
# contract, so a placeholder address would resolve to no prime at all.
_SPARK_ALM = EthAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
_UNKNOWN_PROXY = EthAddress("0x" + "ab" * 20)
_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _position() -> ReferencePosition:
    return ReferencePosition(
        protocol_name="sparklend",
        network="ethereum",
        symbol="spUSDS",
        name="Spark USDS",
        token_address="0x" + "cd" * 20,
        assets_usd=Decimal("787379142.91"),
        allocated_assets_usd=None,
        idle_assets_usd=None,
        receipt_token_id=41,
        chain_id=1,
        chain="mainnet",
    )


def _service(snapshot: ReferencePositionSnapshot | None):
    provider = AsyncMock()
    provider.get_positions.return_value = snapshot
    directory = AsyncMock()
    directory.list_primes.return_value = []
    return ReferencePositionsService(provider, directory), provider


async def test_serves_the_rows_and_the_cycle_they_were_observed_at() -> None:
    # The registry join is the reader's SQL now, so the service passes rows
    # through untouched rather than re-resolving them.
    service, _ = _service(ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=(_position(),)))

    snapshot = await service.get(_SPARK_ALM)

    assert snapshot is not None
    assert snapshot.synced_at == _SYNCED_AT
    assert [row.receipt_token_id for row in snapshot.positions] == [41]


async def test_asks_the_reader_for_the_resolved_star() -> None:
    service, provider = _service(ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=()))

    await service.get(_SPARK_ALM)

    provider.get_positions.assert_awaited_once_with("spark")


async def test_returns_none_when_no_cycle_has_reported_on_the_prime() -> None:
    service, _ = _service(None)

    assert await service.get(_SPARK_ALM) is None


async def test_returns_none_without_asking_when_the_address_names_no_prime() -> None:
    service, provider = _service(ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=(_position(),)))

    assert await service.get(_UNKNOWN_PROXY) is None
    provider.get_positions.assert_not_awaited()
