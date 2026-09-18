"""Pass-through of the stored balance sheet for a named prime."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from app.domain.entities.reference_position import ReferencePosition, ReferencePositionSnapshot
from app.services.reference_positions_service import ReferencePositionsService

_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _position() -> ReferencePosition:
    return ReferencePosition(
        protocol_name="sparklend",
        network="ethereum",
        symbol="spUSDS",
        name="Spark USDS",
        token_address="0x" + "cd" * 20,
        wallet_address="0x" + "ef" * 20,
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
    return ReferencePositionsService(provider), provider


async def test_serves_the_rows_and_the_cycle_they_were_observed_at() -> None:
    # The registry join is the reader's SQL now, so the service passes rows
    # through untouched rather than re-resolving them.
    service, _ = _service(ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=(_position(),)))

    snapshot = await service.get("spark")

    assert snapshot is not None
    assert snapshot.synced_at == _SYNCED_AT
    assert [row.receipt_token_id for row in snapshot.positions] == [41]


async def test_asks_the_reader_for_the_named_star() -> None:
    service, provider = _service(ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=()))

    await service.get("spark")

    provider.get_positions.assert_awaited_once_with("spark")


async def test_returns_none_when_no_cycle_has_reported_on_the_prime() -> None:
    service, _ = _service(None)

    assert await service.get("spark") is None
