import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.psm3 import PriceQuote, Psm3Snapshot
from app.services.psm3_service import Psm3Service

_PSM3_ADDR = "0x" + "ab" * 20


@pytest.fixture
def psm3_log(caplog: pytest.LogCaptureFixture):
    """caplog wired into the service logger; the ``app`` tree has propagate=False."""
    service_logger = logging.getLogger("app.services.psm3_service")
    service_logger.addHandler(caplog.handler)
    yield caplog
    service_logger.removeHandler(caplog.handler)


def _snapshot(**overrides: Any) -> Psm3Snapshot:
    defaults: dict[str, Any] = dict(
        chain_id=8453,
        address=_PSM3_ADDR,
        usds_balance=Decimal(2 * 10**18),
        susds_balance=Decimal(4 * 10**18),
        usdc_balance=Decimal(5 * 10**6),
        total_assets=Decimal(10 * 10**18),
        conversion_rate=Decimal(105 * 10**25),  # 1.05 in 1e27
        block_number=30000000,
        block_version=0,
        block_timestamp=datetime(2026, 6, 12, 12, 0, tzinfo=UTC),
    )
    defaults.update(overrides)
    return Psm3Snapshot(**defaults)


def _repo(
    snapshots: list[Psm3Snapshot],
    prices: dict[str, PriceQuote | None] | None = None,
) -> AsyncMock:
    repo = AsyncMock()
    repo.list_latest_snapshots.return_value = snapshots
    repo.list_snapshot_history.return_value = snapshots
    fresh = datetime.now(UTC)
    defaults: dict[str, PriceQuote | None] = {
        "usds": PriceQuote(price_usd=Decimal("0.99"), timestamp=fresh),
        "usd-coin": PriceQuote(price_usd=Decimal("1.01"), timestamp=fresh),
    }
    repo.get_latest_price.side_effect = lambda asset: (prices or defaults)[asset]
    return repo


@pytest.mark.asyncio
async def test_list_reserves_enriches_with_prices() -> None:
    service = Psm3Service(_repo([_snapshot()]))

    [row] = await service.list_reserves()

    assert row.network == "base"
    assert row.address == _PSM3_ADDR
    assert row.usds_balance == Decimal("2")
    assert row.susds_balance == Decimal("4")
    assert row.usdc_balance == Decimal("5")
    assert row.usds_price == Decimal("0.99")
    assert row.usdc_price == Decimal("1.01")
    assert row.susds_price == Decimal("1.0395")  # 1.05 conversion rate x usds_price
    assert row.usds_balance_usd == Decimal("1.98")
    assert row.susds_balance_usd == Decimal("4.158")
    assert row.usdc_balance_usd == Decimal("5.05")
    assert row.total_assets == Decimal("10")  # served from the snapshot, not sum of *_usd
    assert row.block_number == 30000000
    assert row.block_version == 0


@pytest.mark.asyncio
async def test_list_reserves_maps_network_filter_to_chain_id() -> None:
    repo = _repo([_snapshot(chain_id=42161)])
    service = Psm3Service(repo)

    [row] = await service.list_reserves("arbitrum")

    assert row.network == "arbitrum"
    repo.list_latest_snapshots.assert_awaited_once_with(42161)


@pytest.mark.asyncio
async def test_list_reserves_without_filter_passes_none() -> None:
    repo = _repo([_snapshot()])
    service = Psm3Service(repo)

    await service.list_reserves()

    repo.list_latest_snapshots.assert_awaited_once_with(None)


@pytest.mark.asyncio
async def test_list_reserves_skips_price_lookup_when_no_snapshots() -> None:
    repo = _repo([])
    service = Psm3Service(repo)

    assert await service.list_reserves() == []
    repo.get_latest_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_reserve_history_maps_network_and_limit() -> None:
    repo = _repo([_snapshot(chain_id=130)])
    service = Psm3Service(repo)

    [row] = await service.list_reserve_history("unichain", limit=25)

    assert row.network == "unichain"
    repo.list_snapshot_history.assert_awaited_once_with(130, limit=25)


@pytest.mark.asyncio
async def test_missing_price_falls_back_to_par_and_warns(psm3_log: pytest.LogCaptureFixture) -> None:
    fresh = datetime.now(UTC)
    prices: dict[str, PriceQuote | None] = {
        "usds": None,
        "usd-coin": PriceQuote(price_usd=Decimal("1.01"), timestamp=fresh),
    }
    service = Psm3Service(_repo([_snapshot()], prices))

    with psm3_log.at_level(logging.WARNING):
        [row] = await service.list_reserves()

    assert row.usds_price == Decimal("1.0")
    assert row.usds_balance_usd == Decimal("2.0")
    assert row.susds_price == Decimal("1.05")  # computed from the par usds price
    assert row.usdc_price == Decimal("1.01")
    assert "no usds price found" in psm3_log.text


@pytest.mark.asyncio
async def test_stale_price_falls_back_to_par_and_warns(psm3_log: pytest.LogCaptureFixture) -> None:
    prices: dict[str, PriceQuote | None] = {
        "usds": PriceQuote(price_usd=Decimal("0.99"), timestamp=datetime.now(UTC)),
        "usd-coin": PriceQuote(price_usd=Decimal("1.05"), timestamp=datetime.now(UTC) - timedelta(hours=2)),
    }
    service = Psm3Service(_repo([_snapshot()], prices))

    with psm3_log.at_level(logging.WARNING):
        [row] = await service.list_reserves()

    assert row.usdc_price == Decimal("1.0")
    assert row.usdc_balance_usd == Decimal("5.0")
    assert row.usds_price == Decimal("0.99")
    assert "usd-coin price" in psm3_log.text
    assert "older than" in psm3_log.text


@pytest.mark.asyncio
async def test_repository_error_propagates() -> None:
    repo = _repo([_snapshot()])
    repo.list_latest_snapshots.side_effect = ValueError("db failure")
    service = Psm3Service(repo)

    with pytest.raises(ValueError, match="db failure"):
        await service.list_reserves()
