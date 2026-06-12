import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.domain.entities.psm3 import CHAIN_ID_TO_NETWORK, NETWORK_TO_CHAIN_ID, Psm3Reserves, Psm3Snapshot
from app.ports.psm3_repository import Psm3Repository

logger = logging.getLogger(__name__)

_WAD = Decimal(10) ** 18  # usds/susds/total_assets raw scale
_RAY = Decimal(10) ** 27  # conversion_rate raw scale
_USDC_UNIT = Decimal(10) ** 6
_PAR_PRICE = Decimal("1.0")
_PRICE_MAX_AGE = timedelta(hours=1)


class Psm3Service:
    """Serves PSM3 reserve snapshots with USD prices joined at read time."""

    def __init__(self, repository: Psm3Repository) -> None:
        self._repository = repository

    async def list_reserves(self, network: str | None = None) -> list[Psm3Reserves]:
        chain_id = NETWORK_TO_CHAIN_ID[network] if network is not None else None
        snapshots = await self._repository.list_latest_snapshots(chain_id)
        return await self._enrich(snapshots)

    async def list_reserve_history(self, network: str, *, limit: int = 100) -> list[Psm3Reserves]:
        snapshots = await self._repository.list_snapshot_history(NETWORK_TO_CHAIN_ID[network], limit=limit)
        return await self._enrich(snapshots)

    async def _enrich(self, snapshots: list[Psm3Snapshot]) -> list[Psm3Reserves]:
        if not snapshots:
            return []
        usds_price = await self._usd_price("usds")
        usdc_price = await self._usd_price("usd-coin")
        return [_to_reserves(snapshot, usds_price, usdc_price) for snapshot in snapshots]

    async def _usd_price(self, source_asset_id: str) -> Decimal:
        """Latest coingecko price, falling back to par 1.0 when missing or stale.

        The fallback is intentional best-effort, matching the external API's
        semantics for assets without a fresh feed.
        """
        quote = await self._repository.get_latest_price(source_asset_id)
        if quote is None:
            logger.warning("psm3: no %s price found; falling back to par price 1.0", source_asset_id)
            return _PAR_PRICE
        if datetime.now(UTC) - quote.timestamp > _PRICE_MAX_AGE:
            logger.warning(
                "psm3: latest %s price from %s is older than %s; falling back to par price 1.0",
                source_asset_id,
                quote.timestamp.isoformat(),
                _PRICE_MAX_AGE,
            )
            return _PAR_PRICE
        return quote.price_usd


def _to_reserves(snapshot: Psm3Snapshot, usds_price: Decimal, usdc_price: Decimal) -> Psm3Reserves:
    susds_price = snapshot.conversion_rate / _RAY * usds_price
    usds_balance = snapshot.usds_balance / _WAD
    susds_balance = snapshot.susds_balance / _WAD
    usdc_balance = snapshot.usdc_balance / _USDC_UNIT
    return Psm3Reserves(
        network=CHAIN_ID_TO_NETWORK[snapshot.chain_id],
        address=snapshot.address,
        usds_balance=usds_balance,
        susds_balance=susds_balance,
        usdc_balance=usdc_balance,
        usds_balance_usd=usds_balance * usds_price,
        susds_balance_usd=susds_balance * susds_price,
        usdc_balance_usd=usdc_balance * usdc_price,
        usds_price=usds_price,
        usdc_price=usdc_price,
        susds_price=susds_price,
        # total_assets is the contract's par valuation (PSM3.totalAssets()); never recompute it from the *_usd values.
        total_assets=snapshot.total_assets / _WAD,
        block_number=snapshot.block_number,
        block_version=snapshot.block_version,
        block_timestamp=snapshot.block_timestamp,
    )
