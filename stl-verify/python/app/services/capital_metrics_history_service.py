from datetime import datetime

from app.domain.entities.allocation import EthAddress
from app.domain.entities.capital_metrics import CapitalMetricsSnapshot
from app.domain.entities.time_series_bucket import CapitalMetricsBucket
from app.ports.capital_metrics_history_repository import CapitalMetricsHistoryRepository


class CapitalMetricsHistoryService:
    """Service for stored capital-metrics snapshot retrieval."""

    def __init__(self, repository: CapitalMetricsHistoryRepository) -> None:
        self._repository = repository

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        return await self._repository.prime_exists(prime_address)

    async def list_snapshots(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[CapitalMetricsSnapshot]:
        return await self._repository.list_snapshots(
            prime_address,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            limit=limit,
        )

    async def list_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[CapitalMetricsBucket]:
        return await self._repository.list_buckets(
            prime_address,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )
