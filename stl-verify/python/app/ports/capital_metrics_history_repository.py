from datetime import datetime
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.capital_metrics import CapitalMetricsSnapshot
from app.domain.entities.time_series_bucket import CapitalMetricsBucket


class CapitalMetricsHistoryRepository(Protocol):
    """Repository interface for stored capital-metrics snapshot queries."""

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        """Return whether a prime exists for the given vault address."""
        ...

    async def list_snapshots(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[CapitalMetricsSnapshot]:
        """Return capital-metrics snapshots for a prime vault address."""
        ...

    async def list_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[CapitalMetricsBucket]:
        """Return the last observed metrics per time bucket (LOCF gap-filled)."""
        ...
