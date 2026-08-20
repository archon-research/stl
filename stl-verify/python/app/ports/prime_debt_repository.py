from datetime import datetime
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.domain.entities.time_series_bucket import PrimeDebtBucket


class PrimeDebtRepositoryPort(Protocol):
    """Repository interface for prime debt snapshot queries."""

    async def resolve_prime_id(self, prime_address: EthAddress) -> int | None:
        """Return the prime id for a vault or proxy address, or ``None`` if unknown."""
        ...

    async def list_debt_snapshots(
        self,
        prime_id: int,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[PrimeDebtSnapshot]:
        """Return debt snapshots for a prime id."""
        ...

    async def list_debt_buckets(
        self,
        prime_id: int,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[PrimeDebtBucket]:
        """Return the last observed debt per time bucket (LOCF gap-filled)."""
        ...

    async def list_reference_debt_buckets(
        self,
        prime_id: int,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[PrimeDebtBucket]:
        """Return Sky's reported debt for a prime ID per time bucket (LOCF gap-filled), in wad."""
        ...
