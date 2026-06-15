from datetime import datetime
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot


class PrimeDebtRepository(Protocol):
    """Repository interface for prime debt snapshot queries."""

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        """Return whether a prime exists for the given vault address."""
        ...

    async def list_debt_snapshots(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[PrimeDebtSnapshot]:
        """Return debt snapshots for a prime vault address."""
        ...
