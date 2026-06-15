from datetime import datetime

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.ports.prime_debt_repository import PrimeDebtRepository


class PrimeDebtService:
    """Service for prime debt snapshot retrieval."""

    def __init__(self, repository: PrimeDebtRepository) -> None:
        self._repository = repository

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        return await self._repository.prime_exists(prime_address)

    async def list_debt_snapshots(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[PrimeDebtSnapshot]:
        return await self._repository.list_debt_snapshots(
            prime_address,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            limit=limit,
        )
