"""Service for computing and retrieving prime-level capital metrics."""

from datetime import datetime, timezone
from decimal import Decimal

from app.domain.entities.allocation import EthAddress
from app.domain.entities.capital_metrics import CapitalMetrics
from app.ports.allocation_repository import AllocationRepository


class CapitalMetricsService:
    """Computes capital buffers and metrics for prime risk management."""

    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def get_capital_metrics(self, prime_id: EthAddress) -> CapitalMetrics | None:
        """Retrieve or compute capital metrics for a prime.

        Returns:
            CapitalMetrics if found, None if the prime does not exist.
        """
        # Validate prime exists
        primes = await self._repository.list_primes()
        prime = next((p for p in primes if EthAddress(p.id) == prime_id), None)
        if not prime:
            return None

        # TODO: compute from allocation_position + accounting layer once available.
        return CapitalMetrics(
            prime_id=prime.id,
            prime_name=prime.name,
            risk_capital=Decimal("0"),
            capital_buffer=Decimal("0"),
            first_loss_capital=Decimal("0"),
            total_capital=Decimal("0"),
            risk_to_capital_ratio=Decimal("0"),
            timestamp=datetime.now(timezone.utc),
            benchmark_source=None,
            is_validated=False,
            validation_note="Capital metrics are pending accounting layer integration.",
        )

    async def list_all_capital_metrics(self) -> list[CapitalMetrics]:
        """Return capital metrics for all primes."""
        # TODO: iterate primes and compute once accounting layer is available.
        return []
