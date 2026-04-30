"""Service for computing and retrieving prime-level capital metrics."""

from datetime import datetime
from decimal import Decimal

from app.domain.entities.allocation import EthAddress, Prime
from app.domain.entities.capital_metrics import CapitalMetrics
from app.ports.allocation_repository import AllocationRepository


class CapitalMetricsService:
    """Computes capital buffers and metrics for prime risk management.

    This is a v1 MVP implementation. The actual capital layer computation
    depends on external accounting sources (e.g., onchain snapshots, treasury
    accounting). For now, this returns a stub that:
    1. Indicates where capital data should come from
    2. Provides the data contract for UI integration
    3. Allows for external validation/reconciliation
    """

    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def get_capital_metrics(self, prime_id: EthAddress) -> CapitalMetrics | None:
        """Retrieve or compute capital metrics for a prime.

        Returns:
            CapitalMetrics if available, None if prime not found or computation unavailable.

        Note:
            This MVP returns placeholder metrics. Real implementation requires
            coordination with accounting layer (see comments below).
        """
        # Validate prime exists
        primes = await self._repository.list_primes()
        prime = next((p for p in primes if EthAddress(p.id) == prime_id), None)
        if not prime:
            return None

        # TODO: Real implementation should:
        # 1. Query capital_metrics table (if it exists)
        # 2. Or compute from:
        #    - allocation_position (get total deployed capital)
        #    - treasury_accounts or accounting layer (get buffers)
        #    - governance/config (get first-loss allocation)
        # 3. Cache result with TTL
        # 4. Reconcile against external benchmark (observatory, sentinelwatch)

        # MVP: Return placeholder structure with notes
        return CapitalMetrics(
            prime_id=prime.id,
            prime_name=prime.name,
            risk_capital=Decimal("0"),
            capital_buffer=Decimal("0"),
            first_loss_capital=Decimal("0"),
            total_capital=Decimal("0"),
            risk_to_capital_ratio=Decimal("0"),
            timestamp=datetime.utcnow(),
            benchmark_source=None,
            is_validated=False,
            validation_note="Capital metrics MVP: awaiting accounting layer and schema design. "
            "See data_sources panel for reconciliation references.",
        )

    async def list_all_capital_metrics(self) -> list[CapitalMetrics]:
        """Return capital metrics for all primes (MVP: returns empty list).

        TODO: Once accounting layer is available, iterate primes and compute metrics.
        """
        # MVP: return empty; real implementation would iterate primes
        return []
