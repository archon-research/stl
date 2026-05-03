"""Service for computing and retrieving prime-level capital metrics."""

from datetime import datetime
from decimal import Decimal

from app.domain.entities.allocation import EthAddress
from app.domain.entities.capital_metrics import CapitalMetrics
from app.ports.allocation_repository import AllocationRepository


class CapitalMetricsService:
    """Computes capital buffers and metrics for prime risk management.

    Risk capital is derived from on-chain positions priced with latest
    on-chain token prices. Capital buffer and first-loss components remain
    pending accounting-layer inputs.
    """

    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def get_capital_metrics(self, prime_id: EthAddress) -> CapitalMetrics | None:
        """Retrieve or compute capital metrics for a prime."""
        # Validate prime exists.
        primes = await self._repository.list_primes()
        prime = next((p for p in primes if EthAddress(p.id) == prime_id), None)
        if not prime:
            return None

        risk_capital = await self._repository.get_total_usd_exposure(prime_id)
        capital_buffer = Decimal("0")
        first_loss_capital = Decimal("0")
        total_capital = capital_buffer + first_loss_capital

        if total_capital > 0:
            risk_to_capital_ratio = risk_capital / total_capital
        else:
            risk_to_capital_ratio = Decimal("0")

        return CapitalMetrics(
            prime_id=prime.id,
            prime_name=prime.name,
            risk_capital=risk_capital,
            capital_buffer=capital_buffer,
            first_loss_capital=first_loss_capital,
            total_capital=total_capital,
            risk_to_capital_ratio=risk_to_capital_ratio,
            timestamp=datetime.utcnow(),
            benchmark_source="onchain:allocation_position+onchain_token_price",
            is_validated=False,
            validation_note=(
                "risk_capital is computed from priced on-chain positions. "
                "capital_buffer and first_loss_capital are pending accounting-layer inputs."
            ),
        )

    async def list_all_capital_metrics(self) -> list[CapitalMetrics]:
        """Return capital metrics for all primes."""
        metrics: list[CapitalMetrics] = []
        for prime in await self._repository.list_primes():
            result = await self.get_capital_metrics(EthAddress(prime.id))
            if result is not None:
                metrics.append(result)
        return metrics
