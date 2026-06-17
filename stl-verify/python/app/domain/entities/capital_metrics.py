"""Capital metrics and buffer calculations for prime risk management."""

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


@dataclass(frozen=True)
class CapitalMetrics:
    """Prime-level capital metrics for risk and buffer calculations."""

    prime_id: str
    prime_name: str
    risk_capital: Decimal  # Total risk-bearing capital
    capital_buffer: Decimal  # Baseline protective capital
    first_loss_capital: Decimal  # Prime-owned first-loss layer above buffer
    total_capital: Decimal  # Sum of all capital tiers
    risk_to_capital_ratio: Optional[Decimal]  # Risk / Capital (<1.0 safe, >1.0 risky); None when unavailable
    timestamp: datetime
    benchmark_source: Optional[str] = None  # Source of validation/reconciliation
    is_validated: bool = False  # Whether reconciled against external source
    validation_note: Optional[str] = None  # Caveat or gap in validation


@dataclass(frozen=True)
class CapitalMetricsSnapshot:
    """Per-prime capital metrics observed at a point in time.

    Backed by the capital_metrics_snapshot hypertable. ``capital_buffer`` is
    derived as ``max(total_capital - first_loss_capital, 0)`` and never stored
    independently of its inputs.
    """

    prime_address: str
    prime_name: str
    risk_capital: Decimal
    total_capital: Decimal
    first_loss_capital: Decimal
    capital_buffer: Decimal
    risk_to_capital_ratio: Decimal | None
    benchmark_source: str
    synced_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.prime_address, str) or not _ETH_ADDRESS_RE.match(self.prime_address):
            raise ValueError(
                f"prime_address must be a valid Ethereum address (0x + 40 hex chars), got {self.prime_address!r}"
            )
        if not self.prime_name or not self.prime_name.strip():
            raise ValueError("prime_name must be non-empty")
        for name, value in (
            ("risk_capital", self.risk_capital),
            ("total_capital", self.total_capital),
            ("first_loss_capital", self.first_loss_capital),
            ("capital_buffer", self.capital_buffer),
        ):
            if value < 0:
                raise ValueError(f"{name} must be non-negative, got {value}")
