"""Capital metrics and buffer calculations for prime risk management."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class CapitalMetrics:
    """Prime-level capital metrics for risk and buffer calculations."""

    prime_id: str
    prime_name: str
    risk_capital: Decimal  # Total risk-bearing capital
    capital_buffer: Decimal  # Baseline protective capital
    first_loss_capital: Decimal  # Prime-owned first-loss layer above buffer
    total_capital: Decimal  # Sum of all capital tiers
    risk_to_capital_ratio: Decimal  # Risk / Capital (unitless; <1.0 is safe, >1.0 is risky)
    timestamp: datetime
    benchmark_source: Optional[str] = None  # Source of validation/reconciliation
    is_validated: bool = False  # Whether reconciled against external source
    validation_note: Optional[str] = None  # Caveat or gap in validation
