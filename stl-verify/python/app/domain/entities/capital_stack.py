from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class CapitalStackSnapshot:
    """Latest capital stack snapshot for a prime."""

    capital_buffer: Decimal
    first_loss_capital: Decimal
    timestamp: datetime
    source: str
    reconciliation_status: str | None = None
