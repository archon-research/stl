"""Aggregated time-bucket entities returned by the time-series endpoints.

Each bucket represents one ``resolution``-wide slice of a time window. The
shape is tailored per endpoint: event streams report counts (and a value sum
where meaningful), while the prime-debt value series reports the last observed
value carried forward (LOCF) into each bucket.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class AllocationActivityBucket:
    """Allocation activity aggregated into a single time bucket.

    ``net_flow_usd`` is the signed net flow valued in USD (inflows positive,
    outflows negative); it may be negative, unlike ``total_tx_amount``.
    """

    bucket_start: datetime
    event_count: int
    total_tx_amount: Decimal
    net_flow_usd: Decimal

    def __post_init__(self) -> None:
        if self.event_count < 0:
            raise ValueError(f"event_count must be non-negative, got {self.event_count}")
        if self.total_tx_amount < 0:
            raise ValueError(f"total_tx_amount must be non-negative, got {self.total_tx_amount}")


@dataclass(frozen=True)
class ProtocolEventBucket:
    """Protocol event count for a single time bucket."""

    bucket_start: datetime
    event_count: int

    def __post_init__(self) -> None:
        if self.event_count < 0:
            raise ValueError(f"event_count must be non-negative, got {self.event_count}")


@dataclass(frozen=True)
class PrimeDebtBucket:
    """Prime debt value for a single time bucket.

    ``debt_wad`` is the last observed debt carried forward into the bucket
    (LOCF). It is ``None`` for leading buckets that precede the first
    observation, where there is no prior value to carry.
    """

    bucket_start: datetime
    debt_wad: Decimal | None

    def __post_init__(self) -> None:
        if self.debt_wad is not None and self.debt_wad < 0:
            raise ValueError(f"debt_wad must be non-negative, got {self.debt_wad}")


@dataclass(frozen=True)
class CapitalMetricsBucket:
    """Per-prime capital metrics for a single time bucket (LOCF gap-filled).

    Each value is the last observation carried forward into the bucket, or
    ``None`` for leading buckets before the first observation. ``capital_buffer``
    is derived as ``max(total_capital - first_loss_capital, 0)`` and is ``None``
    when either input is missing.
    """

    bucket_start: datetime
    risk_capital: Decimal | None
    total_capital: Decimal | None
    first_loss_capital: Decimal | None
    capital_buffer: Decimal | None
    risk_to_capital_ratio: Decimal | None
