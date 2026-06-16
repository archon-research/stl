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
    """Allocation activity aggregated into a single time bucket."""

    bucket_start: datetime
    event_count: int
    total_tx_amount: Decimal

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
