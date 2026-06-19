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
class TotalCapitalBucket:
    """A prime's total capital (treasury) for a single time bucket (LOCF gap-filled).

    ``total_capital_usd`` is the last observed SubProxy treasury USDS balance
    carried forward into the bucket, or ``None`` for leading buckets before the
    first observation. USDS is dollar-pegged, so the raw balance is the USD
    figure (it matches the upstream Star ``total_capital`` exactly).
    """

    bucket_start: datetime
    total_capital_usd: Decimal | None

    def __post_init__(self) -> None:
        if self.total_capital_usd is not None and self.total_capital_usd < 0:
            raise ValueError(f"total_capital_usd must be non-negative, got {self.total_capital_usd}")


@dataclass(frozen=True)
class ExposureBucket:
    """A prime's priced receipt-token exposure for a single time bucket (LOCF gap-filled).

    ``exposure_usd`` is the sum across the prime's receipt-token positions of the
    last observed balance carried forward into the bucket, valued at the latest
    underlying oracle price. ``None`` for leading buckets before the first
    observation.
    """

    bucket_start: datetime
    exposure_usd: Decimal | None

    def __post_init__(self) -> None:
        if self.exposure_usd is not None and self.exposure_usd < 0:
            raise ValueError(f"exposure_usd must be non-negative, got {self.exposure_usd}")
