"""Aggregated time-bucket entities returned by the time-series endpoints.

Each bucket represents one ``frequency``-wide slice of a time window. The
shape is tailored per endpoint: event streams report counts (and a value sum
where meaningful), while the prime-debt value series reports the last observed
value carried forward (LOCF) into each bucket.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


def _validate_entity_counts(priced_entity_count: int | None, entity_count: int | None) -> None:
    """Shared invariant for a bucket's priced-vs-total coverage counts."""
    if priced_entity_count is not None and priced_entity_count < 0:
        raise ValueError(f"priced_entity_count must be non-negative, got {priced_entity_count}")
    if entity_count is not None and entity_count < 0:
        raise ValueError(f"entity_count must be non-negative, got {entity_count}")
    if priced_entity_count is not None and entity_count is not None and priced_entity_count > entity_count:
        raise ValueError(f"priced_entity_count {priced_entity_count} exceeds entity_count {entity_count}")


@dataclass(frozen=True)
class AllocationActivityBucket:
    """Allocation activity aggregated into a single time bucket.

    ``net_flow_usd`` is the signed net flow valued in USD (inflows positive,
    outflows negative); it may be negative, unlike ``total_tx_amount``.

    ``balance_usd`` is the bucket's position value read from recorded state
    rather than reconstructed from flow, and is populated only for
    ``series="balance"`` (VEC-760). The two are alternatives, not companions:
    each request runs one query, so the fields the other would fill are left
    ``None`` rather than a misleading zero. It is a different measure as well
    as a cheaper one -- mark-to-market rather than cost basis -- so a
    share-price move appears in ``balance_usd`` on a day with no transaction,
    and in ``net_flow_usd`` not at all.

    ``priced_entity_count`` and ``entity_count`` say how many receipt-token
    entities the reported total (``balance_usd`` or ``net_flow_usd``,
    whichever series ran) accounts for, of how many the bucket knows about --
    equal when the total is complete. On ``series="balance"`` pricing is
    all-or-nothing per token, so one token without an enabled oracle makes
    every position in it unpriceable (VEC-536, VEC-537); retiring that partial
    state once fixed is VEC-782. On both series, a bucket predating a token's
    own first in-window price is unpriced the same way rather than silently
    zeroed (VEC-763); direct (non-receipt-token) holdings need no price by
    design and are excluded from both counts.
    """

    bucket_start: datetime
    event_count: int | None
    total_tx_amount: Decimal | None
    net_flow_usd: Decimal | None
    balance_usd: Decimal | None = None
    priced_entity_count: int | None = None
    entity_count: int | None = None

    def __post_init__(self) -> None:
        if self.event_count is not None and self.event_count < 0:
            raise ValueError(f"event_count must be non-negative, got {self.event_count}")
        if self.total_tx_amount is not None and self.total_tx_amount < 0:
            raise ValueError(f"total_tx_amount must be non-negative, got {self.total_tx_amount}")
        if self.balance_usd is not None and self.balance_usd < 0:
            raise ValueError(f"balance_usd must be non-negative, got {self.balance_usd}")
        _validate_entity_counts(self.priced_entity_count, self.entity_count)


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
    last observed balance carried forward into the bucket, valued at the
    underlying oracle price AT THAT BUCKET (VEC-763). ``None`` for leading
    buckets before the first observation.

    ``priced_entity_count`` and ``entity_count`` say how many receipt-token
    positions ``exposure_usd`` accounts for, of how many the bucket has
    observed -- equal when the total is complete. A bucket predating a
    position's own underlying's first in-window price is unpriced rather than
    silently zeroed, same as the allocation-activity balance series.
    """

    bucket_start: datetime
    exposure_usd: Decimal | None
    priced_entity_count: int | None = None
    entity_count: int | None = None

    def __post_init__(self) -> None:
        if self.exposure_usd is not None and self.exposure_usd < 0:
            raise ValueError(f"exposure_usd must be non-negative, got {self.exposure_usd}")
        _validate_entity_counts(self.priced_entity_count, self.entity_count)
