from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PassThroughHolding:
    """A directly-held allocated asset collapsed to the single token that backs it.

    For a wrapper whose redeemable value is tracked in another token
    (``underlying_token_id`` differs from the held token), the holding IS that
    underlying and ``amount`` is the aggregated ``underlying_value``; otherwise
    it is the held token itself and ``amount`` is the aggregated ``balance``.
    Both source columns are already decimals-normalized. ``price_usd`` is None
    when no enabled oracle prices the token.
    """

    token_id: int
    symbol: str
    amount: Decimal
    price_usd: Decimal | None
