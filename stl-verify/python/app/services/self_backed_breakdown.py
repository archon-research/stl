"""Build a trivial self-backed breakdown for direct asset holdings.

Direct assets (RLUSD, PYUSD, sparkPrimeUSDC1, etc.) have no protocol
wrapper and therefore no collateral decomposition. Their "breakdown"
is a single row: the asset backs itself at 100%.
"""

from decimal import Decimal

from app.domain.entities.allocation import DirectAssetHolding
from app.domain.entities.risk import RiskBreakdown, RiskEnrichedCollateral

_HUNDRED = Decimal("100")


def build_self_backed_breakdown(holding: DirectAssetHolding) -> RiskBreakdown:
    """Return a single-item breakdown where the asset is its own collateral."""
    balance = holding.balance
    has_price = holding.amount_usd is not None
    amount_usd = holding.amount_usd if has_price else Decimal("0")
    price_usd = (amount_usd / balance) if has_price and balance and balance > 0 else None

    item = RiskEnrichedCollateral(
        token_id=holding.token_id,
        symbol=holding.symbol,
        amount=balance,
        backing_pct=_HUNDRED,
        amount_usd=amount_usd,
        price_usd=price_usd,
        liquidation_threshold=None,
        liquidation_bonus=None,
    )
    return RiskBreakdown(backed_asset_id=holding.token_id, items=(item,))
