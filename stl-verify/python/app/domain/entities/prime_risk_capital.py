"""Self-computed per-prime risk-capital entities.

Composed at request time from on-chain exposure (receipt-token allocations),
the on-chain SubProxy treasury (Total Risk Capital), and the default RRC model
(``gap_sweep``). These are model-derived figures, intentionally independent of
the upstream Star feed; they are partial (only allocations the model can price
contribute Required Risk Capital) and will not match Sky's dashboard.
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class AllocationRiskCapital:
    """Per-allocation risk capital from the default model.

    ``required_risk_capital_usd`` / ``crr_pct`` / ``model`` are ``None`` when
    the model does not apply to the allocation (``applied`` is then ``False``),
    e.g. a non-lending or unpriced position.
    """

    receipt_token_id: int
    symbol: str
    protocol_name: str
    exposure_usd: Decimal
    applied: bool
    required_risk_capital_usd: Decimal | None
    crr_pct: Decimal | None
    model: str | None


@dataclass(frozen=True)
class PrimeRiskCapital:
    """Self-computed capital metrics for a prime.

    ``exposure_usd`` sums the prime's priced receipt-token allocations (the set
    the model is applied over), which is distinct from total allocation (that
    also includes bare/direct holdings). ``modeled_pct`` is the share of that
    exposure the model could price. ``encumbrance_ratio`` is ``None`` when
    Total Risk Capital is absent or zero.
    """

    prime_id: str
    model: str
    exposure_usd: Decimal
    total_risk_capital_usd: Decimal | None
    required_risk_capital_usd: Decimal
    encumbrance_ratio: Decimal | None
    modeled_exposure_usd: Decimal
    modeled_pct: Decimal | None
    per_allocation: list[AllocationRiskCapital]
