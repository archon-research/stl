"""Self-computed per-prime risk-capital entities.

Composed at request time from on-chain exposure (receipt-token allocations),
the on-chain SubProxy treasury (Total Risk Capital), and the default RRC model
(``gap_sweep``). These are model-derived figures, intentionally independent of
the upstream Star feed; they are partial (only allocations the model can price
contribute Required Risk Capital) and will not match Sky's dashboard.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from app.domain.exceptions import AllocationUnpricedReason

# Closed set of ``unpriced_reason`` values. The share-data / price-data reasons
# are reused from ``AllocationUnpricedError`` so the two cannot drift.
UnpricedReason = Literal["no_model"] | AllocationUnpricedReason


@dataclass(frozen=True)
class AllocationRiskCapital:
    """Per-allocation risk capital from the default model.

    ``required_risk_capital_usd`` / ``crr_pct`` / ``model`` are ``None`` when the
    allocation is not priced (``applied`` is then ``False``). ``unpriced_reason``
    says *why* it is unpriced so callers can distinguish a genuinely unmodelled
    position from a transient data gap:

    - ``None`` — the allocation is priced (``applied`` is ``True``).
    - ``"no_model"`` — no default model applies (non-lending / zero-exposure).
    - ``"share_data_missing"`` / ``"share_data_stale"`` — a model applies but its
      pool-share lookup could not be resolved (e.g. a warm-up window or an
      un-indexed receipt token); the rest of the prime is still priced.
    - ``"price_data_missing"`` — a model applies but the backed asset's loan token
      has no USD price, so its loan-token-denominated backing cannot be valued;
      the rest of the prime is still priced.
    - ``"adapter_data_missing"`` — a model applies to a Morpho VaultV2 whose active
      liquidity adapters are not indexed yet, so its collateral (and liquidation
      params) is unknown; the rest of the prime is still priced.
    """

    receipt_token_id: int
    symbol: str
    protocol_name: str
    exposure_usd: Decimal
    applied: bool
    required_risk_capital_usd: Decimal | None
    crr_pct: Decimal | None
    model: str | None
    unpriced_reason: UnpricedReason | None = None

    def __post_init__(self) -> None:
        # ``applied`` and ``unpriced_reason`` encode the same bit two ways; guard
        # them (and the priced fields) at construction so a hand-written call site
        # cannot assemble a contradictory allocation.
        priced = self.required_risk_capital_usd is not None
        if priced != self.applied or (self.unpriced_reason is None) != self.applied:
            raise ValueError(
                "applied must agree with priced fields and unpriced_reason: "
                f"applied={self.applied}, required_risk_capital_usd={self.required_risk_capital_usd!r}, "
                f"unpriced_reason={self.unpriced_reason!r}"
            )


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
