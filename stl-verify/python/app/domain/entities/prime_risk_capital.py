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
    """

    # ``None`` only for a reference-sourced allocation whose upstream position
    # does not resolve to a receipt token — a Uniswap V4 row carries a 32-byte
    # pool id where an address is expected, so it can never resolve.
    receipt_token_id: int | None
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
class ChainRiskCapital:
    """One ALM proxy's contribution to its prime's aggregated figures.

    Carried so a consumer can audit the prime-level sum, and so a chain
    contributing nothing is visibly present rather than absent. ``chain`` is
    ``None`` for a proxy absent from the axis-synome contract, which has no
    discoverable chain.

    The figures are ``None`` — not ``0`` — for a proxy on a chain no allocation
    tracker serves. Such a proxy has no ``allocation_position`` rows at all, so a
    zero would claim the prime holds nothing there when the truth is that STL does
    not know: the two must not read alike, because the difference moves the
    prime's encumbrance in the direction that looks safe.
    """

    proxy_address: str
    chain: str | None
    exposure_usd: Decimal | None
    required_risk_capital_usd: Decimal | None
    allocation_count: int | None


@dataclass(frozen=True)
class PrimeRiskCapital:
    """Self-computed capital metrics for a prime.

    ``exposure_usd`` sums the prime's priced receipt-token allocations (the set
    the model is applied over), which is distinct from total allocation (that
    also includes bare/direct holdings). ``modeled_pct`` is the share of that
    exposure the model could price. ``encumbrance_ratio`` is ``None`` when
    Total Risk Capital is absent or zero.

    Fields without a prefix are scoped to the **queried proxy**. Fields prefixed
    ``prime_`` are scoped to the whole prime and are identical whichever of its
    proxies was queried. They aggregate the prime's ALM proxies on chains an
    allocation tracker serves; the rest contribute nothing and are named in
    ``prime_unserved_chains``, so a consumer can see that the total is bounded by
    what STL indexes rather than by what the prime holds.
    ``total_risk_capital_usd`` is prime-wide despite carrying
    no prefix (it predates the convention) and must never be summed.
    ``encumbrance_ratio`` divides a proxy-scoped numerator by that prime-wide
    denominator, so it is meaningless; it is retained unchanged for backwards
    compatibility and superseded by ``prime_encumbrance_ratio``.
    """

    # The proxy the unprefixed figures are scoped to — the address that was
    # queried, not the prime. Named for what it holds: the API serves it as both
    # `proxy_address` and, for backwards compatibility, the misnamed `prime_id`.
    proxy_address: str
    model: str
    exposure_usd: Decimal
    total_risk_capital_usd: Decimal | None
    required_risk_capital_usd: Decimal
    encumbrance_ratio: Decimal | None
    modeled_exposure_usd: Decimal
    modeled_pct: Decimal | None
    per_allocation: list[AllocationRiskCapital]
    prime_name: str | None = None
    prime_exposure_usd: Decimal = Decimal("0")
    prime_required_risk_capital_usd: Decimal = Decimal("0")
    prime_modeled_exposure_usd: Decimal = Decimal("0")
    prime_modeled_pct: Decimal | None = None
    prime_encumbrance_ratio: Decimal | None = None
    prime_proxies: tuple[str, ...] = ()
    prime_per_chain: tuple[ChainRiskCapital, ...] = ()
    prime_unserved_chains: tuple[str, ...] = ()
