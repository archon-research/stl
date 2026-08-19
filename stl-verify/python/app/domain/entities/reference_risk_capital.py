"""Upstream Star-monitor risk-capital entities.

The reference counterpart to :mod:`app.domain.entities.prime_risk_capital`:
where those figures are computed from on-chain data and the default RRC model,
these are read verbatim from Sky's own Star Agents Risk Capital & Requirements
Monitor. They exist so the API can serve the same response shape from either
provenance, and so the two can be compared without either being recomputed.

Everything here is a *current* snapshot. The upstream monitor exposes no
history at any granularity, so a reference figure can never be reconstructed
for a past instant — only observed going forward.
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ReferenceAllocation:
    """One position from the upstream per-allocation breakdown.

    ``token_address`` is not always an address: Uniswap V4 positions carry a
    32-byte pool id (66 chars) in the same field, which by construction cannot
    resolve to a receipt token. Callers join on it defensively and tolerate a
    miss rather than treating it as an address.
    """

    protocol_name: str
    network: str
    symbol: str
    name: str
    token_address: str
    loan_token_address: str
    loan_token_symbol: str
    exposure_usd: Decimal
    required_risk_capital_usd: Decimal
    # Rescaled at the adapter boundary: upstream reports a 0-1 fraction, every
    # consumer in this codebase reads a 0-100 percentage.
    crr_pct: Decimal
    # Resolved against STL's token registry by the service, not the adapter.
    # ``None`` whenever the join cannot be made — an unmapped network, a pool id
    # in place of an address, or a token STL does not index.
    receipt_token_id: int | None = None
    chain: str | None = None


@dataclass(frozen=True)
class ReferencePrimeRiskCapital:
    """A prime's upstream risk-capital snapshot, totals plus breakdown.

    The totals come from the monitor's per-prime detail endpoint and
    ``per_allocation`` from its allocations endpoint — two separately-computed
    live snapshots, not one atomic read. They reconcile only to about 1e-6
    relative (observed), so the totals are served as-is rather than recomputed
    from the breakdown, and consumers must not assert exact agreement.
    """

    star: str
    exposure_usd: Decimal
    required_risk_capital_usd: Decimal
    total_risk_capital_usd: Decimal
    encumbrance_ratio: Decimal | None
    exposure_share: Decimal
    junior_risk_capital_usd: Decimal
    senior_risk_capital_usd: Decimal
    internal_junior_risk_capital_usd: Decimal
    external_junior_risk_capital_usd: Decimal
    tokenized_junior_risk_capital_usd: Decimal
    internal_senior_risk_capital_usd: Decimal
    external_senior_risk_capital_usd: Decimal
    epi_utilization: Decimal
    spj_utilization: Decimal
    per_allocation: tuple[ReferenceAllocation, ...]
