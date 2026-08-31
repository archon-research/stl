"""MorphoVaultAllocationsReader port — a vault's live per-market allocations.

Used by CoreModelRiskService to serve a MetaMorpho vault share: the vault
spreads its loan-token supply across many Blue markets at once (n:m), so the
service needs the allocation weights to combine per-market CORE results into
one figure for the receipt token.

This is deliberately NOT the gap_sweep backed breakdown: that query splits each
market's supply into a borrowed slice (``supply × utilization``, attributed to
the collateral token) and an unborrowed remainder folded into loan-token rows
indistinguishable from vault-level idle cash. CORE results are per *market*,
and a Morpho supplier's bad-debt exposure is its whole supply in that market,
so the weights here are the raw per-market supply amounts.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class MorphoVaultMarketAllocation:
    """The vault's latest supply into one Blue market, in loan-token units.

    ``supply_assets`` is positive by contract: a fully exited market is not an
    allocation, so implementations drop zero-supply rows.
    """

    morpho_market_id: int
    collateral_symbol: str
    loan_symbol: str
    supply_assets: Decimal


@dataclass(frozen=True)
class MorphoVaultAllocations:
    """A vault's total assets and per-market allocations, in loan-token units.

    ``total_assets`` comes from the latest vault state snapshot and is ``0``
    when the vault has no state row yet. Idle liquidity is not a row: it is
    ``total_assets - sum(a.supply_assets)``, clamped at zero (state and
    position snapshots are written at different blocks, so the difference can
    transiently be negative).
    """

    vault_id: int
    loan_token_symbol: str
    total_assets: Decimal
    allocations: tuple[MorphoVaultMarketAllocation, ...]


class MorphoVaultAllocationsReader(Protocol):
    async def get_vault_allocations(self, receipt_token_address: bytes, chain_id: int) -> MorphoVaultAllocations | None:
        """Return the vault behind a receipt-token address, or None if unknown."""
        ...
