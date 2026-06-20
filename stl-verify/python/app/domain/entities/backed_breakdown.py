from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CollateralContribution:
    """A single collateral asset contribution.

    backing_value's basis is protocol-defined; read it together with the producing
    repository:
      - Aave-like / Morpho: attributed debt-token units (not necessarily USD), computed
        as (collateral_usd / total_collateral_usd_for_user) * target_debt summed across
        borrowers, so SUM(backing_value) == total backed asset debt.
      - Maple (MapleBackedBreakdownRepository): raw USD value of the collateral row.
    Consumers must therefore branch on protocol (see CryptoLendingRiskService.is_maple)
    rather than assume a single basis; treating Maple's value as debt-token units would
    silently mis-scale it.

    token_id is None for symbol-keyed collateral (e.g. Maple custody assets such as
    BTC/SOL that have no on-chain Ethereum token).
    """

    token_id: int | None
    symbol: str
    backing_value: Decimal
    backing_pct: Decimal
    price_usd: Decimal | None


@dataclass(frozen=True)
class BackedBreakdown:
    """The full amount-first breakdown of collateral backing a debt token."""

    backed_asset_id: int
    items: tuple[CollateralContribution, ...]
