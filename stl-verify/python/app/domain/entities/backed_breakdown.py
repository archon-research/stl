from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CollateralContribution:
    """A single collateral asset contribution.

    backing_value's basis is protocol-defined; read it together with the producing
    repository:
      - Aave-like: attributed debt-token units (not necessarily USD), computed
        as (collateral_usd / total_collateral_usd_for_user) * target_debt summed across
        borrowers, so SUM(backing_value) == total backed asset debt.
      - Morpho / pre-priced protocols (e.g. Maple Syrup): USD. Morpho attributes the
        vault's loan-token supply across markets, then scales by the loan-token price;
        Maple stores the collateral row's raw USD value.
    Consumers must therefore branch on the protocol's basis rather than assume a single
    one; treating a USD value as debt-token units would silently mis-scale it.

    price_usd is each row token's OWN USD price (the collateral token's price for a
    collateral row, the loan token's price for a loan-token row), so amount and price
    stay denominated in ``symbol``. It is None when that token has no price.

    Morpho has one exception: if the vault's loan token itself has no price, no backing
    amount can be converted to USD, so backing_value is left in raw loan-token units and
    price_usd is None on EVERY row. The risk service detects this all-unpriced breakdown
    and treats the allocation as price_data_missing, never reading the raw value as USD.

    token_id is None for symbol-keyed collateral (custody assets such as BTC/SOL that
    have no on-chain Ethereum token).
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
