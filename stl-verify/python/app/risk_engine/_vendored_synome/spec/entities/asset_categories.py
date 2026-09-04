from enum import StrEnum


class AssetCategory(StrEnum):
    """High-level classification for an Asset.

    The six categories mirror the Atlas Instance Financial CRR taxonomy
    rooted at :source_uuid: 8b6a6ecd-da74-4be5-bcb8-96215f473c08. An Asset
    may carry one or more categories; the categories are used by downstream
    consumers (dashboard, Suraf, scorecards) to group assets that share an
    economic profile independent of token, protocol, or network.

    Notes on cross-category placement:

    - ``BOND_LIKE`` corresponds to Atlas Bond-Like Instruments
      (:source_uuid: da1a154c-6db8-4012-91a7-31ea4e73e95d), whose near-term
      provision (:source_uuid: a479643e-fbd3-4c9b-aba0-40f4657a8011)
      restricts eligibility to "Pendle PT tokens" — but Pendle Ethena PTs
      (PT-USDe, PT-sUSDe) are explicitly carved out into the Perpetual
      Positions branch at
      :source_uuid: 4094c159-9132-454a-81be-361a461b5098 and therefore
      carry ``PERPETUAL_POSITION``, not ``BOND_LIKE``. Other Pendle PTs
      (e.g. a hypothetical PT-USDS) would carry ``BOND_LIKE``.
    - ``DIRECT_EXPOSURE`` (:source_uuid: 69d0776b-786c-408b-b76a-860ea60b6b9a)
      is for volatile cryptoassets held idle in a wallet (ETH, stETH, WBTC).
      Stablecoin LP positions on Curve are ``CASH_STABLECOIN`` per the
      Cash Stablecoins near-term treatment at
      :source_uuid: 8aee612b-fe36-4c6b-adee-2e0762579a40, not
      ``DIRECT_EXPOSURE``.
    """

    LENDING_MARKET = "lending_market"
    DIRECT_EXPOSURE = "direct_exposure"
    REAL_WORLD_ASSET = "real_world_asset"
    CASH_STABLECOIN = "cash_stablecoin"
    BOND_LIKE = "bond_like"
    PERPETUAL_POSITION = "perpetual_position"


NON_CASH_STABILIZING_CATEGORIES: set[AssetCategory] = {
    AssetCategory.REAL_WORLD_ASSET,
    AssetCategory.PERPETUAL_POSITION,
    AssetCategory.DIRECT_EXPOSURE,
    AssetCategory.BOND_LIKE,
}
"""Categories whose presence disqualifies a position from counting as Actively
Stabilizing Collateral, regardless of how its underlying token is labelled. A
real-world asset, perpetual, direct (volatile) exposure, or bond-like position is
not liquid cash-stabilizing capital even when its underlying happens to be a cash
stablecoin (e.g. an RWA lending market whose underlying is USDC). This is the
category-level guard that keeps such positions out of the ASC math even if their
protocol is later added to a counted protocol set."""
