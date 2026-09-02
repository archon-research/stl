"""STL-tracked positions that are NOT yet backed by the Sky Atlas.

Every entry here is reproduced verbatim from the stl-verify allocation tracker
config that axis-synome replaces — specifically
``stl-verify/internal/services/allocation_tracker/entries.go`` at commit
``6e58579^``. They are kept separate from
:data:`axis_synome.spec.entities.assets_by_prime.ASSETS_BY_PRIME` because no
Atlas Instance Configuration Document currently sanctions them: a search of the
Atlas ``content/`` tree by contract address found no Token Address node for any
of them (each constant's docstring records what, if anything, the Atlas does
say).

These rows are exported alongside the Atlas-backed assets so the generated
contract continues to match the legacy tracker, and so axis-synome reviewers can
see exactly which positions still need an Atlas reference (or need to be
dropped). They are ordinary :class:`~axis_synome.spec.entities.assets.Asset`
instances so the spec's formulas can consume them, but unlike Atlas-backed
assets they carry ``allocation_type`` and ``token_type`` explicitly (the literal
STL classifications, which for ``pol`` / ``asset`` / ``proxy`` / ``psm3`` /
``risk_capital`` are not a function of protocol and cannot be derived), and they
carry no per-instance ``:source_uuid:`` because no Atlas document sanctions them
— each constant's prose docstring records the provenance gap instead.

``token`` / ``underlying_assets`` / ``categories`` were filled in to make these
valid Assets: underlyings were confirmed on-chain (ERC-20 ``symbol()``, Curve
``coins()``); for a direct token holding the underlying is the token itself, so
``underlying_asset_address`` equals the holding's own ``address``.

Each constant is a named module-level constant whose docstring carries its
provenance note; ``MISSING_FROM_ATLAS_BY_PRIME`` at the bottom aggregates the
constants per (Prime). This keeps the notes machine-readable (a ``#`` comment on
a list element is not).
"""

from typing import Final

from app.risk_engine._vendored_synome.spec.entities.asset_categories import AssetCategory
from app.risk_engine._vendored_synome.spec.entities.assets import Asset
from app.risk_engine._vendored_synome.spec.entities.assets_by_prime import PrimeName
from app.risk_engine._vendored_synome.spec.entities.networks import Network
from app.risk_engine._vendored_synome.spec.entities.protocol_sets import (
    AllocationType,
    Protocol,
    TokenType,
)
from app.risk_engine._vendored_synome.spec.entities.tokens import Token
from app.risk_engine._vendored_synome.spec_support.evm_address import EvmAddress

# Mainnet USDC, reused as the underlying asset for several entries below.
USDC_MAINNET = EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")


# --- Spark ---

SPARK_MAINNET_ARKIS_VAULT: Final[Asset] = Asset(
    token=Token.SPARK_PRIME_USDC_1,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x38464507E02c983F20428a6E8566693fE9e422a9"),
    protocol=Protocol.ARKIS,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.ERC4626,
    underlying_asset_address=USDC_MAINNET,
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.LENDING_MARKET},
)
"""Real Arkis vault. Absent from the Atlas: the Arkis Instance doc
(A.6.1.1.1.2.6.1.3.1.10.1) Token Address is 0x377C3bd9… (the SparkLend USDC
aToken — the mislabel removed in e031fbe6), not this vault."""


SPARK_MAINNET_CURVE_PYUSD_USDS: Final[Asset] = Asset(
    token=Token.PYUSD_USDS,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f"),
    protocol=Protocol.CURVE,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.CURVE,
    underlying_asset_address=EvmAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
    underlying_assets=(Token.PYUSD, Token.USDS),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Curve pyUSD/USDS pool. The underlying_asset_address (PYUSD) is the stl-verify
value, like every field here. This lives here and not among the Atlas-backed
assets because the Atlas Instance doc (A.6.1.1.1.2.6.1.3.1.7.4) has a Token
Address node but no "Underlying Asset Address" node, so there is no Atlas
document to cite as the underlying_asset source (and the rule is: never invent
one). Carries no per-instance ``:source_uuid:``, so it holds the address without
Atlas provenance."""


SPARK_MAINNET_SUPERSTATE_USCC: Final[Asset] = Asset(
    token=Token.USCC,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x14d60E7FDC0D71d8611742720E4C50E7a974020c"),
    protocol=Protocol.SUPERSTATE,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.SUPERSTATE,
    underlying_asset_address=USDC_MAINNET,
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""Superstate (USCC). Absent from the Atlas (only the USTB Superstate instance,
0x43415eB6…, exists). Categorised PERPETUAL_POSITION: the Atlas Financial-CRR
taxonomy classes the whole Superstate issuer under Perpetual Positions (the
sibling USTB instance is PERPETUAL_POSITION), and USCC is a crypto basis/carry
fund — not a real-world asset."""


SPARK_MAINNET_ANCHORAGE: Final[Asset] = Asset(
    token=Token.USAT,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x49506C3Aa028693458d6eE816b2EC28522946872"),
    protocol=Protocol.ANCHORAGE,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.ANCHORAGE,
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""Anchorage. This contract is the Atlas "Destination Address" of the Anchorage
USAT instance (A.6.1.1.1.2.6.1.3.1.13.1), not its Token Address (0x07041776…);
the STL entry carries no underlying asset.

:ambiguity: underlying_asset is USDC (the presumed RWA settlement/funding asset);
the on-chain address is an Anchorage custody/Destination Address with no
``symbol()``, so the underlying is not on-chain-verifiable."""


SPARK_BASE_PSM3: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.BASE,
    address=EvmAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E"),
    protocol=Protocol.PSM3,
    allocation_type=AllocationType.PSM3,
    token_type=TokenType.PSM3,
    underlying_asset_address=EvmAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Base PSM3. Absent: no PSM3 Instance docs exist, and this address is actually
the Spark mainnet ALM Proxy (A.6.1.1.1.2.6.1.2.1.1.1.2.1.5)."""


SPARK_ARBITRUM_PSM3: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.ARBITRUM,
    address=EvmAddress("0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266"),
    protocol=Protocol.PSM3,
    allocation_type=AllocationType.PSM3,
    token_type=TokenType.PSM3,
    underlying_asset_address=EvmAddress("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Arbitrum PSM3. Absent (no PSM3 Instance docs)."""


SPARK_ARBITRUM_POL: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.ARBITRUM,
    address=EvmAddress("0x6491c05A82219b8D1479057361ff1654749b876b"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x6491c05A82219b8D1479057361ff1654749b876b"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Arbitrum POL (sky). Absent (Spark Arbitrum tree has only Fluid + Aave)."""


SPARK_BASE_POL: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.BASE,
    address=EvmAddress("0x820C137fa70C8691f0e44Dc420a5e53c168921Dc"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x820C137fa70C8691f0e44Dc420a5e53c168921Dc"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Base POL (sky). Absent (Spark Base tree has only Morpho/Fluid/Aave)."""


# Optimism. The Atlas defines Spark's Optimism ALM infrastructure (the ALM Proxy,
# A.6.1.1.1.2.6.1.2.1.1.1.2.5.5) but has no allocation / instance configuration
# documents for positions on Optimism, so the entries below have no Token Address
# node to source.

SPARK_OPTIMISM_PSM3: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.OPTIMISM,
    address=EvmAddress("0xe0F9978b907853F354d79188A3dEfbD41978af62"),
    protocol=Protocol.PSM3,
    allocation_type=AllocationType.PSM3,
    token_type=TokenType.PSM3,
    underlying_asset_address=EvmAddress("0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Optimism PSM3 (see the Optimism note above)."""


SPARK_OPTIMISM_POL_PROXY: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.OPTIMISM,
    address=EvmAddress("0xb5B2dc7fd34C249F4be7fB1fCea07950784229e0"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.PROXY,
    underlying_asset_address=EvmAddress("0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Optimism protocol-owned liquidity held via a proxy (see the Optimism note)."""


SPARK_OPTIMISM_ASSET_USDC: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.OPTIMISM,
    address=EvmAddress("0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"),
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Optimism idle USDC (see the Optimism note)."""


SPARK_OPTIMISM_POL: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.OPTIMISM,
    address=EvmAddress("0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Optimism protocol-owned liquidity (see the Optimism note)."""


# Unichain. As with Optimism, the Atlas defines Spark's Unichain ALM
# infrastructure (the ALM Proxy, A.6.1.1.1.2.6.1.2.1.1.1.2.4.5) but has no
# allocation / instance configuration documents for positions on Unichain, so
# the entries below have no Token Address node to source.

SPARK_UNICHAIN_PSM3: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.UNICHAIN,
    address=EvmAddress("0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f"),
    protocol=Protocol.PSM3,
    allocation_type=AllocationType.PSM3,
    token_type=TokenType.PSM3,
    underlying_asset_address=EvmAddress("0x078D782b760474a361dDA0AF3839290b0EF57AD6"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Unichain PSM3 (see the Unichain note above)."""


SPARK_UNICHAIN_POL_PROXY: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.UNICHAIN,
    address=EvmAddress("0xA06b10Db9F390990364A3984C04FaDf1c13691b5"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.PROXY,
    underlying_asset_address=EvmAddress("0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Unichain protocol-owned liquidity held via a proxy (see the Unichain note)."""


SPARK_UNICHAIN_ASSET_USDC: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.UNICHAIN,
    address=EvmAddress("0x078D782b760474a361dDA0AF3839290b0EF57AD6"),
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x078D782b760474a361dDA0AF3839290b0EF57AD6"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Unichain idle USDC (see the Unichain note)."""


SPARK_UNICHAIN_POL: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.UNICHAIN,
    address=EvmAddress("0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Unichain protocol-owned liquidity (see the Unichain note)."""


# Idle asset / POL / risk-capital holdings and "skip existing worker"
# allocations. These were live rows in entries.go but were not part of the
# handover's 25 backfill. They have no Atlas Instance Configuration Document
# (idle treasury / protocol-owned liquidity held in the proxies, plus a few
# allocations a separate STL worker also tracked) and are reproduced verbatim so
# the tracker keeps accounting for these balances now that the export is its sole
# source.

SPARK_MAINNET_POL_SUSDS: Final[Asset] = Asset(
    token=Token.S_USDS,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC4626,
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet sUSDS held as protocol-owned liquidity."""


SPARK_MAINNET_ETHENA_USDE: Final[Asset] = Asset(
    token=Token.USDE,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"),
    protocol=Protocol.ETHENA,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"),
    underlying_assets=(Token.USDE,),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""Mainnet Ethena USDe ("skip — existing worker" in entries.go).

:ambiguity: Categorised PERPETUAL_POSITION for consistency with the sUSDe
sibling (Ethena synthetic-dollar exposure); idle USDe is arguably
CASH_STABLECOIN or DIRECT_EXPOSURE. Pending Atlas confirmation."""


SPARK_MAINNET_ASSET_DAI: Final[Asset] = Asset(
    token=Token.DAI,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    underlying_assets=(Token.DAI,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet idle DAI."""


SPARK_MAINNET_ASSET_USDC: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.ETHEREUM_MAINNET,
    address=USDC_MAINNET,
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet idle USDC."""


SPARK_MAINNET_ASSET_USDT: Final[Asset] = Asset(
    token=Token.USDT,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
    underlying_assets=(Token.USDT,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet idle USDT."""


SPARK_MAINNET_POL_USDS: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet USDS protocol-owned liquidity (ALM Proxy)."""


SPARK_MAINNET_ASSET_PYUSD: Final[Asset] = Asset(
    token=Token.PYUSD,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
    protocol=Protocol.PAYPAL,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
    underlying_assets=(Token.PYUSD,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet PYUSD held idle (Paypal)."""


SPARK_MAINNET_RISK_CAPITAL_USDS: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.RISK_CAPITAL,
    token_type=TokenType.ERC20,
    wallet_address=EvmAddress("0x3300f198988e4C9C63F75dF86De36421f06af8c4"),
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet USDS risk capital held in the Spark SubProxy (not the ALM Proxy) —
Atlas A.6.1.1.1.2.6.1.2.1.1.1.3.1.1.2."""


SPARK_BASE_POL_PROXY: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.BASE,
    address=EvmAddress("0x5875eEE11Cf8398102FdAd704C9E96607675467a"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.PROXY,
    underlying_asset_address=EvmAddress("0x820C137fa70C8691f0e44Dc420a5e53c168921Dc"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Base USDS protocol-owned liquidity held via a proxy."""


SPARK_ARBITRUM_POL_PROXY: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.ARBITRUM,
    address=EvmAddress("0xdDb46999F8891663a8F2828d25298f70416d7610"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.PROXY,
    underlying_asset_address=EvmAddress("0x6491c05A82219b8D1479057361ff1654749b876b"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Arbitrum USDS protocol-owned liquidity held via a proxy."""


SPARK_ARBITRUM_ASSET_USDC: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.ARBITRUM,
    address=EvmAddress("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Arbitrum idle USDC."""


# --- Grove ---

GROVE_MAINNET_CURVE_AUSD_USDC: Final[Asset] = Asset(
    token=Token.AUSD_USDC,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xE79C1C7E24755574438A26D5e062Ad2626C04662"),
    protocol=Protocol.CURVE,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.CURVE,
    underlying_asset_address=USDC_MAINNET,
    underlying_assets=(Token.USDC, Token.AUSD),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet Curve AUSD/USDC. Present in the Atlas only as a Grove "Pool Address"
(A.6.1.1.2.2.6.1.3.1.6.4 / 6.5), not as a Token Address."""


GROVE_MAINNET_UNISWAP_AUSD_USDC: Final[Asset] = Asset(
    token=Token.UNISWAP_AUSD_USDC_POOL,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xbAFeAd7c60Ea473758ED6c6021505E8BBd7e8E5d"),
    protocol=Protocol.UNISWAP,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.UNI_V3_POOL,
    underlying_asset_address=USDC_MAINNET,
    underlying_assets=(Token.USDC, Token.AUSD),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet Uniswap v3 AUSD/USDC. Present in the Atlas only as a Grove "Pool
Address" (A.6.1.1.2.2.6.1.3.1.12.2 / 12.3), not a Token Address."""


GROVE_PLUME_CENTRIFUGE_FEEDER: Final[Asset] = Asset(
    token=Token.JTRSY,
    network=Network.PLUME,
    address=EvmAddress("0xa5d465251fBCc907f5Dd6bB2145488DFC6a2627b"),
    protocol=Protocol.CENTRIFUGE,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.CENTRIFUGE_FEEDER,
    underlying_asset_address=EvmAddress("0x222365EF19F7947e5484218551B56bb3965Aa7aF"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""Plume Centrifuge feeder (second feeder). Absent: only the 0x9477… ACRDX feeder
is in the Atlas (A.6.1.1.2.2.6.1.3.5.1.1); this one is not."""


GROVE_PLUME_ASSET_USDC: Final[Asset] = Asset(
    token=Token.USDC,
    network=Network.PLUME,
    address=EvmAddress("0x222365EF19F7947e5484218551B56bb3965Aa7aF"),
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x222365EF19F7947e5484218551B56bb3965Aa7aF"),
    underlying_assets=(Token.USDC,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Plume USDC held idle. In the Atlas this address is the Underlying Asset Address
of the ACRDX feeder, not a tracked position of its own."""


GROVE_MAINNET_ASSET_DAI: Final[Asset] = Asset(
    token=Token.DAI,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    underlying_assets=(Token.DAI,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet idle DAI."""


GROVE_MAINNET_POL_USDS: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.POL,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet USDS protocol-owned liquidity (ALM Proxy)."""


GROVE_MAINNET_RISK_CAPITAL_USDS: Final[Asset] = Asset(
    token=Token.USDS,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    protocol=Protocol.SKY,
    allocation_type=AllocationType.RISK_CAPITAL,
    token_type=TokenType.ERC20,
    wallet_address=EvmAddress("0x1369f7b2b38c76B6478c0f0E66D94923421891Ba"),
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    underlying_assets=(Token.USDS,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Mainnet USDS risk capital held in the Grove SubProxy (not the ALM Proxy) —
Atlas A.6.1.1.2.2.6.1.2.1.1.1.3.1.1.2."""


# Robinhood Chain (ARCT-374). The Atlas has no documents for Grove on Robinhood
# yet (no ALM Proxy Contract, no instance configuration documents); addresses and
# holdings were verified on-chain: the vault's asset() is USDG and the Grove ALM
# Proxy (0x29626c2d…42BD5) holds both positions.

GROVE_ROBINHOOD_ASSET_USDG: Final[Asset] = Asset(
    token=Token.USDG,
    network=Network.ROBINHOOD,
    address=EvmAddress("0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
    protocol=None,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
    underlying_assets=(Token.USDG,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Robinhood USDG (Paxos Global Dollar) held idle (see the Robinhood note)."""


GROVE_ROBINHOOD_STEAKHOUSE_USDG: Final[Asset] = Asset(
    token=Token.GROVE_USDG,
    network=Network.ROBINHOOD,
    address=EvmAddress("0xBEEff039907422219Fb367e525954DDC092854d9"),
    protocol=Protocol.GROVE_STEAKHOUSE_USDG_MORPHO_VAULT,
    allocation_type=AllocationType.ALLOCATION,
    token_type=TokenType.ERC4626,
    underlying_asset_address=EvmAddress("0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
    underlying_assets=(Token.USDG,),
    categories={AssetCategory.LENDING_MARKET},
)
"""Robinhood Grove x Steakhouse USDG Morpho vault (groveUSDG, see the Robinhood
note)."""


GROVE_MONAD_ASSET_AUSD: Final[Asset] = Asset(
    token=Token.AUSD,
    network=Network.MONAD,
    address=EvmAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"),
    protocol=Protocol.AGORA,
    allocation_type=AllocationType.ASSET,
    token_type=TokenType.ERC20,
    underlying_asset_address=EvmAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"),
    underlying_assets=(Token.AUSD,),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Monad AUSD held idle (Agora)."""


# star -> list of STL entries that lack an Atlas reference, in the same order as
# entries.go.
MISSING_FROM_ATLAS_BY_PRIME: Final[dict[PrimeName, list[Asset]]] = {
    PrimeName.SPARK: [
        SPARK_MAINNET_ARKIS_VAULT,
        SPARK_MAINNET_CURVE_PYUSD_USDS,
        SPARK_MAINNET_SUPERSTATE_USCC,
        SPARK_MAINNET_ANCHORAGE,
        SPARK_BASE_PSM3,
        SPARK_ARBITRUM_PSM3,
        SPARK_ARBITRUM_POL,
        SPARK_BASE_POL,
        SPARK_OPTIMISM_PSM3,
        SPARK_OPTIMISM_POL_PROXY,
        SPARK_OPTIMISM_ASSET_USDC,
        SPARK_OPTIMISM_POL,
        SPARK_UNICHAIN_PSM3,
        SPARK_UNICHAIN_POL_PROXY,
        SPARK_UNICHAIN_ASSET_USDC,
        SPARK_UNICHAIN_POL,
        SPARK_MAINNET_POL_SUSDS,
        SPARK_MAINNET_ETHENA_USDE,
        SPARK_MAINNET_ASSET_DAI,
        SPARK_MAINNET_ASSET_USDC,
        SPARK_MAINNET_ASSET_USDT,
        SPARK_MAINNET_POL_USDS,
        SPARK_MAINNET_ASSET_PYUSD,
        SPARK_MAINNET_RISK_CAPITAL_USDS,
        SPARK_BASE_POL_PROXY,
        SPARK_ARBITRUM_POL_PROXY,
        SPARK_ARBITRUM_ASSET_USDC,
    ],
    PrimeName.GROVE: [
        # Grove's Centrifuge JAAA/JTRSY share holdings are emitted from the
        # Centrifuge instances in assets_by_prime.py (the ERC-7540 vault's share()
        # is resolved at export time), so the former Janus-Henderson workaround
        # entries for those share tokens are no longer listed here.
        GROVE_MAINNET_CURVE_AUSD_USDC,
        GROVE_MAINNET_UNISWAP_AUSD_USDC,
        GROVE_PLUME_CENTRIFUGE_FEEDER,
        GROVE_PLUME_ASSET_USDC,
        GROVE_MAINNET_ASSET_DAI,
        GROVE_MAINNET_POL_USDS,
        GROVE_MAINNET_RISK_CAPITAL_USDS,
        GROVE_MONAD_ASSET_AUSD,
        GROVE_ROBINHOOD_ASSET_USDG,
        GROVE_ROBINHOOD_STEAKHOUSE_USDG,
    ],
}
