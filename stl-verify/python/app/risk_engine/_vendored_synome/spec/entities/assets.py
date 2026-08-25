from typing import Annotated, Final

from pydantic import Field

from app.risk_engine._vendored_synome.spec.entities.asset_categories import AssetCategory
from app.risk_engine._vendored_synome.spec.entities.networks import Network
from app.risk_engine._vendored_synome.spec.entities.protocol_sets import AllocationType, Protocol, TokenType
from app.risk_engine._vendored_synome.spec.entities.tokens import Token
from app.risk_engine._vendored_synome.spec_support.evm_address import EvmAddress
from app.risk_engine._vendored_synome.spec_support.validated_dataclass import validated_dataclass


@validated_dataclass
class Asset:
    """One Allocation Instance of the Allocation System Primitive, as recorded
    in the owning Prime Agent's Artifact.

    Every Agent Artifact must have an Instance Configuration Document
    detailing the configuration of each Instance of a given Primitive,
    including its Parameters. The fields below mirror the Parameters subtree
    every Allocation Instance Configuration Document carries: the Instance
    Identifiers children (Network, Target Protocol, Asset Supplied By <Prime>
    Liquidity Layer, Token) and the Contract Addresses children (Token
    Address, Underlying Asset Address). The Atlas defines those child
    documents only through this repeated per-instance structure — there is no
    generic field-definition document — so per-instance provenance lives on
    each named binding's trailing docstring.

    :source_uuid: 3d7f42a5-8c7d-44ed-9a85-2ab8e7cca2f5
    """

    token: Token
    """The token the Instance holds, mirroring the Instance Identifiers
    "Token" document of the Instance Configuration Document."""

    network: Network
    """Blockchain the Instance is deployed on, mirroring the Instance
    Identifiers "Network" document. The List Of Allocation Instances
    requirement groups active Allocation Instances by blockchain, with the
    blockchain named.

    :source_uuid: e4975062-6d19-438b-a5d5-cfc1a7fd8cb9
    """

    protocol: Protocol | None
    """Protocol the Instance allocates into, mirroring the Instance
    Identifiers "Target Protocol" document. ``None`` for idle holdings that sit
    in a wallet with no target protocol (see assets_missing_from_atlas.py)."""

    address: EvmAddress
    """On-chain address of the Instance's token contract, mirroring the
    Contract Addresses "Token Address" document. The Required Allocation
    Instance Parameters requirement makes the address of each Allocation
    Instance mandatory.

    :source_uuid: 6ad0fdb4-bd11-4d5a-a436-0d106873e0ec
    """

    underlying_assets: Annotated[tuple[Token, ...], Field(min_length=1)]
    """The asset(s) underlying this position. For a single-asset holding this is
    a one-tuple naming the asset whose contract is at ``underlying_asset_address``
    (Atlas counterpart: the Instance Identifiers "Asset Supplied By <Prime>
    Liquidity Layer" document). For a liquidity-pool position it lists every coin
    in the pool (e.g. ``(Token.USDC, Token.USDT)``), since the Atlas single
    "Asset Supplied By" identifier cannot represent a multi-coin pool."""

    underlying_asset_address: EvmAddress
    """On-chain address of the primary underlying asset's token contract on the
    Instance's network, mirroring the Contract Addresses "Underlying Asset
    Address" document. For pool positions this is one representative coin; the
    full coin set is in ``underlying_assets``."""

    categories: Annotated[set[AssetCategory], Field(min_length=1)]
    """High-level economic classification(s). See ``AssetCategory`` for the
    Atlas Instance Financial CRR taxonomy provenance."""

    allocation_type: AllocationType | None = None
    """STL allocation classification (``allocation`` / ``pol`` / ``asset`` /
    ``psm3`` / ``risk_capital``). ``None`` for Atlas-backed instances, where the
    export derives it from ``protocol``; set explicitly only for positions whose
    legacy STL value is not a function of protocol (see
    assets_missing_from_atlas.py)."""

    token_type: TokenType | None = None
    """STL token-wrapper classification (``erc20`` / ``erc4626`` / ``atoken`` /
    ``curve`` / ``psm3`` / ``proxy`` / …). ``None`` for Atlas-backed instances,
    where the export derives it from ``protocol``; set explicitly otherwise."""

    wallet_address: EvmAddress | None = None
    """Overrides the wallet this position binds to in the export. ``None`` binds
    to the canonical ALM Proxy; set only for ``risk_capital`` holdings that live
    in a SubProxy wallet."""


# Singular named asset bindings. Each trailing docstring carries two
# ``:source_uuid:`` fields: first the Atlas Token Address document of the
# asset, second its Underlying Asset Address document.

GROVE_GACLO_1: Final[Asset] = Asset(
    token=Token.GACLO_1,
    network=Network.AVALANCHE,
    protocol=Protocol.GALAXY,
    address=EvmAddress("0x2C0aDFF8e114f3cA106051144353aC703D24B901"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""Galaxy Arch CLOs; the underlying is USDC. The GACLO-1 token and position live
on Avalanche, so the Atlas Underlying Asset Address node records the
Avalanche-native USDC (0xB97EF9Ef…), not mainnet USDC; a consumer reading the
underlying on Avalanche needs the chain-local token (same convention as
GROVE_JAAA_AVALANCHE / GROVE_JTRSY_AVALANCHE and GROVE_MONAD_UNISWAP_AUSD_USDC).

:source_uuid: 931d7521-9740-4913-8f36-52bbb856dca2
:source_uuid: 44803afa-e8f9-4247-afda-a25fcedd8226
"""


GROVE_MONAD_UNISWAP_AUSD_USDC: Final[Asset] = Asset(
    token=Token.UNISWAP_AUSD_USDC_POOL,
    network=Network.MONAD,
    protocol=Protocol.UNISWAP,
    address=EvmAddress("0x6B405DCA74897c9442d369DcF6c0EC230f7E1c7C"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0x754704Bc059F8C67012fEd69BC8A327a5aafb603"),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""Monad Uniswap AUSD/USDC pool. The pool and its tokens live on Monad, so the
Atlas Underlying Asset Address node records the Monad-native USDC
(0x754704Bc…), not mainnet USDC; a consumer reading the underlying on Monad
needs the chain-local token. The token name carries no "LP" marker so the
export derives token_type "uni_v3_pool".

:source_uuid: 65f7e17e-61c6-452c-a352-abc2f9e92fb3
:source_uuid: 4e680b6a-9f48-41d7-b7aa-655a36a5c068
"""


GROVE_JAAA_AVALANCHE: Final[Asset] = Asset(
    token=Token.JAAA,
    network=Network.AVALANCHE,
    protocol=Protocol.CENTRIFUGE,
    address=EvmAddress("0x1121F4e21eD8B9BC1BB9A2952cDD8639aC897784"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""JAAA via Centrifuge on Avalanche.

:source_uuid: c0f78cfe-30ca-4026-a8ad-a0391debe389
:source_uuid: 87989bfc-6d92-4d66-b26b-e007d0b7bbc0
"""


GROVE_JTRSY_AVALANCHE: Final[Asset] = Asset(
    token=Token.JTRSY,
    network=Network.AVALANCHE,
    protocol=Protocol.CENTRIFUGE,
    address=EvmAddress("0xFE6920eB6C421f1179cA8c8d4170530CDBdfd77A"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""JTRSY via Centrifuge on Avalanche.

:source_uuid: f9f790ea-f67a-4e6d-ac63-cd84faf208fe
:source_uuid: fcf231c8-f8a5-4073-be59-cde9a5f86a29
"""


GROVE_BBQ_USDC_BASE: Final[Asset] = Asset(
    token=Token.GROVE_BBQ_USDC,
    network=Network.BASE,
    protocol=Protocol.MORPHO,
    address=EvmAddress("0xBeEf2d50B428675a1921bC6bBF4bfb9D8cF1461A"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
    categories={AssetCategory.LENDING_MARKET},
)
"""GROVE_BBQ_USDC via Morpho on Base.

:source_uuid: 1dc90986-481b-4e3a-a38c-7a9a636bb1da
:source_uuid: 200c6217-d44c-4a1e-90b3-94735e35959a
"""


GROVE_STEAK_USDC: Final[Asset] = Asset(
    token=Token.STEAK_USDC,
    network=Network.BASE,
    protocol=Protocol.MORPHO,
    address=EvmAddress("0xbeef0e0834849aCC03f0089F01f4F1Eeb06873C9"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
    categories={AssetCategory.LENDING_MARKET},
)
"""STEAK_USDC via Morpho on Base.

:source_uuid: e85ae1d4-c31d-4eae-a05c-6a1844918cfd
:source_uuid: 889f7585-dcca-4e87-a9ca-bb1308115252
"""


GROVE_A_ETH_USDC: Final[Asset] = Asset(
    token=Token.A_ETH_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_CORE_V3,
    address=EvmAddress("0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_USDC via Aave Core V3 on Ethereum Mainnet.

:source_uuid: 1899a80c-f660-488a-8ec1-7c9322bd602c
:source_uuid: 39624e4c-91c2-4520-b353-b8c06b7bb4d8
"""


GROVE_A_ETH_RLUSD: Final[Asset] = Asset(
    token=Token.A_ETH_RLUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_CORE_V3,
    address=EvmAddress("0xFa82580c16A31D0c1bC632A36F82e83EfEF3Eec0"),
    underlying_assets=(Token.RLUSD,),
    underlying_asset_address=EvmAddress("0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_RLUSD via Aave Core V3 on Ethereum Mainnet.

:source_uuid: 9e6c6c25-b323-4406-a327-2da9de622c3b
:source_uuid: 02e46eb2-5c2e-4d28-861a-aba2b729fb7a
"""


GROVE_A_HOR_RWA_RLUSD: Final[Asset] = Asset(
    token=Token.A_HOR_RWA_RLUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_HORIZON,
    address=EvmAddress("0xE3190143Eb552456F88464662f0c0C4aC67A77eB"),
    underlying_assets=(Token.RLUSD,),
    underlying_asset_address=EvmAddress("0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD"),
    categories={AssetCategory.LENDING_MARKET, AssetCategory.REAL_WORLD_ASSET},
)
"""A_HOR_RWA_RLUSD via Aave Horizon on Ethereum Mainnet.

:source_uuid: 9d0c9c24-3982-44a7-b96f-c1bf25c41b10
:source_uuid: 80dd4ee6-eb44-4a56-8679-5d9df5a18fb2
"""


GROVE_A_HOR_RWA_USDC: Final[Asset] = Asset(
    token=Token.A_HOR_RWA_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_HORIZON,
    address=EvmAddress("0x68215B6533c47ff9f7125aC95adf00fE4a62f79e"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET, AssetCategory.REAL_WORLD_ASSET},
)
"""A_HOR_RWA_USDC via Aave Horizon on Ethereum Mainnet.

:source_uuid: 6a88f6e1-07be-4994-b7c5-a9f7a9b0d2cc
:source_uuid: 8c65206c-cba3-4e4b-bb97-7ce46b5bcf91
"""


GROVE_AUSD: Final[Asset] = Asset(
    token=Token.AUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AGORA_AUSD,
    address=EvmAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""AUSD via Agora Ausd on Ethereum Mainnet.

:source_uuid: b4a55280-bf9d-477a-9d42-1b2a421f6028
:source_uuid: f8bcf8e0-e5a8-41c3-9963-2ba85697265f
:ambiguity: underlying_asset set to USDC to match this instance's Atlas Underlying Asset Address document (mainnet USDC, 0xA0b8…), which records the USDC cash funding the position rather than the held AUSD token itself (same convention as GROVE_GACLO_1). The economic underlying of AUSD is arguably the token itself; revisit at the Atlas level.
"""


GROVE_BUIDL_I: Final[Asset] = Asset(
    token=Token.BUIDL_I,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.BLACKROCK,
    address=EvmAddress("0x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""BUIDL_I via Blackrock on Ethereum Mainnet.

:source_uuid: 2c3d5162-5aac-4b5d-838d-8bc2952b7852
:source_uuid: 45250d99-f5d0-48f7-a8b6-a92bcbb95c05
"""


GROVE_JAAA_ETHEREUM_MAINNET: Final[Asset] = Asset(
    token=Token.JAAA,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.CENTRIFUGE,
    address=EvmAddress("0x4880799eE5200fC58DA299e965df644fBf46780B"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""JAAA via Centrifuge on Ethereum Mainnet.

:source_uuid: fe31ba7d-30cc-4fec-b74a-0dea0f633730
:source_uuid: 14d69bb4-b7a8-4780-8563-4a798768d8b3
"""


GROVE_JTRSY_ETHEREUM_MAINNET: Final[Asset] = Asset(
    token=Token.JTRSY,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.CENTRIFUGE,
    address=EvmAddress("0xFE6920eB6C421f1179cA8c8d4170530CDBdfd77A"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""JTRSY via Centrifuge on Ethereum Mainnet.

:source_uuid: 39cf2050-ef55-49d4-b59d-fc1b0a11ac59
:source_uuid: fbe40152-f7e6-4a4e-87ed-4e419687e40d
"""


GROVE_PT_S_USDE: Final[Asset] = Asset(
    token=Token.PT_S_USDE,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.ETHENA,
    address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""PT_S_USDE via Ethena on Ethereum Mainnet.

:source_uuid: 3b108b97-6f88-4d96-a55a-38c39191281e
:source_uuid: 914e6bcf-a73b-4c80-bfa8-04dbd58a7805
:ambiguity: underlying_asset set to USDC to match this instance's Atlas Underlying Asset Address document (mainnet USDC, 0xA0b8…), which records the USDC cash funding the position rather than the held PT-sUSDe token itself (same convention as GROVE_GACLO_1). The economic underlying of PT-sUSDe is arguably the token itself; revisit at the Atlas level.
"""


GROVE_PT_USDE: Final[Asset] = Asset(
    token=Token.PT_USDE,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.ETHENA,
    address=EvmAddress("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""PT_USDE via Ethena on Ethereum Mainnet.

:source_uuid: 9008b149-de3d-429d-824f-48524063d657
:source_uuid: dd7eaa92-d40a-4ffc-9359-ce7d8a9f01fe
:ambiguity: underlying_asset set to USDC to match this instance's Atlas Underlying Asset Address document (mainnet USDC, 0xA0b8…), which records the USDC cash funding the position rather than the held PT-USDe token itself (same convention as GROVE_GACLO_1). The economic underlying of PT-USDe is arguably the token itself; revisit at the Atlas level.
"""


GROVE_S_USDE: Final[Asset] = Asset(
    token=Token.S_USDE,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.ETHENA_PROTOCOL,
    address=EvmAddress("0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"),
    underlying_assets=(Token.USDE,),
    underlying_asset_address=EvmAddress("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""S_USDE via Ethena Protocol on Ethereum Mainnet.

:source_uuid: 002874e2-43a6-4daa-8e02-1b6f9291d02f
:source_uuid: be355bce-80b5-4e9d-a78f-ee5b87aa7117
"""


GROVE_BBQ_AUSD: Final[Asset] = Asset(
    token=Token.GROVE_BBQ_AUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.GROVE_STEAKHOUSE_AUSD_MORPHO_VAULT,
    address=EvmAddress("0xBEEfF0d672ab7F5018dFB614c93981045D4aA98a"),
    underlying_assets=(Token.AUSD,),
    underlying_asset_address=EvmAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"),
    categories={AssetCategory.LENDING_MARKET},
)
"""GROVE_BBQ_AUSD via Grove Steakhouse Ausd Morpho Vault on Ethereum Mainnet.

:source_uuid: 69648727-b4ab-45e7-85f9-c2846917d944
:source_uuid: 5ec16337-978a-4b21-bf47-326db289a2ef
"""


GROVE_BBQ_USDC_STEAKHOUSE_V2: Final[Asset] = Asset(
    token=Token.GROVE_BBQ_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.GROVE_STEAKHOUSE_USDC_HIGH_YIELD_VAULT_V2,
    address=EvmAddress("0xBeefF08dF54897e7544aB01d0e86f013DA354111"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""GROVE_BBQ_USDC via Grove Steakhouse Usdc High Yield Vault V2 on Ethereum Mainnet.

:source_uuid: 3c0cd2b5-035d-460d-92dd-b45c1e7a64a1
:source_uuid: 76adcd24-8473-4e8e-a42c-0c7583e13936
"""


GROVE_BBQ_USDC_MORPHO: Final[Asset] = Asset(
    token=Token.GROVE_BBQ_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MORPHO,
    address=EvmAddress("0xBEEf2B5FD3D94469b7782aeBe6364E6e6FB1B709"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""GROVE_BBQ_USDC via Morpho on Ethereum Mainnet.

:source_uuid: dcdff78c-809f-4ec8-80a2-36c124ca9ae8
:source_uuid: ff7b0875-7f6d-4b18-b609-34eec3f725a0
"""


GROVE_RLUSD: Final[Asset] = Asset(
    token=Token.RLUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.RIPPLE,
    address=EvmAddress("0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""RLUSD via Ripple on Ethereum Mainnet.

:source_uuid: a7a0518a-cc8a-460c-b9a4-58eae7130455
:source_uuid: 6f3ad27d-cb6d-4135-a0ad-444c3d3b2df6
:ambiguity: underlying_asset set to USDC to match this instance's Atlas Underlying Asset Address document (mainnet USDC, 0xA0b8…), which records the USDC cash funding the position rather than the held RLUSD token itself (same convention as GROVE_GACLO_1). The economic underlying of RLUSD is arguably the token itself; revisit at the Atlas level.
"""


GROVE_STAC: Final[Asset] = Asset(
    token=Token.STAC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SECURITIZE,
    address=EvmAddress("0x51C2d74017390CbBd30550179A16A1c28F7210fc"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""STAC via Securitize on Ethereum Mainnet.

:source_uuid: b6737216-3829-40ae-b033-846080f61d34
:source_uuid: 19b23b6a-f8a5-4db8-8768-88b045bab3d2
:ambiguity: underlying_asset set to USDC to match this instance's Atlas Underlying Asset Address document (mainnet USDC, 0xA0b8…), which records the USDC cash funding the position rather than the held STAC token itself (same convention as GROVE_GACLO_1). The economic underlying of STAC is arguably the token itself; revisit at the Atlas level.
"""


GROVE_BBQ_PYUSD: Final[Asset] = Asset(
    token=Token.GROVE_BBQ_PYUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.STEAKHOUSE_PYUSD_MORPHO_VAULT,
    address=EvmAddress("0xd8A6511979D9C5D387c819E9F8ED9F3a5C6c5379"),
    underlying_assets=(Token.PYUSD,),
    underlying_asset_address=EvmAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
    categories={AssetCategory.LENDING_MARKET},
)
"""GROVE_BBQ_PYUSD via Steakhouse Pyusd Morpho Vault on Ethereum Mainnet.

:source_uuid: b7a5cbbf-15c4-4b0e-ba18-4dfa9994a212
:source_uuid: c9d27694-0ebd-4f06-b5b3-07c3879bf438
"""


GROVE_USDT_0: Final[Asset] = Asset(
    token=Token.USDT_0,
    network=Network.PLASMA,
    protocol=Protocol.AAVE_V3,
    address=EvmAddress("0x5D72a9d9A9510Cd8cBdBA12aC62593A58930a948"),
    underlying_assets=(Token.USDT_0,),
    underlying_asset_address=EvmAddress("0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb"),
    categories={AssetCategory.LENDING_MARKET},
)
"""USDT_0 via Aave V3 on Plasma.

:source_uuid: c1b87980-9050-48ba-82ef-b5f65ba0840f
:source_uuid: bfacb6db-078c-49f1-9e68-0e8a1c9ddef8
:ambiguity: underlying_asset_address diverges from its Atlas source document (A.6.1.1.2.2.6.1.3.4.1.1.2.2.2), which records 0x8292Bb45…317eD — that is RLUSD's mainnet token address, an apparent Atlas authoring error. Corrected here to the aToken's on-chain UNDERLYING_ASSET_ADDRESS() (0xB8CE59…5ebb = USDT0); the Atlas document needs a corresponding fix.
"""


GROVE_ACRDX: Final[Asset] = Asset(
    token=Token.ACRDX,
    network=Network.PLUME,
    protocol=Protocol.CENTRIFUGE,
    address=EvmAddress("0x9477724Bb54AD5417de8Baff29e59DF3fB4DA74f"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0x222365EF19F7947e5484218551B56bb3965Aa7aF"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""ACRDX via Centrifuge on Plume.

:source_uuid: 08beab86-27af-4c89-8d68-c8b1ad0c8476
:source_uuid: e9d21b2c-cfec-4abd-a611-c7586d5acdb2
"""


OBEX_SYRUP_USDC: Final[Asset] = Asset(
    token=Token.SYRUP_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MAPLE,
    address=EvmAddress("0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SYRUP_USDC via Maple on Ethereum Mainnet.

:source_uuid: 85a64942-705d-4079-a265-2510ae4310f7
:source_uuid: e9a1b7f7-df7c-4b9c-83d1-96fc3b109089
"""


SPARK_A_ARB_USDCN: Final[Asset] = Asset(
    token=Token.A_ARB_USDCN,
    network=Network.ARBITRUM,
    protocol=Protocol.AAVE,
    address=EvmAddress("0x724dc807b04555b71ed48a6896b6F41593b8C637"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ARB_USDCN via Aave on Arbitrum.

:source_uuid: 8768aad5-79b2-4e6d-a92b-6e1c654681a7
:source_uuid: 3fe18ab3-8f90-494e-8c8b-0b4218dd77f6
"""


SPARK_FS_USDS_ARBITRUM: Final[Asset] = Asset(
    token=Token.FS_USDS,
    network=Network.ARBITRUM,
    protocol=Protocol.FLUID_FINANCE_ERC4626_VAULT,
    address=EvmAddress("0x3459fcc94390C3372c0F7B4cD3F8795F0E5aFE96"),
    underlying_assets=(Token.S_USDS,),
    underlying_asset_address=EvmAddress("0xdDb46999F8891663a8F2828d25298f70416d7610"),
    categories={AssetCategory.LENDING_MARKET},
)
"""FS_USDS via Fluid Finance Erc4626 Vault on Arbitrum.

:source_uuid: d9b0d43b-3d65-453d-8099-f49e7959e6a4
:source_uuid: 656e1bad-91a3-4360-9804-a04ac194b1c7
"""


SPARK_A_AVAX_USDC: Final[Asset] = Asset(
    token=Token.A_AVAX_USDC,
    network=Network.AVALANCHE,
    protocol=Protocol.AAVE,
    address=EvmAddress("0x625E7708f30cA75bfd92586e17077590C60eb4cD"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_AVAX_USDC via Aave on Avalanche.

:source_uuid: ea787215-4911-47e3-a9dc-e6b3f16f6e47
:source_uuid: 494409b0-468f-4abb-b634-9f26d02f2bbe
"""


SPARK_SP_USDC_AVALANCHE_SPARK_SAVINGS: Final[Asset] = Asset(
    token=Token.SP_USDC,
    network=Network.AVALANCHE,
    protocol=Protocol.SPARK_SAVINGS_PROTOCOL,
    address=EvmAddress("0x28B3a8fb53B741A8Fd78c0fb9A6B2393d896a43d"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_USDC via Spark Savings Protocol on Avalanche.

:source_uuid: 32e9d09c-1f8c-44b8-a281-f51a68351d41
:source_uuid: f6168c84-306f-4f20-afd6-fd24e84d405e
"""


SPARK_A_BAS_USDC: Final[Asset] = Asset(
    token=Token.A_BAS_USDC,
    network=Network.BASE,
    protocol=Protocol.AAVE,
    address=EvmAddress("0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_BAS_USDC via Aave on Base.

:source_uuid: 92d1ebed-51c8-4877-898e-e21c0cc85e6d
:source_uuid: ce769e22-56cc-4ab1-91a7-ae8d12c2f9fd
"""


SPARK_FS_USDS_BASE: Final[Asset] = Asset(
    token=Token.FS_USDS,
    network=Network.BASE,
    protocol=Protocol.FLUID_FINANCE_ERC4626_VAULT,
    address=EvmAddress("0xf62e339f21d8018940f188F6987Bcdf02A849619"),
    underlying_assets=(Token.S_USDS,),
    underlying_asset_address=EvmAddress("0x5875eEE11Cf8398102FdAd704C9E96607675467a"),
    categories={AssetCategory.LENDING_MARKET},
)
"""FS_USDS via Fluid Finance Erc4626 Vault on Base.

:source_uuid: 5ce2cf40-bc6f-48fe-894b-aca0c6a8ecec
:source_uuid: 8b66880c-e943-46b2-8411-a1c84dc0a5f6
"""


SPARK_USDC: Final[Asset] = Asset(
    token=Token.SPARK_USDC,
    network=Network.BASE,
    protocol=Protocol.MORPHO_BLUE_ERC4626_VAULT,
    address=EvmAddress("0x7BfA7C4f149E7415b73bdeDfe609237e29CBF34A"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SPARK_USDC via Morpho Blue Erc4626 Vault on Base.

:source_uuid: 89fc9a0a-0407-463e-8f45-2b2ca6e1d832
:source_uuid: 2efab1a6-9c66-4b61-af68-2740efd8d475
"""


SPARK_A_ETH_USDC: Final[Asset] = Asset(
    token=Token.A_ETH_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_CORE,
    address=EvmAddress("0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_USDC via Aave Core on Ethereum Mainnet.

:source_uuid: 2f0e8c66-aabb-48c0-a9ed-d9a7d0652737
:source_uuid: a11796f1-e89e-4dfc-b53e-0ab6527cc025
"""


SPARK_A_ETH_USDE: Final[Asset] = Asset(
    token=Token.A_ETH_USDE,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_CORE,
    address=EvmAddress("0x4F5923Fc5FD4a93352581b38B7cD26943012DECF"),
    underlying_assets=(Token.USDE,),
    underlying_asset_address=EvmAddress("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_USDE via Aave Core on Ethereum Mainnet.

:source_uuid: 6f8813ff-3f2c-4eb1-be25-10b6b428781d
:source_uuid: a0682e7a-b111-4283-80c9-e806dd1bd225
"""


SPARK_A_ETH_USDS: Final[Asset] = Asset(
    token=Token.A_ETH_USDS,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_CORE,
    address=EvmAddress("0x32a6268f9Ba3642Dda7892aDd74f1D34469A4259"),
    underlying_assets=(Token.USDS,),
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_USDS via Aave Core on Ethereum Mainnet.

:source_uuid: b1c3fe3e-922f-4261-ab62-f0103b5a1cdd
:source_uuid: f366a310-9e3a-4b4f-9437-4fa3bbf72d65
"""


SPARK_A_ETH_USDT: Final[Asset] = Asset(
    token=Token.A_ETH_USDT,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_CORE,
    address=EvmAddress("0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a"),
    underlying_assets=(Token.USDT,),
    underlying_asset_address=EvmAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_USDT via Aave Core on Ethereum Mainnet.

:source_uuid: 6f712e66-f262-4db6-b846-282865e16156
:source_uuid: 1f74c7a5-f038-4bc6-824b-6005ff313297
"""


SPARK_A_ETH_LIDO_USDS: Final[Asset] = Asset(
    token=Token.A_ETH_LIDO_USDS,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.AAVE_PRIME,
    address=EvmAddress("0x09AA30b182488f769a9824F15E6Ce58591Da4781"),
    underlying_assets=(Token.USDS,),
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    categories={AssetCategory.LENDING_MARKET},
)
"""A_ETH_LIDO_USDS via Aave Prime on Ethereum Mainnet.

:source_uuid: 5c10f62b-25cc-4daf-877c-36f9291d585d
:source_uuid: 6a4979e7-46f8-49ce-acbe-fa8b28d2693a
"""


SPARK_BUIDL_I: Final[Asset] = Asset(
    token=Token.BUIDL_I,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.BLACKROCK,
    address=EvmAddress("0x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""BUIDL_I via Blackrock on Ethereum Mainnet.

:source_uuid: 43930781-984c-4ba4-91e1-5e564fe448ad
:source_uuid: 284c77f5-ea1b-4569-a4f4-9241cf338f9b
"""


SPARK_JTRSY: Final[Asset] = Asset(
    token=Token.JTRSY,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.CENTRIFUGE,
    address=EvmAddress("0x8c213ee79581Ff4984583C6a801e5263418C4b86"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.REAL_WORLD_ASSET},
)
"""JTRSY via Centrifuge on Ethereum Mainnet.

:source_uuid: 75405fc4-d493-410d-b036-dc7f67242ca3
:source_uuid: 8e1a1625-29e0-46d0-ac50-d43b40c4c79d
"""


SPARK_CRV_USDC_USDT_POOL: Final[Asset] = Asset(
    token=Token.CRV_2_POOL,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.CURVE,
    address=EvmAddress("0x4f493B7dE8aAC7d55F71853688b1F7C8F0243C85"),
    underlying_assets=(Token.USDC, Token.USDT),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""CRV_2_POOL via Curve on Ethereum Mainnet.

:source_uuid: a6a50db9-901d-44c4-84d9-cbd581637394
:source_uuid: 5e58a4bf-c0a0-4351-a069-b39b420edb5f
"""


SPARK_CRV_WEETH_WETH_POOL: Final[Asset] = Asset(
    token=Token.CRV_2_POOL,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.CURVE,
    address=EvmAddress("0xDB74dfDD3BB46bE8Ce6C33dC9D82777BCFc3dEd5"),
    underlying_assets=(Token.WETH, Token.WEETH),
    underlying_asset_address=EvmAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    categories={AssetCategory.DIRECT_EXPOSURE},
)
"""CRV_2_POOL via Curve on Ethereum Mainnet.

:source_uuid: 92a9fec3-7b4d-4388-bd05-7d18790bc584
:source_uuid: ad094f82-5619-4558-abd1-e793131e9ec0
"""


SPARK_S_USDS_USDT: Final[Asset] = Asset(
    token=Token.S_USDS_USDT,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.CURVE,
    address=EvmAddress("0x00836Fe54625BE242BcFA286207795405ca4fD10"),
    underlying_assets=(Token.S_USDS, Token.USDT),
    underlying_asset_address=EvmAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"),
    categories={AssetCategory.CASH_STABLECOIN},
)
"""S_USDS_USDT via Curve on Ethereum Mainnet.

:source_uuid: 8ce212dc-4f34-41a5-8621-01edd0ab2ea4
:source_uuid: 2957563b-3948-40b3-a247-15c6ddd41b03
"""


SPARK_S_USDE: Final[Asset] = Asset(
    token=Token.S_USDE,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.ETHENA_PROTOCOL,
    address=EvmAddress("0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"),
    underlying_assets=(Token.USDE,),
    underlying_asset_address=EvmAddress("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""S_USDE via Ethena Protocol on Ethereum Mainnet.

:source_uuid: da72f25e-649c-45b6-bac1-54e7c4f714a5
:source_uuid: 36beeacb-b9c7-4dac-aa1a-db6a69f3af24
"""


SPARK_FS_USDS_ETHEREUM_MAINNET: Final[Asset] = Asset(
    token=Token.FS_USDS,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.FLUID_FINANCE_ERC4626_VAULT,
    address=EvmAddress("0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11"),
    underlying_assets=(Token.S_USDS,),
    underlying_asset_address=EvmAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"),
    categories={AssetCategory.LENDING_MARKET},
)
"""FS_USDS via Fluid Finance Erc4626 Vault on Ethereum Mainnet.

:source_uuid: 1f34b538-6081-4be9-9d69-3ae4bc75200f
:source_uuid: dbe01ca1-3431-402b-a742-48ceb6d710d8
"""


SPARK_SYRUP_USDC: Final[Asset] = Asset(
    token=Token.SYRUP_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MAPLE,
    address=EvmAddress("0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SYRUP_USDC via Maple on Ethereum Mainnet.

:source_uuid: 953bd87a-5781-42f1-b989-f9ab267bc707
:source_uuid: acb94b04-e58a-4948-9a85-aaf6887d8f65
"""


SPARK_SYRUP_USDT: Final[Asset] = Asset(
    token=Token.SYRUP_USDT,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MAPLE,
    address=EvmAddress("0x356B8d89c1e1239Cbbb9dE4815c39A1474d5BA7D"),
    underlying_assets=(Token.USDT,),
    underlying_asset_address=EvmAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SYRUP_USDT via Maple on Ethereum Mainnet.

:source_uuid: 348f78f4-07a2-4e72-8d2a-4a62a2e44bed
:source_uuid: ffaf2d1b-8942-489f-8408-ab0e5718d3c5
"""


SPARK_USDC_BC: Final[Asset] = Asset(
    token=Token.SPARK_USDC_BC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MORPHO,
    address=EvmAddress("0x56A76b428244a50513ec81e225a293d128fd581D"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SPARK_USDC_BC via Morpho on Ethereum Mainnet.

:source_uuid: 711b3b1f-ecf8-42d4-8112-00d032cb4293
:source_uuid: cfad62db-289a-4840-a31b-1ec231c8a1da
"""


SPARK_USDS: Final[Asset] = Asset(
    token=Token.SPARK_USDS,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MORPHO,
    address=EvmAddress("0xe41a0583334f0dc4E023Acd0bFef3667F6FE0597"),
    underlying_assets=(Token.USDS,),
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SPARK_USDS via Morpho on Ethereum Mainnet.

:source_uuid: 972aa481-5c2c-44e0-956e-f649e86f6cc2
:source_uuid: 64e52154-360d-49e5-882c-6ef389b7a2df
"""


SPARK_SP_DAI_MORPHO: Final[Asset] = Asset(
    token=Token.SP_DAI,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.MORPHO,
    address=EvmAddress("0x73e65DBD630f90604062f6E02fAb9138e713edD9"),
    underlying_assets=(Token.DAI,),
    underlying_asset_address=EvmAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_DAI via Morpho on Ethereum Mainnet.

:source_uuid: 1614a57a-15d9-4081-862b-d1b1d80f59f4
:source_uuid: faf749a9-9737-49c8-8783-e09034ab190d
"""


SPARK_SP_DAI_SPARKLEND: Final[Asset] = Asset(
    token=Token.SP_DAI,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARKLEND_PROTOCOL,
    address=EvmAddress("0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B"),
    underlying_assets=(Token.DAI,),
    underlying_asset_address=EvmAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_DAI via Sparklend Protocol on Ethereum Mainnet.

:source_uuid: edbdeb06-4b87-4dd6-960a-cba704f7bf94
:source_uuid: 388c73f9-17cc-4518-9af9-4bc619963172
"""


SPARK_SP_PY_USD: Final[Asset] = Asset(
    token=Token.SP_PY_USD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARKLEND_PROTOCOL,
    address=EvmAddress("0x779224df1c756b4EDD899854F32a53E8c2B2ce5d"),
    underlying_assets=(Token.PYUSD,),
    underlying_asset_address=EvmAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_PY_USD via Sparklend Protocol on Ethereum Mainnet.

:source_uuid: 9730bb57-1bab-44c2-bdfb-805b992d53d0
:source_uuid: af95c4ee-4010-436b-8717-c747f5a46d96
"""


SPARK_SP_USDC_ETHEREUM_MAINNET_SPARKLEND: Final[Asset] = Asset(
    token=Token.SP_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARKLEND_PROTOCOL,
    address=EvmAddress("0x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_USDC via Sparklend Protocol on Ethereum Mainnet.

:source_uuid: de211b16-8d7a-4560-9cb9-52a98941fb43
:source_uuid: c78afa3b-9e4b-4c25-a85f-28492d7729aa
"""


SPARK_SP_USDS: Final[Asset] = Asset(
    token=Token.SP_USDS,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARKLEND_PROTOCOL,
    address=EvmAddress("0xC02aB1A5eaA8d1B114EF786D9bde108cD4364359"),
    underlying_assets=(Token.USDS,),
    underlying_asset_address=EvmAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_USDS via Sparklend Protocol on Ethereum Mainnet.

:source_uuid: a8171359-d11e-4014-bd9b-ef19712e556d
:source_uuid: aeb1bcc7-1214-4544-b686-687d1bb2fa70
"""


SPARK_SP_USDT_SPARKLEND: Final[Asset] = Asset(
    token=Token.SP_USDT,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARKLEND_PROTOCOL,
    address=EvmAddress("0xe7dF13b8e3d6740fe17CBE928C7334243d86c92f"),
    underlying_assets=(Token.USDT,),
    underlying_asset_address=EvmAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_USDT via Sparklend Protocol on Ethereum Mainnet.

:source_uuid: 85a280c2-a45d-4e67-ab6e-7cdcf5746106
:source_uuid: a7f5e722-e39c-4f9c-be0d-c43484cc18ae
"""


SPARK_SP_WETH: Final[Asset] = Asset(
    token=Token.SP_WETH,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARKLEND_PROTOCOL,
    address=EvmAddress("0x59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB"),
    underlying_assets=(Token.WETH,),
    underlying_asset_address=EvmAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_WETH via Sparklend Protocol on Ethereum Mainnet.

:source_uuid: fa19fb59-fb98-4082-826b-649ce7cdc037
:source_uuid: 3267c66e-aefa-48ec-8f76-62e50eddd1b4
"""


SPARK_SP_ETH: Final[Asset] = Asset(
    token=Token.SP_ETH,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARK_SAVINGS_PROTOCOL,
    address=EvmAddress("0xfE6eb3b609a7C8352A241f7F3A21CEA4e9209B8f"),
    underlying_assets=(Token.WETH,),
    underlying_asset_address=EvmAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_ETH via Spark Savings Protocol on Ethereum Mainnet.

:source_uuid: 93f2939e-b2d2-4c2e-b74d-8af8b9fbf12e
:source_uuid: cb300f77-edf2-45eb-8f2a-14d1455a7d1d
"""


SPARK_SP_PYUSD: Final[Asset] = Asset(
    token=Token.SP_PYUSD,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARK_SAVINGS_PROTOCOL,
    address=EvmAddress("0x80128DbB9f07b93DDE62A6daeadb69ED14a7D354"),
    underlying_assets=(Token.PYUSD,),
    underlying_asset_address=EvmAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_PYUSD via Spark Savings Protocol on Ethereum Mainnet.

:source_uuid: 32e9ffdc-e437-46cb-a2fc-272fb3e826a7
:source_uuid: e3bf5dca-f865-45b0-87dd-1bb67b9b52af
"""


SPARK_SP_USDC_ETHEREUM_MAINNET_SPARK_SAVINGS: Final[Asset] = Asset(
    token=Token.SP_USDC,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARK_SAVINGS_PROTOCOL,
    address=EvmAddress("0x28B3a8fb53B741A8Fd78c0fb9A6B2393d896a43d"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_USDC via Spark Savings Protocol on Ethereum Mainnet.

:source_uuid: 60e2171d-0b25-405c-9ca5-627e8049b8b1
:source_uuid: 4065d210-a9ee-4d96-83f0-c4ff4ac09a07
"""


SPARK_SP_USDT_SPARK_SAVINGS: Final[Asset] = Asset(
    token=Token.SP_USDT,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SPARK_SAVINGS_PROTOCOL,
    address=EvmAddress("0xe2e7a17dFf93280dec073C995595155283e3C372"),
    underlying_assets=(Token.USDT,),
    underlying_asset_address=EvmAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
    categories={AssetCategory.LENDING_MARKET},
)
"""SP_USDT via Spark Savings Protocol on Ethereum Mainnet.

:source_uuid: 224538f9-fde7-43f3-aa9b-1c3cf6036663
:source_uuid: 2959cf7c-9026-45d9-83d4-2ef755613d33
"""


SPARK_USTB: Final[Asset] = Asset(
    token=Token.USTB,
    network=Network.ETHEREUM_MAINNET,
    protocol=Protocol.SUPERSTATE,
    address=EvmAddress("0x43415eB6ff9DB7E26A15b704e7A3eDCe97d31C4e"),
    underlying_assets=(Token.USDC,),
    underlying_asset_address=EvmAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    categories={AssetCategory.PERPETUAL_POSITION},
)
"""USTB via Superstate on Ethereum Mainnet.

:source_uuid: 818944d2-c16f-4bd8-af85-09c3a31eccd3
:source_uuid: b4e63a9d-65e7-4c61-826e-fe9733b3f00f
"""
