from enum import Enum, StrEnum


class Protocol(Enum):
    # PSM
    LITE_PSM = "LitePSM"
    PSM3 = "PSM3"
    # Lending
    SPARKLEND = "SparkLend"
    SPARKLEND_PROTOCOL = "SparkLend Protocol"
    SPARK_SAVINGS_PROTOCOL = "Spark Savings Protocol"
    AAVE = "Aave"
    AAVE_V2 = "Aave V2"
    AAVE_CORE = "Aave Core"
    AAVE_CORE_V3 = "Aave Core V3"
    AAVE_PRIME = "Aave Prime"
    AAVE_HORIZON = "Aave Horizon"
    AAVE_V3 = "Aave V3"
    MORPHO = "Morpho"
    MORPHO_BLUE_ERC4626_VAULT = "Morpho Blue erc4626 Vault"
    MAPLE = "Maple"
    FLUID = "Fluid"
    KAMINO = "Kamino"
    DRIFT = "Drift"
    # Pools
    CURVE = "Curve"
    UNISWAP = "Uniswap"
    UNISWAP_V3 = "Uniswap V3"
    GUNI = "GUNI"
    # Real-world asset / structured credit
    CENTRIFUGE = "Centrifuge"
    BLACKROCK = "BlackRock"
    SECURITIZE = "Securitize"
    SUPERSTATE = "Superstate"
    ANCHORAGE = "Anchorage"
    GALAXY = "Galaxy"
    # Stablecoin issuers / protocols
    ETHENA = "Ethena"
    ETHENA_PROTOCOL = "Ethena Protocol"
    AGORA = "Agora"
    AGORA_AUSD = "Agora aUSD"
    RIPPLE = "Ripple"
    ARKIS = "Arkis"
    # Protocols tracked by the STL allocation tracker but not yet formalised in
    # an Atlas instance configuration document (used only by
    # assets_missing_from_atlas.MISSING_FROM_ATLAS_BY_PRIME).
    SKY = "Sky"
    PAYPAL = "Paypal"
    JANUS_HENDERSON = "Janus Henderson"
    FLUID_FINANCE_ERC4626_VAULT = "Fluid Finance erc4626 Vault"
    GROVE_STEAKHOUSE_AUSD_MORPHO_VAULT = "Grove x Steakhouse aUSD Morpho Vault"
    GROVE_STEAKHOUSE_USDC_HIGH_YIELD_VAULT_V2 = "Grove x Steakhouse USDC High Yield Vault V2"
    GROVE_STEAKHOUSE_USDG_MORPHO_VAULT = "Grove x Steakhouse USDG Morpho Vault"
    STEAKHOUSE_PYUSD_MORPHO_VAULT = "Steakhouse pyUSD Morpho Vault"


# PSM (Peg Stability Module)
# L1 PSM: directly on Ethereum mainnet.
L1_PSM_PROTOCOLS = {Protocol.LITE_PSM}

# L2 PSM: cross-chain peg-stability modules.
L2_PSM_PROTOCOLS = {Protocol.PSM3}

# Lending
LENDING_PROTOCOLS = {
    Protocol.SPARKLEND,
    Protocol.AAVE,
    Protocol.MORPHO,
    Protocol.SPARKLEND_PROTOCOL,
    Protocol.SPARK_SAVINGS_PROTOCOL,
    Protocol.AAVE_PRIME,
    Protocol.AAVE_CORE,
    Protocol.AAVE_HORIZON,
}
"""Protocols treated as Lending Protocols for Latent ASC calculations.

Atlas A.3.3.2.2.1.2.1 lists SparkLend, Aave, and Morpho as the eligible
lending protocols for Latent ASC. The remaining members are A.6
instance-configuration name variants of those same protocols (e.g. SparkLend
Protocol, Aave Prime / Core / Horizon), so positions sourced from A.6
instances match the same filter.

:source_uuid: 35ce6b38-9fc1-456e-93da-10ab1468a8bf
"""

POOL_PROTOCOLS = {Protocol.CURVE, Protocol.UNISWAP}

GUNI_PROTOCOLS = {Protocol.GUNI}

GUNI_RESTING_FEE_TIERS = {0.0001, 0.0005}


# STL exporter protocol classifications.
ATOKEN_PROTOCOLS = {
    Protocol.AAVE,
    Protocol.AAVE_V2,
    Protocol.AAVE_V3,
    Protocol.AAVE_CORE,
    Protocol.AAVE_CORE_V3,
    Protocol.AAVE_PRIME,
    Protocol.AAVE_HORIZON,
    Protocol.SPARKLEND,
    Protocol.SPARKLEND_PROTOCOL,
}

ERC4626_PROTOCOLS = {
    Protocol.MORPHO,
    Protocol.MORPHO_BLUE_ERC4626_VAULT,
    Protocol.MAPLE,
    Protocol.FLUID,
    Protocol.FLUID_FINANCE_ERC4626_VAULT,
    Protocol.SPARK_SAVINGS_PROTOCOL,
    Protocol.STEAKHOUSE_PYUSD_MORPHO_VAULT,
    Protocol.GROVE_STEAKHOUSE_AUSD_MORPHO_VAULT,
    Protocol.GROVE_STEAKHOUSE_USDC_HIGH_YIELD_VAULT_V2,
    Protocol.GROVE_STEAKHOUSE_USDG_MORPHO_VAULT,
    Protocol.ARKIS,
}

UNISWAP_STYLE_PROTOCOLS = {Protocol.UNISWAP, Protocol.UNISWAP_V3, Protocol.GUNI}


class AllocationType(StrEnum):
    """STL ``allocation_type`` classification emitted in the exported contract.

    A closed set of the values the legacy allocation tracker used; the enum
    values are exactly those strings so the export remains byte-for-byte
    compatible.
    """

    ALLOCATION = "allocation"
    POL = "pol"
    ASSET = "asset"
    PSM3 = "psm3"
    RISK_CAPITAL = "risk_capital"


class TokenType(StrEnum):
    """STL ``token_type`` classification emitted in the exported contract.

    A closed set of the values the legacy allocation tracker used; the enum
    values are exactly those strings so the export remains byte-for-byte
    compatible.
    """

    ATOKEN = "atoken"
    ERC4626 = "erc4626"
    ERC20 = "erc20"
    CURVE = "curve"
    UNI_V3_POOL = "uni_v3_pool"
    UNI_V3_LP = "uni_v3_lp"
    CENTRIFUGE = "centrifuge"
    CENTRIFUGE_FEEDER = "centrifuge_feeder"
    ANCHORAGE = "anchorage"
    SUPERSTATE = "superstate"
    PSM3 = "psm3"
    PROXY = "proxy"
