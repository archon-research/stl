from enum import StrEnum

from app.risk_engine._vendored_synome.spec.entities.assets import (
    GROVE_A_ETH_RLUSD,
    GROVE_A_ETH_USDC,
    GROVE_A_HOR_RWA_RLUSD,
    GROVE_A_HOR_RWA_USDC,
    GROVE_ACRDX,
    GROVE_AUSD,
    GROVE_BBQ_AUSD,
    GROVE_BBQ_PYUSD,
    GROVE_BBQ_USDC_BASE,
    GROVE_BBQ_USDC_MORPHO,
    GROVE_BBQ_USDC_STEAKHOUSE_V2,
    GROVE_BUIDL_I,
    GROVE_GACLO_1,
    GROVE_JAAA_AVALANCHE,
    GROVE_JAAA_ETHEREUM_MAINNET,
    GROVE_JTRSY_AVALANCHE,
    GROVE_JTRSY_ETHEREUM_MAINNET,
    GROVE_MONAD_UNISWAP_AUSD_USDC,
    GROVE_PT_S_USDE,
    GROVE_PT_USDE,
    GROVE_RLUSD,
    GROVE_S_USDE,
    GROVE_STAC,
    GROVE_STEAK_USDC,
    GROVE_USDT_0,
    OBEX_SYRUP_USDC,
    SPARK_A_ARB_USDCN,
    SPARK_A_AVAX_USDC,
    SPARK_A_BAS_USDC,
    SPARK_A_ETH_LIDO_USDS,
    SPARK_A_ETH_USDC,
    SPARK_A_ETH_USDE,
    SPARK_A_ETH_USDS,
    SPARK_A_ETH_USDT,
    SPARK_BUIDL_I,
    SPARK_CRV_USDC_USDT_POOL,
    SPARK_CRV_WEETH_WETH_POOL,
    SPARK_FS_USDS_ARBITRUM,
    SPARK_FS_USDS_BASE,
    SPARK_FS_USDS_ETHEREUM_MAINNET,
    SPARK_JTRSY,
    SPARK_S_USDE,
    SPARK_S_USDS_USDT,
    SPARK_SP_DAI_MORPHO,
    SPARK_SP_DAI_SPARKLEND,
    SPARK_SP_ETH,
    SPARK_SP_PY_USD,
    SPARK_SP_PYUSD,
    SPARK_SP_USDC_AVALANCHE_SPARK_SAVINGS,
    SPARK_SP_USDC_ETHEREUM_MAINNET_SPARK_SAVINGS,
    SPARK_SP_USDC_ETHEREUM_MAINNET_SPARKLEND,
    SPARK_SP_USDS,
    SPARK_SP_USDT_SPARK_SAVINGS,
    SPARK_SP_USDT_SPARKLEND,
    SPARK_SP_WETH,
    SPARK_SYRUP_USDC,
    SPARK_SYRUP_USDT,
    SPARK_USDC,
    SPARK_USDC_BC,
    SPARK_USDS,
    SPARK_USTB,
    Asset,
)


class PrimeName(StrEnum):
    """Canonical names of the Sky Star Agents (primes).

    Each member's trailing docstring carries the ``:source_uuid:`` of that
    Star's defining Atlas document (the children of A.6.1.1).
    """

    GROVE = "Grove"
    """:source_uuid: 727b0de6-095b-485e-bf9c-02108a364480"""
    KEEL = "Keel"
    """:source_uuid: bc6aed17-2969-4d04-9af6-c7bf3e4497e6"""
    LAUNCH_AGENT_7 = "Launch Agent 7"
    """:source_uuid: d0d77316-0b08-447c-b75a-ae7926b07019"""
    OBEX = "Obex"
    """:source_uuid: f558e673-cbab-4696-8ca1-3af9b90fe5d4"""
    OSERO = "Osero"
    """:source_uuid: eba0dcc7-e135-496f-b866-342deeb91dc4"""
    PATTERN = "Pattern"
    """:source_uuid: dc083d10-74bc-43b6-ab2f-c91efce76e84"""
    SKYBASE = "Skybase"
    """:source_uuid: c88439b5-f456-4e51-8825-42e0ba83546f"""
    SPARK = "Spark"
    """:source_uuid: dee2f5a4-279a-488c-9a9d-9583e3216fbf"""


ASSETS_BY_PRIME: dict[PrimeName, list[Asset]] = {
    PrimeName.GROVE: [
        GROVE_JAAA_AVALANCHE,
        GROVE_JTRSY_AVALANCHE,
        GROVE_BBQ_USDC_BASE,
        GROVE_STEAK_USDC,
        GROVE_A_ETH_USDC,
        GROVE_A_ETH_RLUSD,
        GROVE_A_HOR_RWA_RLUSD,
        GROVE_A_HOR_RWA_USDC,
        GROVE_AUSD,
        GROVE_BUIDL_I,
        GROVE_JAAA_ETHEREUM_MAINNET,
        GROVE_JTRSY_ETHEREUM_MAINNET,
        GROVE_PT_S_USDE,
        GROVE_PT_USDE,
        GROVE_S_USDE,
        GROVE_BBQ_AUSD,
        GROVE_BBQ_USDC_STEAKHOUSE_V2,
        GROVE_BBQ_USDC_MORPHO,
        GROVE_RLUSD,
        GROVE_STAC,
        GROVE_BBQ_PYUSD,
        GROVE_USDT_0,
        GROVE_GACLO_1,
        GROVE_ACRDX,
        GROVE_MONAD_UNISWAP_AUSD_USDC,
    ],
    PrimeName.OBEX: [
        OBEX_SYRUP_USDC,
    ],
    PrimeName.SPARK: [
        SPARK_A_ARB_USDCN,
        SPARK_FS_USDS_ARBITRUM,
        SPARK_A_AVAX_USDC,
        SPARK_SP_USDC_AVALANCHE_SPARK_SAVINGS,
        SPARK_A_BAS_USDC,
        SPARK_FS_USDS_BASE,
        SPARK_USDC,
        SPARK_A_ETH_USDC,
        SPARK_A_ETH_USDE,
        SPARK_A_ETH_USDS,
        SPARK_A_ETH_USDT,
        SPARK_A_ETH_LIDO_USDS,
        SPARK_BUIDL_I,
        SPARK_JTRSY,
        SPARK_CRV_USDC_USDT_POOL,
        SPARK_CRV_WEETH_WETH_POOL,
        SPARK_S_USDS_USDT,
        SPARK_S_USDE,
        SPARK_FS_USDS_ETHEREUM_MAINNET,
        SPARK_SYRUP_USDC,
        SPARK_SYRUP_USDT,
        SPARK_USDC_BC,
        SPARK_USDS,
        SPARK_SP_DAI_MORPHO,
        SPARK_SP_DAI_SPARKLEND,
        SPARK_SP_PY_USD,
        SPARK_SP_USDC_ETHEREUM_MAINNET_SPARKLEND,
        SPARK_SP_USDS,
        SPARK_SP_USDT_SPARKLEND,
        SPARK_SP_WETH,
        SPARK_SP_ETH,
        SPARK_SP_PYUSD,
        SPARK_SP_USDC_ETHEREUM_MAINNET_SPARK_SAVINGS,
        SPARK_SP_USDT_SPARK_SAVINGS,
        SPARK_USTB,
    ],
}
