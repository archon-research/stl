from enum import Enum


class Token(Enum):
    USDC = "USDC"
    USDS = "USDS"
    DAI = "DAI"
    USDT = "USDT"
    WBTC = "WBTC"
    A_ARB_USDCN = "aArbUSDCn"
    A_AVAX_USDC = "aAvaxUSDC"
    A_BAS_USDC = "aBasUSDC"
    A_ETH_LIDO_USDS = "aEthLidoUSDS"
    A_ETH_RLUSD = "aEthRLUSD"
    A_ETH_USDC = "aEthUSDC"
    A_ETH_USDE = "aEthUSDe"
    A_ETH_USDS = "aEthUSDS"
    A_ETH_USDT = "aEthUSDT"
    A_HOR_RWA_RLUSD = "aHorRwaRLUSD"
    A_HOR_RWA_USDC = "aHorRwaUSDC"
    ACRDX = "ACRDX"
    AUSD = "AUSD"
    BUIDL_I = "BUIDL-I"
    CRV_2_POOL = "crv2pool"
    GACLO_1 = "GACLO-1"
    GROVE_BBQ_AUSD = "grove-bbqAUSD"
    GROVE_BBQ_PYUSD = "grove-bbqPYUSD"
    GROVE_BBQ_USDC = "grove-bbqUSDC"
    JAAA = "JAAA"
    JTRSY = "JTRSY"
    FS_USDS = "fsUSDS"
    PT_S_USDE = "PT-sUSDe"
    PT_USDE = "PT-USDe"
    PYUSD = "PYUSD"
    RLUSD = "RLUSD"
    SP_ETH = "spETH"
    SP_PYUSD = "spPYUSD"
    SP_USDC = "spUSDC"
    SP_USDS = "spUSDS"
    SP_USDT = "spUSDT"
    SP_WETH = "spwETH"
    SPARK_USDC = "sparkUSDC"
    SPARK_USDC_BC = "sparkUSDCbc"
    SPARK_USDS = "sparkUSDS"
    SP_DAI = "spDai"
    SP_PY_USD = "sppyUSD"
    STAC = "STAC"
    STEAK_USDC = "steakUSDC"
    S_USDE = "sUSDe"
    S_USDS = "sUSDS"
    SYRUP_USDC = "syrupUSDC"
    SYRUP_USDT = "syrupUSDT"
    S_USDS_USDT = "sUSDSUSDT"
    USCC = "USCC"
    USDE = "USDe"
    USDT_0 = "USDT0"
    UNISWAP_AUSD_USDC_POOL = "Uniswap AUSD/USDC Pool"
    USAT = "USAT"
    USTB = "USTB"
    WEETH = "weETH"
    WETH = "WETH"
    # Liquidity-pool LP tokens and vault shares held as positions but absent from
    # the Atlas (see assets_missing_from_atlas.py).
    SPARK_PRIME_USDC_1 = "sparkPrimeUSDC1"
    AUSD_USDC = "AUSDUSDC"
    PYUSD_USDS = "PYUSDUSDS"


CASH_STABLECOINS = {Token.USDC, Token.USDT, Token.PYUSD}
"""Tokens treated as Cash Stablecoins for ASC/DAB calculations.

:source_uuid: 066a4d9f-13ed-4ac3-a55a-df7bf3429649
"""

DIRECT_ETHENA_TOKENS = {Token.USDE, Token.S_USDE}

PENDLE_ETHENA_TOKENS = {Token.PT_USDE, Token.PT_S_USDE}
