/**
 * Mapping from Sparklend spToken (aToken) addresses to their underlying asset addresses
 *
 * spTokens are the receipt tokens users receive when supplying collateral.
 * When users transfer spTokens directly (ERC20 Transfer), we need to update
 * their positions in the indexer.
 */

// Map spToken addresses (lowercase) to their underlying asset addresses
// Addresses retrieved from Pool.getReserveData() on mainnet
export const SPTOKEN_TO_UNDERLYING: Record<`0x${string}`, `0x${string}`> = {
  // spWETH -> WETH
  "0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db":
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
  // spwstETH -> wstETH
  "0x12b54025c112aa61face2cdb7118740875a566e9":
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
  // spsDAI -> sDAI
  "0x78f897f0fe2d3b5690ebae7f19862deacedf10a7":
    "0x83f20f44975d03b1b09e64809b757c47f942beea",
  // spUSDC -> USDC
  "0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815":
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
  // spUSDT -> USDT
  "0xe7df13b8e3d6740fe17cbe928c7334243d86c92f":
    "0xdac17f958d2ee523a2206206994597c13d831ec7",
  // sprETH -> rETH
  "0x9985df20d7e9103ecbceb16a84956434b6f06ae8":
    "0xae78736cd615f374d3085123a210448e74fc6393",
  // spGNO -> GNO
  "0x7b481acc9fdaddc9af2cbea1ff2342cb1733e50f":
    "0x6810e776880c02933d47db1b9fc05908e5386b96",
  // spWBTC -> WBTC
  "0x4197ba364ae6698015ae5c1468f54087602715b2":
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
};

/**
 * Get underlying asset address from spToken address
 * @param spTokenAddress - The spToken (aToken) contract address
 * @returns The underlying asset address, or null if unknown
 */
export function getUnderlyingAsset(
  spTokenAddress: `0x${string}`
): `0x${string}` | null {
  const normalized = spTokenAddress.toLowerCase() as `0x${string}`;
  return SPTOKEN_TO_UNDERLYING[normalized] || null;
}

/**
 * List of all spToken addresses for ponder.config.ts
 * Addresses retrieved from Pool.getReserveData() on mainnet
 */
export const SP_TOKEN_ADDRESSES: `0x${string}`[] = [
  "0x59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB", // spWETH
  "0x12B54025C112Aa61fAce2CDB7118740875A566E9", // spwstETH
  "0x78f897F0fE2d3B5690EbAe7f19862DEacedF10a7", // spsDAI
  "0x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815", // spUSDC
  "0xe7dF13b8e3d6740fe17CBE928C7334243d86c92f", // spUSDT
  "0x9985dF20D7e9103ECBCeb16a84956434B6f06ae8", // sprETH
  "0x7b481aCC9fDADDc9af2cBEA1Ff2342CB1733E50F", // spGNO
  "0x4197ba364AE6698015AE5c1468f54087602715b2", // spWBTC
];
