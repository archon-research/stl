import { createPublicClient, http } from "viem";
import { mainnet } from "viem/chains";
import { AaveV3PoolAbi } from "../abis/AaveV3PoolAbi";
import { PoolAddressesProviderAbi } from "../abis/PoolAddressesProviderAbi";
import { AaveOracleAbi } from "../abis/AaveOracleAbi";

import { SPARKLEND_MAINNET_POOL_ADDRESS } from "@/constants";

const POOL_ADDRESS = SPARKLEND_MAINNET_POOL_ADDRESS;

// Cache for oracle address (rarely changes)
let cachedOracleAddress: `0x${string}` | null = null;

// Stablecoins that always return $1 (8 decimals) - skip RPC calls for these
const STABLECOIN_ADDRESSES = new Set([
  "0x6b175474e89094c44da98b954eedeac495271d0f", // DAI
  "0x83f20f44975d03b1b09e64809b757c47f942beea", // sDAI
  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
  "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
]);

const ONE_USD = 100000000n; // $1 with 8 decimals

/**
 * Check if an asset is a stablecoin (always $1)
 */
function isStablecoin(address: `0x${string}`): boolean {
  return STABLECOIN_ADDRESSES.has(address.toLowerCase());
}

/**
 * Get the PoolAddressesProvider address from the Pool contract
 */
export async function getPoolAddressesProvider(): Promise<`0x${string}`> {
  const client = createPublicClient({
    chain: mainnet,
    transport: http(process.env.PONDER_RPC_URL_1),
  });

  const addressesProvider = await client.readContract({
    address: POOL_ADDRESS,
    abi: AaveV3PoolAbi,
    functionName: "ADDRESSES_PROVIDER",
  });

  return addressesProvider as `0x${string}`;
}

/**
 * Get the Price Oracle address from the PoolAddressesProvider
 */
export async function getPriceOracleAddress(): Promise<`0x${string}`> {
  // Return cached if available
  if (cachedOracleAddress) {
    return cachedOracleAddress;
  }

  const client = createPublicClient({
    chain: mainnet,
    transport: http(process.env.PONDER_RPC_URL_1),
  });

  // Get PoolAddressesProvider address
  const addressesProvider = await getPoolAddressesProvider();

  // Get Price Oracle address
  const oracleAddress = await client.readContract({
    address: addressesProvider,
    abi: PoolAddressesProviderAbi,
    functionName: "getPriceOracle",
  });

  cachedOracleAddress = oracleAddress as `0x${string}`;
  return cachedOracleAddress;
}

/**
 * Get the price of an asset from the Aave Oracle
 *
 * @param assetAddress - The address of the asset to get the price for
 * @param blockNumber - Optional block number for historical price queries
 * @returns The price in USD (typically 8 decimals)
 */
export async function getAssetPrice(
  assetAddress: `0x${string}`,
  blockNumber?: bigint
): Promise<bigint> {
  // Stablecoins always return $1 - skip RPC call
  if (isStablecoin(assetAddress)) {
    return ONE_USD;
  }

  const client = createPublicClient({
    chain: mainnet,
    transport: http(process.env.PONDER_RPC_URL_1),
  });

  const oracleAddress = await getPriceOracleAddress();

  const price = await client.readContract({
    address: oracleAddress,
    abi: AaveOracleAbi,
    functionName: "getAssetPrice",
    args: [assetAddress],
    blockNumber, // Query price at specific block
  });

  return price as bigint;
}

/**
 * Get prices for multiple assets in a single call (more efficient)
 * Falls back to individual calls if batch fails (e.g., early blocks missing some assets)
 *
 * @param assetAddresses - Array of asset addresses
 * @param blockNumber - Optional block number for historical price queries
 * @returns Array of prices in USD (typically 8 decimals), 0n for unavailable assets
 */
export async function getAssetsPrices(
  assetAddresses: `0x${string}`[],
  blockNumber?: bigint
): Promise<bigint[]> {
  const client = createPublicClient({
    chain: mainnet,
    transport: http(process.env.PONDER_RPC_URL_1),
  });

  const oracleAddress = await getPriceOracleAddress();

  try {
    // Try batch call first (most efficient)
    const prices = await client.readContract({
      address: oracleAddress,
      abi: AaveOracleAbi,
      functionName: "getAssetsPrices",
      args: [assetAddresses],
      blockNumber,
    });

    return prices as bigint[];
  } catch (batchError) {
    // Batch failed - likely some assets not listed at this block
    // Fall back to individual price fetches
    console.log(`  ⚠️  Batch price fetch failed at block ${blockNumber}, falling back to individual fetches...`);

    const prices: bigint[] = [];
    let stablecoinCount = 0;

    for (const asset of assetAddresses) {
      // Stablecoins always return $1 - skip RPC call
      if (isStablecoin(asset)) {
        prices.push(ONE_USD);
        stablecoinCount++;
        continue;
      }

      try {
        const price = await client.readContract({
          address: oracleAddress,
          abi: AaveOracleAbi,
          functionName: "getAssetPrice",
          args: [asset],
          blockNumber,
        });
        prices.push(price as bigint);
      } catch (individualError) {
        // Asset not listed at this block - use 0
        prices.push(0n);
      }
    }

    const validPrices = prices.filter(p => p > 0n).length;
    console.log(`  ✓ Fetched ${validPrices}/${assetAddresses.length} prices (${stablecoinCount} stablecoins skipped)`);

    return prices;
  }
}

