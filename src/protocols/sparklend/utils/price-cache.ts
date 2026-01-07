import type { Context } from "ponder:registry";
import { getAssetsPrices } from "./oracle";

/**
 * Get prices from oracle (no caching)
 * Fetches prices directly from the oracle for the given assets and block
 */
export async function getCachedPrices(
  context: Context,
  assets: `0x${string}`[],
  blockNumber: bigint,
  timestamp: bigint
): Promise<Map<string, bigint>> {
  const priceMap = new Map<string, bigint>();

  // Fetch all prices directly from oracle
  try {
    const prices = await getAssetsPrices(assets, blockNumber);
    
    for (let i = 0; i < assets.length; i++) {
      const asset = assets[i];
      if (!asset) continue;
      
      const price = prices[i];
      
      if (price && price > 0n) {
        priceMap.set(asset.toLowerCase(), price);
      }
    }
  } catch (error) {
    console.error(`❌ Failed to fetch prices at block ${blockNumber}:`, error);
    throw error;
  }

  return priceMap;
}
