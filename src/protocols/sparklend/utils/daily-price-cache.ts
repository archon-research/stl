import type { Context } from "ponder:registry";
import { eq } from "ponder";
import { getAssetsPrices } from "./oracle";
import { getAllReserveAssets } from "./reserve-assets";

/**
 * In-memory cache for daily prices
 * Key format: `${protocolId}-${blockNumber}`
 * Value: Map of reserveId -> price
 */
const dailyPriceCache = new Map<string, Map<string, bigint>>();

/**
 * Maximum cache entries to prevent memory issues
 */
const MAX_CACHE_ENTRIES = 100;

/**
 * Get or fetch prices for all assets at a specific block
 *
 * Priority order:
 * 1. In-memory cache (fastest)
 * 2. Database AssetPriceSnapshot table (no RPC, persisted)
 * 3. RPC call via oracle (fallback, slowest)
 *
 * @param context - Ponder context
 * @param protocolId - Protocol ID (e.g. "sparklend-mainnet")
 * @param assetPriceSnapshotTable - Chain-specific price snapshot table
 * @param reserveDataUpdatedTable - Chain-specific reserve data table
 * @param blockNumber - Block number to fetch prices at
 * @returns Map of reserveId -> price (8 decimals)
 */
export async function getPricesAtBlock(
  context: Context,
  protocolId: string,
  assetPriceSnapshotTable: any,
  reserveDataUpdatedTable: any,
  blockNumber: bigint
): Promise<Map<string, bigint>> {
  const cacheKey = `${protocolId}-${blockNumber}`;

  // 1. Check in-memory cache first (fastest path)
  if (dailyPriceCache.has(cacheKey)) {
    return dailyPriceCache.get(cacheKey)!;
  }

  // 2. Check database for persisted prices (no RPC needed)
  try {
    const dbPrices = await context.db.sql
      .select()
      .from(assetPriceSnapshotTable)
      .where(eq(assetPriceSnapshotTable.blockNumber, blockNumber));

    if (dbPrices.length > 0) {
      // Build price map from DB results (reserveId -> price)
      const priceMap = new Map<string, bigint>();
      for (const row of dbPrices) {
        priceMap.set(row.reserveId, row.priceUSD);
      }

      // Cache in memory for subsequent lookups
      cacheWithEviction(cacheKey, priceMap);
      console.log(
        `📦 Loaded ${priceMap.size} prices from DB for block ${blockNumber}`
      );
      return priceMap;
    }
  } catch (error: any) {
    // DB query failed, fall through to RPC
    console.warn(
      `⚠️ DB price lookup failed for block ${blockNumber}: ${error?.message}`
    );
  }

  // 3. Fall back to RPC if not in DB (for blocks not yet price-captured)
  console.log(
    `⚠️ Prices not in DB for block ${blockNumber}, fetching from RPC...`
  );

  const allAssets = await getAllReserveAssets(context, reserveDataUpdatedTable);
  
  if (allAssets.length === 0) {
    console.warn(`No reserve assets found at block ${blockNumber}`);
    return new Map();
  }

  // Fetch prices via RPC
  const prices = await getAssetsPrices(allAssets, blockNumber);

  // Build price map (reserveId -> price)
  const priceMap = new Map<string, bigint>();
  const chainId = protocolId.split('-')[1] || 'mainnet'; // Extract chain from protocolId
  for (let i = 0; i < allAssets.length; i++) {
    const asset = allAssets[i];
    const price = prices[i];
    if (asset && price !== undefined && price > 0n) {
      const reserveId = `${chainId}-${asset.toLowerCase()}`;
      priceMap.set(reserveId, price);
    }
  }

  // Cache in memory
  cacheWithEviction(cacheKey, priceMap);
  console.log(
    `💰 Fetched ${priceMap.size} prices via RPC for block ${blockNumber}`
  );

  return priceMap;
}

/**
 * Cache prices with automatic eviction when cache is full
 */
function cacheWithEviction(cacheKey: string, priceMap: Map<string, bigint>): void {
  // Evict old entries if cache is too large
  if (dailyPriceCache.size >= MAX_CACHE_ENTRIES) {
    const keys = Array.from(dailyPriceCache.keys());
    const toRemove = Math.floor(MAX_CACHE_ENTRIES / 2);
    for (let i = 0; i < toRemove; i++) {
      const key = keys[i];
      if (key) {
        dailyPriceCache.delete(key);
      }
    }
    console.log(`🗑️  Evicted ${toRemove} price cache entries`);
  }

  dailyPriceCache.set(cacheKey, priceMap);
}

/**
 * Clear the price cache (for testing or reset)
 */
export function clearPriceCache(): void {
  dailyPriceCache.clear();
}

/**
 * Get current cache size (for monitoring)
 */
export function getPriceCacheSize(): number {
  return dailyPriceCache.size;
}
