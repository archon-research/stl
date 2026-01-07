import { createPublicClient, http } from "viem";
import { mainnet } from "viem/chains";
import { AaveV3PoolAbi } from "../abis/AaveV3PoolAbi";

import { SPARKLEND_MAINNET_POOL_ADDRESS } from "@/constants";

const POOL_ADDRESS = SPARKLEND_MAINNET_POOL_ADDRESS;

// Cache for eMode category data
const eModeCache = new Map<number, {
  ltv: number;
  liquidationThreshold: number;
  liquidationBonus: number;
  timestamp: number;
}>();

const CACHE_TTL = 60 * 60 * 1000; // 1 hour

/**
 * Get eMode category data from the Pool contract
 * 
 * Returns:
 * - ltv: Loan-to-value in basis points
 * - liquidationThreshold: Liquidation threshold in basis points
 * - liquidationBonus: Liquidation bonus in basis points
 */
export async function getEModeCategoryData(categoryId: number): Promise<{
  ltv: number;
  liquidationThreshold: number;
  liquidationBonus: number;
}> {
  // Category 0 means no eMode
  if (categoryId === 0) {
    throw new Error("Category 0 is not a valid eMode category");
  }

  // Check cache
  const cached = eModeCache.get(categoryId);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return {
      ltv: cached.ltv,
      liquidationThreshold: cached.liquidationThreshold,
      liquidationBonus: cached.liquidationBonus,
    };
  }

  try {
    const client = createPublicClient({
      chain: mainnet,
      transport: http(process.env.PONDER_RPC_URL_1),
    });

    const eModeData = await client.readContract({
      address: POOL_ADDRESS,
      abi: AaveV3PoolAbi,
      functionName: "getEModeCategoryData",
      args: [categoryId],
    }) as any;

    const result = {
      ltv: Number(eModeData.ltv),
      liquidationThreshold: Number(eModeData.liquidationThreshold),
      liquidationBonus: Number(eModeData.liquidationBonus),
    };

    // Cache the result
    eModeCache.set(categoryId, {
      ...result,
      timestamp: Date.now(),
    });

    return result;
  } catch (error) {
    console.error(`Error fetching eMode category ${categoryId}:`, error);
    throw error;
  }
}

/**
 * Check if an asset belongs to a specific eMode category
 * 
 * In Aave V3, assets have an eMode category ID stored in their reserve configuration.
 * When a user is in eMode, only assets in the same eMode category can be used as collateral
 * with the eMode's liquidation threshold.
 * 
 * @param assetAddress - The asset address to check
 * @param eModeCategoryId - The eMode category ID to check against
 * @param blockNumber - Optional block number for historical queries
 */
export async function isAssetInEModeCategory(
  assetAddress: string,
  eModeCategoryId: number,
  blockNumber?: bigint
): Promise<boolean> {
  if (eModeCategoryId === 0) {
    // No eMode - assets use their reserve thresholds
    return false;
  }

  try {
    const { getReserveConfiguration } = await import("./reserve-config");
    const config = await getReserveConfiguration(assetAddress, blockNumber);
    
    // Check if asset's eMode category matches user's eMode category
    return config.eModeCategory === eModeCategoryId;
  } catch (error) {
    console.error(`Error checking eMode category for asset ${assetAddress}:`, error);
    // Fail safe: assume asset is not in eMode category
    return false;
  }
}

