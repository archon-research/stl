import { createPublicClient, http } from "viem";
import { mainnet } from "viem/chains";
import { AaveV3PoolAbi } from "../abis/AaveV3PoolAbi";

import { SPARKLEND_MAINNET_POOL_ADDRESS } from "@/constants";

const POOL_ADDRESS = SPARKLEND_MAINNET_POOL_ADDRESS;

/**
 * Get full reserve configuration including liquidation threshold
 * 
 * Returns decoded configuration parameters from the reserve's bitmap
 * @param assetAddress - The asset address
 * @param blockNumber - Optional block number for historical queries
 */
export async function getReserveConfiguration(
  assetAddress: string,
  blockNumber?: bigint
): Promise<{
  ltv: number;
  liquidationThreshold: number;
  liquidationBonus: number;
  decimals: number;
  isActive: boolean;
  isFrozen: boolean;
  borrowingEnabled: boolean;
  stableBorrowingEnabled: boolean;
  isPaused: boolean;
  eModeCategory: number; // eMode category ID (0 = no eMode)
}> {
  try {
    const client = createPublicClient({
      chain: mainnet,
      transport: http(process.env.PONDER_RPC_URL_1),
    });

    // Read reserve data (includes configuration) at specific block
    const reserveData = await client.readContract({
      address: POOL_ADDRESS,
      abi: AaveV3PoolAbi,
      functionName: "getReserveData",
      args: [assetAddress as `0x${string}`],
      blockNumber, // Fetch config at historical block
    }) as any;

    // Extract values from configuration bitmap
    // Bit positions from Aave V3 ReserveConfiguration.sol
    const configData = BigInt(reserveData.configuration.data);
    
    const ltv = Number(configData & BigInt(0xFFFF)); // Bits 0-15
    const liquidationThreshold = Number((configData >> 16n) & BigInt(0xFFFF)); // Bits 16-31
    const liquidationBonus = Number((configData >> 32n) & BigInt(0xFFFF)); // Bits 32-47
    const decimals = Number((configData >> 48n) & BigInt(0xFF)); // Bits 48-55
    const isActive = Boolean((configData >> 56n) & 1n); // Bit 56
    const isFrozen = Boolean((configData >> 57n) & 1n); // Bit 57
    const borrowingEnabled = Boolean((configData >> 58n) & 1n); // Bit 58
    const stableBorrowingEnabled = Boolean((configData >> 59n) & 1n); // Bit 59
    const isPaused = Boolean((configData >> 60n) & 1n); // Bit 60
    const eModeCategory = Number((configData >> 168n) & BigInt(0xFF)); // Bits 168-175 (8 bits)

    return {
      ltv,
      liquidationThreshold,
      liquidationBonus,
      decimals,
      isActive,
      isFrozen,
      borrowingEnabled,
      stableBorrowingEnabled,
      isPaused,
      eModeCategory,
    };
  } catch (error) {
    console.error(`Error reading reserve configuration for ${assetAddress}:`, error);
    throw error;
  }
}

/**
 * Check if an asset is enabled as collateral by reading its config from the Pool contract
 * 
 * Returns true if ALL of these conditions are met:
 * - LTV >= MIN_LTV_BPS (default 5%)
 * - Reserve is NOT frozen
 * - Reserve is NOT paused
 * 
 * LTV is stored in basis points: 10000 = 100%, 8000 = 80%, 500 = 5%
 */

