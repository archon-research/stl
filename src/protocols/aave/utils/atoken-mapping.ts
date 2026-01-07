/**
 * AToken to Underlying Asset Mapping
 * 
 * Maps Aave V3 aToken addresses to their underlying asset addresses.
 * This is needed for the transfer tracker to properly attribute transfers
 * to the correct reserve positions.
 * 
 * TODO: Populate this mapping once aToken addresses are added to contracts.ts
 * Can be discovered via:
 * 1. ReserveInitialized events from the Pool contract
 * 2. Aave V3 subgraph queries
 * 3. Direct contract calls to Pool.getReserveData(asset).aTokenAddress
 */

// Core Market AToken Mappings
const CORE_ATOKEN_TO_UNDERLYING: Record<string, `0x${string}`> = {
  // TODO: Add Core market aToken mappings
  // Example:
  // "0xaTokenAddress": "0xUnderlyingAssetAddress",
};

// Horizon Market AToken Mappings
const HORIZON_ATOKEN_TO_UNDERLYING: Record<string, `0x${string}`> = {
  // TODO: Add Horizon market aToken mappings
  // Example:
  // "0xaTokenAddress": "0xUnderlyingAssetAddress",
};

/**
 * Get the underlying asset address from an aToken address
 */
export function getUnderlyingAsset(
  aTokenAddress: `0x${string}`,
  market: "core" | "horizon" = "core"
): `0x${string}` | undefined {
  const normalized = aTokenAddress.toLowerCase();
  const mapping = market === "core" ? CORE_ATOKEN_TO_UNDERLYING : HORIZON_ATOKEN_TO_UNDERLYING;
  return mapping[normalized];
}

