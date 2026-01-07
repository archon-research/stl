/**
 * Application Constants
 * 
 * Shared constants used across all protocols.
 */

// ===========================
// PRECISION CONSTANTS
// ===========================

export const RAY = 27n; // Ray precision used by Aave (10^27)
export const WAD = 18n; // Wad precision (10^18)
export const SECONDS_PER_YEAR = 31536000n;

// ===========================
// CHAIN CONSTANTS
// ===========================

// Block times for different chains (in seconds)
export const BLOCK_TIMES = {
  mainnet: 12,
  gnosis: 5,
  arbitrum: 0.25,
  optimism: 2,
} as const;

// Blocks per day for different chains
export const BLOCKS_PER_DAY = {
  mainnet: (24 * 60 * 60) / BLOCK_TIMES.mainnet, // 7200 blocks
  gnosis: (24 * 60 * 60) / BLOCK_TIMES.gnosis, // 17280 blocks
  arbitrum: (24 * 60 * 60) / BLOCK_TIMES.arbitrum, // 345600 blocks
  optimism: (24 * 60 * 60) / BLOCK_TIMES.optimism, // 43200 blocks
} as const;

// Chain IDs
export const CHAIN_IDS = {
  mainnet: 1,
  gnosis: 100,
  arbitrum: 42161,
  optimism: 10,
} as const;

// ===========================
// PROTOCOL ADDRESSES
// ===========================

// Aave V3 Pool Addresses
export const AAVE_V3_CORE_POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2" as const;
export const AAVE_V3_HORIZON_POOL_ADDRESS = "0x1E4f2Ab406aa9764ff05a9a8c8bf6B1c8B6F531f" as const;

// Sparklend Pool Addresses
export const SPARKLEND_MAINNET_POOL_ADDRESS = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987" as const;
export const SPARKLEND_GNOSIS_POOL_ADDRESS = "0x2Dae5307c5E3FD1CF5A72Cb6F698f915860607e0" as const;


