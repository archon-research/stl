/**
 * Aave Protocol Configuration
 * 
 * Aggregates all configuration for the Aave protocol module.
 */

export * from "./contracts";
export * from "./blocks";

// Market identifiers
export const AAVE_MARKETS = {
  CORE: "Core",
  HORIZON: "Horizon",
} as const;

export type AaveMarket = typeof AAVE_MARKETS[keyof typeof AAVE_MARKETS];


