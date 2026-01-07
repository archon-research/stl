/**
 * Aave Mainnet Schema
 * 
 * Generates all table schemas for Aave V3 Core and Horizon markets on Mainnet.
 */

import { 
  createAaveSharedEventTables, 
  createAaveCoreEventTables, 
  createAaveHorizonEventTables,
  createAaveStateTables,
  createAaveSnapshotTables,
  createAavePriceTables,
} from "../factory";

// Core Market Tables
export const AaveMainnetCoreSharedEvents = createAaveSharedEventTables("Mainnet", "Core");
export const AaveMainnetCoreUniqueEvents = createAaveCoreEventTables("Mainnet");
export const AaveMainnetCoreState = createAaveStateTables("Mainnet", "Core");
export const AaveMainnetCoreSnapshots = createAaveSnapshotTables("Mainnet", "Core");
export const AaveMainnetCorePrices = createAavePriceTables("Mainnet", "Core");

// Horizon Market Tables
export const AaveMainnetHorizonSharedEvents = createAaveSharedEventTables("Mainnet", "Horizon");
export const AaveMainnetHorizonUniqueEvents = createAaveHorizonEventTables("Mainnet");
export const AaveMainnetHorizonState = createAaveStateTables("Mainnet", "Horizon");
export const AaveMainnetHorizonSnapshots = createAaveSnapshotTables("Mainnet", "Horizon");
export const AaveMainnetHorizonPrices = createAavePriceTables("Mainnet", "Horizon");


