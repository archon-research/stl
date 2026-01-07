/**
 * Aave Mainnet Schema
 * 
 * Generates all table schemas for Aave V3 Core and Horizon markets on Mainnet.
 * Each table must be a named export for Ponder to properly register it.
 */

import { 
  createAaveSharedEventTables, 
  createAaveCoreEventTables, 
  createAaveHorizonEventTables,
  createAaveStateTables,
  createAaveSnapshotTables,
  createAavePriceTables,
} from "../factory";

// Create table collections
const coreSharedEvents = createAaveSharedEventTables("Mainnet", "Core");
const coreUniqueEvents = createAaveCoreEventTables("Mainnet");
const coreState = createAaveStateTables("Mainnet", "Core");
const coreSnapshots = createAaveSnapshotTables("Mainnet", "Core");
const corePrices = createAavePriceTables("Mainnet", "Core");

const horizonSharedEvents = createAaveSharedEventTables("Mainnet", "Horizon");
const horizonUniqueEvents = createAaveHorizonEventTables("Mainnet");
const horizonState = createAaveStateTables("Mainnet", "Horizon");
const horizonSnapshots = createAaveSnapshotTables("Mainnet", "Horizon");
const horizonPrices = createAavePriceTables("Mainnet", "Horizon");

// ===== CORE MARKET - Named Exports (Required by Ponder) =====

// Core Shared Events
export const AaveMainnetCoreBorrow = coreSharedEvents.Borrow;
export const AaveMainnetCoreDeficitCovered = coreSharedEvents.DeficitCovered;
export const AaveMainnetCoreDeficitCreated = coreSharedEvents.DeficitCreated;
export const AaveMainnetCoreFlashLoan = coreSharedEvents.FlashLoan;
export const AaveMainnetCoreIsolationModeTotalDebtUpdated = coreSharedEvents.IsolationModeTotalDebtUpdated;
export const AaveMainnetCoreLiquidationCall = coreSharedEvents.LiquidationCall;
export const AaveMainnetCoreMintedToTreasury = coreSharedEvents.MintedToTreasury;
export const AaveMainnetCoreRepay = coreSharedEvents.Repay;
export const AaveMainnetCoreReserveDataUpdated = coreSharedEvents.ReserveDataUpdated;
export const AaveMainnetCoreReserveUsedAsCollateralDisabled = coreSharedEvents.ReserveUsedAsCollateralDisabled;
export const AaveMainnetCoreReserveUsedAsCollateralEnabled = coreSharedEvents.ReserveUsedAsCollateralEnabled;
export const AaveMainnetCoreSupply = coreSharedEvents.Supply;
export const AaveMainnetCoreUserEModeSet = coreSharedEvents.UserEModeSet;
export const AaveMainnetCoreWithdraw = coreSharedEvents.Withdraw;

// Core Unique Events
export const AaveMainnetCorePositionManagerApproved = coreUniqueEvents.PositionManagerApproved;
export const AaveMainnetCorePositionManagerRevoked = coreUniqueEvents.PositionManagerRevoked;

// Core State
export const AaveMainnetCoreUserSupplyPosition = coreState.UserSupplyPosition;
export const AaveMainnetCoreUserBorrowPosition = coreState.UserBorrowPosition;
export const AaveMainnetCoreActiveUser = coreState.ActiveUser;

// Core Snapshots
export const AaveMainnetCoreUserScaledSupplyPosition = coreSnapshots.UserScaledSupplyPosition;
export const AaveMainnetCoreUserScaledBorrowPosition = coreSnapshots.UserScaledBorrowPosition;
export const AaveMainnetCoreUserEModeCategory = coreSnapshots.UserEModeCategory;
export const AaveMainnetCoreUserHealthFactorHistory = coreSnapshots.UserHealthFactorHistory;
export const AaveMainnetCoreUserPositionBreakdown = coreSnapshots.UserPositionBreakdown;

// Core Prices
export const AaveMainnetCoreAssetPriceSnapshot = corePrices.AssetPriceSnapshot;

// ===== HORIZON MARKET - Named Exports (Required by Ponder) =====

// Horizon Shared Events
export const AaveMainnetHorizonBorrow = horizonSharedEvents.Borrow;
export const AaveMainnetHorizonDeficitCovered = horizonSharedEvents.DeficitCovered;
export const AaveMainnetHorizonDeficitCreated = horizonSharedEvents.DeficitCreated;
export const AaveMainnetHorizonFlashLoan = horizonSharedEvents.FlashLoan;
export const AaveMainnetHorizonIsolationModeTotalDebtUpdated = horizonSharedEvents.IsolationModeTotalDebtUpdated;
export const AaveMainnetHorizonLiquidationCall = horizonSharedEvents.LiquidationCall;
export const AaveMainnetHorizonMintedToTreasury = horizonSharedEvents.MintedToTreasury;
export const AaveMainnetHorizonRepay = horizonSharedEvents.Repay;
export const AaveMainnetHorizonReserveDataUpdated = horizonSharedEvents.ReserveDataUpdated;
export const AaveMainnetHorizonReserveUsedAsCollateralDisabled = horizonSharedEvents.ReserveUsedAsCollateralDisabled;
export const AaveMainnetHorizonReserveUsedAsCollateralEnabled = horizonSharedEvents.ReserveUsedAsCollateralEnabled;
export const AaveMainnetHorizonSupply = horizonSharedEvents.Supply;
export const AaveMainnetHorizonUserEModeSet = horizonSharedEvents.UserEModeSet;
export const AaveMainnetHorizonWithdraw = horizonSharedEvents.Withdraw;

// Horizon Unique Events
export const AaveMainnetHorizonBackUnbacked = horizonUniqueEvents.BackUnbacked;
export const AaveMainnetHorizonMintUnbacked = horizonUniqueEvents.MintUnbacked;

// Horizon State
export const AaveMainnetHorizonUserSupplyPosition = horizonState.UserSupplyPosition;
export const AaveMainnetHorizonUserBorrowPosition = horizonState.UserBorrowPosition;
export const AaveMainnetHorizonActiveUser = horizonState.ActiveUser;

// Horizon Snapshots
export const AaveMainnetHorizonUserScaledSupplyPosition = horizonSnapshots.UserScaledSupplyPosition;
export const AaveMainnetHorizonUserScaledBorrowPosition = horizonSnapshots.UserScaledBorrowPosition;
export const AaveMainnetHorizonUserEModeCategory = horizonSnapshots.UserEModeCategory;
export const AaveMainnetHorizonUserHealthFactorHistory = horizonSnapshots.UserHealthFactorHistory;
export const AaveMainnetHorizonUserPositionBreakdown = horizonSnapshots.UserPositionBreakdown;

// Horizon Prices
export const AaveMainnetHorizonAssetPriceSnapshot = horizonPrices.AssetPriceSnapshot;

// Legacy grouped exports (for backward compatibility with handlers)
export const AaveMainnetCoreSharedEvents = coreSharedEvents;
export const AaveMainnetCoreUniqueEvents = coreUniqueEvents;
export const AaveMainnetCoreState = coreState;
export const AaveMainnetCoreSnapshots = coreSnapshots;
export const AaveMainnetCorePrices = corePrices;

export const AaveMainnetHorizonSharedEvents = horizonSharedEvents;
export const AaveMainnetHorizonUniqueEvents = horizonUniqueEvents;
export const AaveMainnetHorizonState = horizonState;
export const AaveMainnetHorizonSnapshots = horizonSnapshots;
export const AaveMainnetHorizonPrices = horizonPrices;


