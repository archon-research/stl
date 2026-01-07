/**
 * Sparklend Mainnet Schema
 *
 * Chain-specific tables for Ethereum Mainnet (chain ID: 1)
 */

import {
  createSparklendEventTables,
  createSparklendStateTables,
  createSparklendSnapshotTables,
  createSparklendPriceTables,
} from "../factory";

// Create all tables for Mainnet
export const mainnetEvents = createSparklendEventTables("Mainnet");
export const mainnetState = createSparklendStateTables("Mainnet");
export const mainnetSnapshots = createSparklendSnapshotTables("Mainnet");
export const mainnetPrices = createSparklendPriceTables("Mainnet");

// Re-export with Sparklend prefix for consistency
export const SparklendMainnetBackUnbacked = mainnetEvents.BackUnbacked;
export const SparklendMainnetBorrow = mainnetEvents.Borrow;
export const SparklendMainnetFlashLoan = mainnetEvents.FlashLoan;
export const SparklendMainnetIsolationModeTotalDebtUpdated = mainnetEvents.IsolationModeTotalDebtUpdated;
export const SparklendMainnetLiquidationCall = mainnetEvents.LiquidationCall;
export const SparklendMainnetMintUnbacked = mainnetEvents.MintUnbacked;
export const SparklendMainnetMintedToTreasury = mainnetEvents.MintedToTreasury;
export const SparklendMainnetRepay = mainnetEvents.Repay;
export const SparklendMainnetReserveDataUpdated = mainnetEvents.ReserveDataUpdated;
export const SparklendMainnetReserveUsedAsCollateralDisabled = mainnetEvents.ReserveUsedAsCollateralDisabled;
export const SparklendMainnetReserveUsedAsCollateralEnabled = mainnetEvents.ReserveUsedAsCollateralEnabled;
export const SparklendMainnetSupply = mainnetEvents.Supply;
export const SparklendMainnetSwapBorrowRateMode = mainnetEvents.SwapBorrowRateMode;
export const SparklendMainnetUserEModeSet = mainnetEvents.UserEModeSet;
export const SparklendMainnetWithdraw = mainnetEvents.Withdraw;

export const SparklendMainnetUserSupplyPosition = mainnetState.UserSupplyPosition;
export const SparklendMainnetUserBorrowPosition = mainnetState.UserBorrowPosition;
export const SparklendMainnetActiveUser = mainnetState.ActiveUser;

export const SparklendMainnetUserScaledSupplyPosition = mainnetSnapshots.UserScaledSupplyPosition;
export const SparklendMainnetUserScaledBorrowPosition = mainnetSnapshots.UserScaledBorrowPosition;
export const SparklendMainnetUserEModeCategory = mainnetSnapshots.UserEModeCategory;
export const SparklendMainnetUserHealthFactorHistory = mainnetSnapshots.UserHealthFactorHistory;
export const SparklendMainnetUserPositionBreakdown = mainnetSnapshots.UserPositionBreakdown;

export const SparklendMainnetAssetPriceSnapshot = mainnetPrices.AssetPriceSnapshot;

