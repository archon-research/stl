/**
 * Sparklend Gnosis Schema
 *
 * Chain-specific tables for Gnosis Chain (chain ID: 100)
 */

import {
  createSparklendEventTables,
  createSparklendStateTables,
  createSparklendSnapshotTables,
  createSparklendPriceTables,
} from "../factory";

// Create all tables for Gnosis
export const gnosisEvents = createSparklendEventTables("Gnosis");
export const gnosisState = createSparklendStateTables("Gnosis");
export const gnosisSnapshots = createSparklendSnapshotTables("Gnosis");
export const gnosisPrices = createSparklendPriceTables("Gnosis");

// Re-export with Sparklend prefix for consistency
export const SparklendGnosisBackUnbacked = gnosisEvents.BackUnbacked;
export const SparklendGnosisBorrow = gnosisEvents.Borrow;
export const SparklendGnosisFlashLoan = gnosisEvents.FlashLoan;
export const SparklendGnosisIsolationModeTotalDebtUpdated = gnosisEvents.IsolationModeTotalDebtUpdated;
export const SparklendGnosisLiquidationCall = gnosisEvents.LiquidationCall;
export const SparklendGnosisMintUnbacked = gnosisEvents.MintUnbacked;
export const SparklendGnosisMintedToTreasury = gnosisEvents.MintedToTreasury;
export const SparklendGnosisRepay = gnosisEvents.Repay;
export const SparklendGnosisReserveDataUpdated = gnosisEvents.ReserveDataUpdated;
export const SparklendGnosisReserveUsedAsCollateralDisabled = gnosisEvents.ReserveUsedAsCollateralDisabled;
export const SparklendGnosisReserveUsedAsCollateralEnabled = gnosisEvents.ReserveUsedAsCollateralEnabled;
export const SparklendGnosisSupply = gnosisEvents.Supply;
export const SparklendGnosisSwapBorrowRateMode = gnosisEvents.SwapBorrowRateMode;
export const SparklendGnosisUserEModeSet = gnosisEvents.UserEModeSet;
export const SparklendGnosisWithdraw = gnosisEvents.Withdraw;

export const SparklendGnosisUserSupplyPosition = gnosisState.UserSupplyPosition;
export const SparklendGnosisUserBorrowPosition = gnosisState.UserBorrowPosition;
export const SparklendGnosisActiveUser = gnosisState.ActiveUser;

export const SparklendGnosisUserScaledSupplyPosition = gnosisSnapshots.UserScaledSupplyPosition;
export const SparklendGnosisUserScaledBorrowPosition = gnosisSnapshots.UserScaledBorrowPosition;
export const SparklendGnosisUserEModeCategory = gnosisSnapshots.UserEModeCategory;
export const SparklendGnosisUserHealthFactorHistory = gnosisSnapshots.UserHealthFactorHistory;
export const SparklendGnosisUserPositionBreakdown = gnosisSnapshots.UserPositionBreakdown;

export const SparklendGnosisAssetPriceSnapshot = gnosisPrices.AssetPriceSnapshot;

