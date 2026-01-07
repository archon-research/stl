import type { Context } from "ponder:registry";
import { eq, desc, and, lte } from "ponder";

const RAY = 10n ** 27n;

/**
 * Position Tracker - Multi-chain support
 * All functions accept chainId and chain-specific schema tables
 */

/**
 * Get the latest reserve data update at or before a specific block
 */
async function getReserveDataAtBlock(
  context: Context,
  reserveDataUpdatedTable: any,
  reserveId: string,
  blockNumber: bigint
) {
  const updates = await context.db.sql
    .select()
    .from(reserveDataUpdatedTable)
    .where(
      and(
        eq(reserveDataUpdatedTable.reserveId, reserveId),
        lte(reserveDataUpdatedTable.blockNumber, blockNumber)
      )
    )
    .orderBy(desc(reserveDataUpdatedTable.blockNumber))
    .limit(1);

  if (updates.length === 0 || !updates[0]) {
    throw new Error(`No reserve data found for ${reserveId} at or before block ${blockNumber}`);
  }

  return updates[0];
}

/**
 * Get previous supply snapshot for a user-reserveId
 */
async function getPreviousSupplySnapshot(
  context: Context,
  userScaledSupplyPositionTable: any,
  user: string,
  reserveId: string,
  beforeBlock: bigint
) {
  const snapshots = await context.db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(
      and(
        eq(userScaledSupplyPositionTable.user, user as `0x${string}`),
        eq(userScaledSupplyPositionTable.reserveId, reserveId),
        lte(userScaledSupplyPositionTable.blockNumber, beforeBlock)
      )
    )
    .orderBy(desc(userScaledSupplyPositionTable.blockNumber))
    .limit(1);

  return snapshots[0] || null;
}

/**
 * Get previous borrow snapshot for a user-reserveId
 */
async function getPreviousBorrowSnapshot(
  context: Context,
  userScaledBorrowPositionTable: any,
  user: string,
  reserveId: string,
  beforeBlock: bigint
) {
  const snapshots = await context.db.sql
    .select()
    .from(userScaledBorrowPositionTable)
    .where(
      and(
        eq(userScaledBorrowPositionTable.user, user as `0x${string}`),
        eq(userScaledBorrowPositionTable.reserveId, reserveId),
        lte(userScaledBorrowPositionTable.blockNumber, beforeBlock)
      )
    )
    .orderBy(desc(userScaledBorrowPositionTable.blockNumber))
    .limit(1);

  return snapshots[0] || null;
}

/**
 * Track scaled supply balance
 */
export async function trackScaledSupply(
  context: Context,
  chainId: string,
  userScaledSupplyPositionTable: any,
  reserveDataUpdatedTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: string,
  amount: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();

  const reserveData = await getReserveDataAtBlock(context, reserveDataUpdatedTable, reserveId, blockNumber);
  const currentIndex = reserveData.liquidityIndex;
  const scaledAmount = (amount * RAY) / currentIndex;
  const snapshotId = `${protocolId}-${normalizedUser}-${reserveId}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(eq(userScaledSupplyPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    const newScaledBalance = currentSnapshot[0].scaledBalance + scaledAmount;
    await db.sql
      .update(userScaledSupplyPositionTable)
      .set({
        scaledBalance: newScaledBalance,
        lastLiquidityIndex: currentIndex,
      })
      .where(eq(userScaledSupplyPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousSupplySnapshot(context, userScaledSupplyPositionTable, normalizedUser, reserveId, blockNumber - 1n);
    const newScaledBalance = prevSnapshot ? prevSnapshot.scaledBalance + scaledAmount : scaledAmount;
    const isCollateral = prevSnapshot ? prevSnapshot.isCollateral : true;

    await db.insert(userScaledSupplyPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      scaledBalance: newScaledBalance,
      isCollateral,
      lastLiquidityIndex: currentIndex,
    });
  }
}

/**
 * Track scaled withdraw
 */
export async function trackScaledWithdraw(
  context: Context,
  chainId: string,
  userScaledSupplyPositionTable: any,
  reserveDataUpdatedTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: string,
  amount: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();

  const reserveData = await getReserveDataAtBlock(context, reserveDataUpdatedTable, reserveId, blockNumber);
  const currentIndex = reserveData.liquidityIndex;
  const scaledAmount = (amount * RAY) / currentIndex;
  const snapshotId = `${protocolId}-${normalizedUser}-${reserveId}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(eq(userScaledSupplyPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    const newScaledBalance = currentSnapshot[0].scaledBalance - scaledAmount;
    await db.sql
      .update(userScaledSupplyPositionTable)
      .set({
        scaledBalance: newScaledBalance >= 0n ? newScaledBalance : 0n,
        lastLiquidityIndex: currentIndex,
      })
      .where(eq(userScaledSupplyPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousSupplySnapshot(context, userScaledSupplyPositionTable, normalizedUser, reserveId, blockNumber);
    if (!prevSnapshot) {
      console.warn(`⚠️  No previous supply snapshot found for ${normalizedUser}-${reserveId} at withdraw`);
      await db.insert(userScaledSupplyPositionTable).values({
        id: snapshotId,
        protocolId,
        reserveId,
        user: normalizedUser as `0x${string}`,
        blockNumber,
        timestamp,
        scaledBalance: 0n,
        isCollateral: false,
        lastLiquidityIndex: currentIndex,
      });
      return;
    }

    const newScaledBalance = prevSnapshot.scaledBalance - scaledAmount;
    await db.insert(userScaledSupplyPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      scaledBalance: newScaledBalance >= 0n ? newScaledBalance : 0n,
      isCollateral: prevSnapshot.isCollateral,
      lastLiquidityIndex: currentIndex,
    });
  }
}

/**
 * Track scaled borrow
 */
export async function trackScaledBorrow(
  context: Context,
  chainId: string,
  userScaledBorrowPositionTable: any,
  reserveDataUpdatedTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: string,
  amount: bigint,
  interestRateMode: number,
  blockNumber: bigint,
  timestamp: bigint
) {
  if (interestRateMode !== 2) return;

  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();

  const reserveData = await getReserveDataAtBlock(context, reserveDataUpdatedTable, reserveId, blockNumber);
  const currentIndex = reserveData.variableBorrowIndex;
  const scaledDebt = (amount * RAY) / currentIndex;
  const snapshotId = `${protocolId}-${normalizedUser}-${reserveId}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledBorrowPositionTable)
    .where(eq(userScaledBorrowPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    const newScaledDebt = currentSnapshot[0].scaledVariableDebt + scaledDebt;
    await db.sql
      .update(userScaledBorrowPositionTable)
      .set({
        scaledVariableDebt: newScaledDebt,
        lastVariableBorrowIndex: currentIndex,
      })
      .where(eq(userScaledBorrowPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousBorrowSnapshot(context, userScaledBorrowPositionTable, normalizedUser, reserveId, blockNumber - 1n);
    const newScaledDebt = prevSnapshot ? prevSnapshot.scaledVariableDebt + scaledDebt : scaledDebt;

    await db.insert(userScaledBorrowPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      scaledVariableDebt: newScaledDebt,
      lastVariableBorrowIndex: currentIndex,
    });
  }
}

/**
 * Track scaled repay
 */
export async function trackScaledRepay(
  context: Context,
  chainId: string,
  userScaledBorrowPositionTable: any,
  reserveDataUpdatedTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: string,
  amount: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();

  const reserveData = await getReserveDataAtBlock(context, reserveDataUpdatedTable, reserveId, blockNumber);
  const currentIndex = reserveData.variableBorrowIndex;
  const scaledDebt = (amount * RAY) / currentIndex;
  const snapshotId = `${protocolId}-${normalizedUser}-${reserveId}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledBorrowPositionTable)
    .where(eq(userScaledBorrowPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    const newScaledDebt = currentSnapshot[0].scaledVariableDebt - scaledDebt;
    await db.sql
      .update(userScaledBorrowPositionTable)
      .set({
        scaledVariableDebt: newScaledDebt >= 0n ? newScaledDebt : 0n,
        lastVariableBorrowIndex: currentIndex,
      })
      .where(eq(userScaledBorrowPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousBorrowSnapshot(context, userScaledBorrowPositionTable, normalizedUser, reserveId, blockNumber);
    if (!prevSnapshot) {
      console.warn(`⚠️  No previous borrow snapshot found for ${normalizedUser}-${reserveId} at repay`);
      await db.insert(userScaledBorrowPositionTable).values({
        id: snapshotId,
        protocolId,
        reserveId,
        user: normalizedUser as `0x${string}`,
        blockNumber,
        timestamp,
        scaledVariableDebt: 0n,
        lastVariableBorrowIndex: currentIndex,
      });
      return;
    }

    const newScaledDebt = prevSnapshot.scaledVariableDebt - scaledDebt;
    await db.insert(userScaledBorrowPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      scaledVariableDebt: newScaledDebt >= 0n ? newScaledDebt : 0n,
      lastVariableBorrowIndex: currentIndex,
    });
  }
}

/**
 * Track collateral enabled
 */
export async function trackCollateralEnabled(
  context: Context,
  chainId: string,
  userScaledSupplyPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: string,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();
  const snapshotId = `${protocolId}-${normalizedUser}-${reserveId}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(eq(userScaledSupplyPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    await db.sql
      .update(userScaledSupplyPositionTable)
      .set({
        isCollateral: true,
      })
      .where(eq(userScaledSupplyPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousSupplySnapshot(context, userScaledSupplyPositionTable, normalizedUser, reserveId, blockNumber);
    if (!prevSnapshot) {
      return;
    }

    await db.insert(userScaledSupplyPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      scaledBalance: prevSnapshot.scaledBalance,
      isCollateral: true,
      lastLiquidityIndex: prevSnapshot.lastLiquidityIndex,
    });
  }
}

/**
 * Track collateral disabled
 */
export async function trackCollateralDisabled(
  context: Context,
  chainId: string,
  userScaledSupplyPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: string,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();
  const snapshotId = `${protocolId}-${normalizedUser}-${reserveId}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(eq(userScaledSupplyPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    await db.sql
      .update(userScaledSupplyPositionTable)
      .set({
        isCollateral: false,
      })
      .where(eq(userScaledSupplyPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousSupplySnapshot(context, userScaledSupplyPositionTable, normalizedUser, reserveId, blockNumber);
    if (!prevSnapshot) {
      return;
    }

    await db.insert(userScaledSupplyPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      scaledBalance: prevSnapshot.scaledBalance,
      isCollateral: false,
      lastLiquidityIndex: prevSnapshot.lastLiquidityIndex,
    });
  }
}

/**
 * Track eMode category change
 */
export async function trackEModeSet(
  context: Context,
  chainId: string,
  userEModeCategoryTable: any,
  protocolId: string,
  userAddress: string,
  categoryId: number,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase();
  const snapshotId = `${protocolId}-${normalizedUser}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userEModeCategoryTable)
    .where(eq(userEModeCategoryTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0) {
    await db.sql
      .update(userEModeCategoryTable)
      .set({
        categoryId,
        timestamp,
      })
      .where(eq(userEModeCategoryTable.id, snapshotId));
  } else {
    await db.insert(userEModeCategoryTable).values({
      id: snapshotId,
      protocolId,
      user: normalizedUser as `0x${string}`,
      blockNumber,
      timestamp,
      categoryId,
    });
  }
}
