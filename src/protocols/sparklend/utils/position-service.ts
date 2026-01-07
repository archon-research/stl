import type { Context } from "ponder:registry";
import { eq } from "ponder";

/**
 * Position Service - Handles all position updates
 * 
 * This service ONLY updates user positions (supplies and borrows).
 * Snapshot calculations are done separately via snapshot-calculator.ts
 * 
 * All functions accept chainId and chain-specific schema tables for multi-chain support.
 */

/**
 * Handle supply (deposit) event
 */
export async function handleSupplyChange(
  context: Context,
  userSupplyPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: `0x${string}`,
  amountDelta: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const positionId = `${protocolId}-${userAddress}-${reserveId}`;

  const existing = await db.sql
    .select()
    .from(userSupplyPositionTable)
    .where(eq(userSupplyPositionTable.id, positionId))
    .limit(1);

  if (existing.length > 0 && existing[0]) {
    await db.sql
      .update(userSupplyPositionTable)
      .set({
        balance: existing[0].balance + amountDelta,
        lastUpdateBlockNumber: blockNumber,
        lastUpdateTimestamp: timestamp,
      })
      .where(eq(userSupplyPositionTable.id, positionId));
  } else {
    await db.insert(userSupplyPositionTable).values({
      id: positionId,
      protocolId,
      reserveId,
      user: userAddress,
      balance: amountDelta,
      isCollateral: true,
      lastUpdateBlockNumber: blockNumber,
      lastUpdateTimestamp: timestamp,
    });
  }
}

/**
 * Handle withdraw event
 */
export async function handleWithdrawChange(
  context: Context,
  userSupplyPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: `0x${string}`,
  amount: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const positionId = `${protocolId}-${userAddress}-${reserveId}`;

  const existing = await db.sql
    .select()
    .from(userSupplyPositionTable)
    .where(eq(userSupplyPositionTable.id, positionId))
    .limit(1);

  if (existing.length > 0 && existing[0]) {
    const newBalance = existing[0].balance - amount;
    
    if (newBalance > 0n) {
      await db.sql
        .update(userSupplyPositionTable)
        .set({
          balance: newBalance,
          lastUpdateBlockNumber: blockNumber,
          lastUpdateTimestamp: timestamp,
        })
        .where(eq(userSupplyPositionTable.id, positionId));
    } else {
      await db.sql
        .delete(userSupplyPositionTable)
        .where(eq(userSupplyPositionTable.id, positionId));
    }
  }
}

/**
 * Handle borrow event
 */
export async function handleBorrowChange(
  context: Context,
  userBorrowPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: `0x${string}`,
  amount: bigint,
  interestRateMode: number,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const positionId = `${protocolId}-${userAddress}-${reserveId}`;
  const isStableRate = interestRateMode === 1;

  const existing = await db.sql
    .select()
    .from(userBorrowPositionTable)
    .where(eq(userBorrowPositionTable.id, positionId))
    .limit(1);

  if (existing.length > 0 && existing[0]) {
    await db.sql
      .update(userBorrowPositionTable)
      .set({
        stableDebt: isStableRate ? existing[0].stableDebt + amount : existing[0].stableDebt,
        variableDebt: !isStableRate ? existing[0].variableDebt + amount : existing[0].variableDebt,
        lastUpdateBlockNumber: blockNumber,
        lastUpdateTimestamp: timestamp,
      })
      .where(eq(userBorrowPositionTable.id, positionId));
  } else {
    await db.insert(userBorrowPositionTable).values({
      id: positionId,
      protocolId,
      reserveId,
      user: userAddress,
      stableDebt: isStableRate ? amount : 0n,
      variableDebt: !isStableRate ? amount : 0n,
      lastUpdateBlockNumber: blockNumber,
      lastUpdateTimestamp: timestamp,
    });
  }
}

/**
 * Handle repay event
 */
export async function handleRepayChange(
  context: Context,
  userBorrowPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: `0x${string}`,
  amount: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const positionId = `${protocolId}-${userAddress}-${reserveId}`;

  const existing = await db.sql
    .select()
    .from(userBorrowPositionTable)
    .where(eq(userBorrowPositionTable.id, positionId))
    .limit(1);

  if (existing.length > 0 && existing[0]) {
    const totalDebt = existing[0].stableDebt + existing[0].variableDebt;
    const remaining = totalDebt - amount;

    if (remaining > 0n) {
      const stableRatio = totalDebt > 0n ? Number(existing[0].stableDebt) / Number(totalDebt) : 0;
      const variableRatio = totalDebt > 0n ? Number(existing[0].variableDebt) / Number(totalDebt) : 0;

      await db.sql
        .update(userBorrowPositionTable)
        .set({
          stableDebt: BigInt(Math.floor(Number(remaining) * stableRatio)),
          variableDebt: BigInt(Math.floor(Number(remaining) * variableRatio)),
          lastUpdateBlockNumber: blockNumber,
          lastUpdateTimestamp: timestamp,
        })
        .where(eq(userBorrowPositionTable.id, positionId));
    } else {
      await db.sql
        .delete(userBorrowPositionTable)
        .where(eq(userBorrowPositionTable.id, positionId));
    }
  }
}

/**
 * Handle collateral toggle
 */
export async function handleCollateralToggle(
  context: Context,
  userSupplyPositionTable: any,
  protocolId: string,
  reserveId: string,
  userAddress: `0x${string}`,
  isCollateral: boolean,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  const positionId = `${protocolId}-${userAddress}-${reserveId}`;

  const existing = await db.sql
    .select()
    .from(userSupplyPositionTable)
    .where(eq(userSupplyPositionTable.id, positionId))
    .limit(1);

  if (existing.length > 0) {
    await db.sql
      .update(userSupplyPositionTable)
      .set({
        isCollateral,
        lastUpdateBlockNumber: blockNumber,
        lastUpdateTimestamp: timestamp,
      })
      .where(eq(userSupplyPositionTable.id, positionId));
  }
}

/**
 * Handle liquidation event
 */
export async function handleLiquidation(
  context: Context,
  userSupplyPositionTable: any,
  userBorrowPositionTable: any,
  protocolId: string,
  collateralReserveId: string,
  debtReserveId: string,
  userAddress: `0x${string}`,
  liquidatedCollateralAmount: bigint,
  debtToCover: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;

  // Update collateral position
  const collateralPositionId = `${protocolId}-${userAddress}-${collateralReserveId}`;
  const collateralExisting = await db.sql
    .select()
    .from(userSupplyPositionTable)
    .where(eq(userSupplyPositionTable.id, collateralPositionId))
    .limit(1);

  if (collateralExisting.length > 0 && collateralExisting[0]) {
    const newBalance = collateralExisting[0].balance - liquidatedCollateralAmount;
    
    if (newBalance > 0n) {
      await db.sql
        .update(userSupplyPositionTable)
        .set({
          balance: newBalance,
          lastUpdateBlockNumber: blockNumber,
          lastUpdateTimestamp: timestamp,
        })
        .where(eq(userSupplyPositionTable.id, collateralPositionId));
    } else {
      await db.sql
        .delete(userSupplyPositionTable)
        .where(eq(userSupplyPositionTable.id, collateralPositionId));
    }
  }

  // Update borrow position
  const borrowPositionId = `${protocolId}-${userAddress}-${debtReserveId}`;
  const borrowExisting = await db.sql
    .select()
    .from(userBorrowPositionTable)
    .where(eq(userBorrowPositionTable.id, borrowPositionId))
    .limit(1);

  if (borrowExisting.length > 0 && borrowExisting[0]) {
    const totalDebt = borrowExisting[0].stableDebt + borrowExisting[0].variableDebt;
    const remaining = totalDebt - debtToCover;

    if (remaining > 0n) {
      const stableRatio = totalDebt > 0n ? Number(borrowExisting[0].stableDebt) / Number(totalDebt) : 0;
      const variableRatio = totalDebt > 0n ? Number(borrowExisting[0].variableDebt) / Number(totalDebt) : 0;

      await db.sql
        .update(userBorrowPositionTable)
        .set({
          stableDebt: BigInt(Math.floor(Number(remaining) * stableRatio)),
          variableDebt: BigInt(Math.floor(Number(remaining) * variableRatio)),
          lastUpdateBlockNumber: blockNumber,
          lastUpdateTimestamp: timestamp,
        })
        .where(eq(userBorrowPositionTable.id, borrowPositionId));
    } else {
      await db.sql
        .delete(userBorrowPositionTable)
        .where(eq(userBorrowPositionTable.id, borrowPositionId));
    }
  }
}
