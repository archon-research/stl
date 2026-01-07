import type { Context } from "ponder:registry";
import { eq, and, lte } from "ponder";
import { getProtocolId } from "@/db/helpers";

/**
 * Track a user as active in the protocol
 */
export async function trackActiveUser(
  context: Context,
  chainId: string,
  activeUserTable: any,
  userAddress: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint
): Promise<void> {
  const { db } = context;
  const normalized = userAddress.toLowerCase() as `0x${string}`;
  const id = `sparklend-${chainId}-${normalized}`;

  await db.insert(activeUserTable)
    .values({
      id,
      user: normalized,
      hasActivePosition: true,
      lastUpdateBlockNumber: blockNumber,
      lastUpdateTimestamp: timestamp,
    })
    .onConflictDoUpdate((oc) =>
      oc.set({
        hasActivePosition: true,
        lastUpdateBlockNumber: blockNumber,
        lastUpdateTimestamp: timestamp,
      })
    );
}

/**
 * Update user's position status (active/inactive)
 */
async function updateUserPositionStatus(
  context: Context,
  activeUserTable: any,
  userAddress: `0x${string}`,
  hasPosition: boolean,
  blockNumber: bigint,
  timestamp: bigint
): Promise<void> {
  const { db } = context;
  
  await db.sql
    .update(activeUserTable)
    .set({
      hasActivePosition: hasPosition,
      lastUpdateBlockNumber: blockNumber,
      lastUpdateTimestamp: timestamp,
    })
    .where(eq(activeUserTable.user, userAddress));
}

/**
 * Get all active users from the ActiveUser table
 */
export async function getActiveUsers(
  context: Context,
  activeUserTable: any
): Promise<`0x${string}`[]> {
  const { db } = context;
  
  try {
    const rows = await db.sql
      .select()
      .from(activeUserTable)
      .where(eq(activeUserTable.hasActivePosition, true));
    
    return rows.map((r: any) => r.user as `0x${string}`);
  } catch (error) {
    console.error("Error fetching active users:", error);
    return [];
  }
}

/**
 * Calculate health factor for a user based on their positions and prices
 * 
 * Health Factor = (Total Collateral × Avg Liquidation Threshold) / Total Debt
 * Returns max value if no debt exists
 */
async function calculateHealthFactorForUser(
  context: Context,
  protocolId: string,
  userSupplyPositionTable: any,
  userBorrowPositionTable: any,
  userAddress: `0x${string}`,
  priceMap: Map<string, bigint>
): Promise<{
  healthFactor: bigint;
  totalCollateralBase: bigint;
  totalDebtBase: bigint;
  collateralBreakdown: Array<{
    reserveId: string;
    balance: bigint;
    price: bigint;
    valueUSD: bigint;
    liquidationThreshold: bigint;
  }>;
  debtBreakdown: Array<{
    reserveId: string;
    debt: bigint;
    price: bigint;
    valueUSD: bigint;
  }>;
}> {
  const { db } = context;
  
  // Get all supply positions for this user
  const supplyPositions = await db.sql
    .select()
    .from(userSupplyPositionTable)
    .where(and(
      eq(userSupplyPositionTable.protocolId, protocolId),
      eq(userSupplyPositionTable.user, userAddress)
    ));

  // Get all borrow positions for this user
  const borrowPositions = await db.sql
    .select()
    .from(userBorrowPositionTable)
    .where(and(
      eq(userBorrowPositionTable.protocolId, protocolId),
      eq(userBorrowPositionTable.user, userAddress)
    ));

  const RAY = 10n ** 27n;
  const LIQUIDATION_THRESHOLD = 8000n; // 80% default (in basis points out of 10000)

  let totalCollateralBase = 0n;
  let totalDebtBase = 0n;
  let weightedLiquidationThreshold = 0n;

  const collateralBreakdown: Array<{
    reserveId: string;
    balance: bigint;
    price: bigint;
    valueUSD: bigint;
    liquidationThreshold: bigint;
  }> = [];

  const debtBreakdown: Array<{
    reserveId: string;
    debt: bigint;
    price: bigint;
    valueUSD: bigint;
  }> = [];

  // Calculate collateral value
  for (const position of supplyPositions) {
    if (!position.isCollateral || position.balance === 0n) continue;

    const price = priceMap.get(position.reserveId);
    if (!price) {
      console.warn(`No price found for reserve ${position.reserveId}`);
      continue;
    }

    // Value in base currency (8 decimals)
    const valueUSD = (position.balance * price) / 10n ** 18n;
    totalCollateralBase += valueUSD;
    weightedLiquidationThreshold += (valueUSD * LIQUIDATION_THRESHOLD);

    collateralBreakdown.push({
      reserveId: position.reserveId,
      balance: position.balance,
      price,
      valueUSD,
      liquidationThreshold: LIQUIDATION_THRESHOLD,
    });
  }

  // Calculate debt value
  for (const position of borrowPositions) {
    if (position.variableDebt === 0n && position.stableDebt === 0n) continue;

    const totalDebt = position.variableDebt + position.stableDebt;
    const price = priceMap.get(position.reserveId);
    if (!price) {
      console.warn(`No price found for reserve ${position.reserveId}`);
      continue;
    }

    // Value in base currency (8 decimals)
    const valueUSD = (totalDebt * price) / 10n ** 18n;
    totalDebtBase += valueUSD;

    debtBreakdown.push({
      reserveId: position.reserveId,
      debt: totalDebt,
      price,
      valueUSD,
    });
  }

  // Calculate health factor
  let healthFactor: bigint;
  if (totalDebtBase === 0n) {
    // No debt = infinite health factor (use max value)
    healthFactor = 2n ** 256n - 1n;
  } else if (totalCollateralBase === 0n) {
    // No collateral but has debt = zero health factor (liquidatable)
    healthFactor = 0n;
  } else {
    // Health Factor = (Total Collateral × Avg LT) / Total Debt
    const avgLiquidationThreshold = weightedLiquidationThreshold / totalCollateralBase;
    const collateralValue = (totalCollateralBase * avgLiquidationThreshold) / 10000n;
    healthFactor = (collateralValue * RAY) / totalDebtBase;
  }

  return {
    healthFactor,
    totalCollateralBase,
    totalDebtBase,
    collateralBreakdown,
    debtBreakdown,
  };
}

/**
 * Snapshot a specific user's health factor at a specific block
 */
export async function snapshotUserHealthFactor(
  context: Context,
  chainId: string,
  protocolId: string,
  userHealthFactorHistoryTable: any,
  userPositionBreakdownTable: any,
  userSupplyPositionTable: any,
  userBorrowPositionTable: any,
  activeUserTable: any,
  userAddress: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint,
  priceMap: Map<string, bigint>
): Promise<void> {
  const { db } = context;
  const normalizedUser = userAddress.toLowerCase() as `0x${string}`;

  try {
    // Calculate health factor
    const result = await calculateHealthFactorForUser(
      context,
      protocolId,
      userSupplyPositionTable,
      userBorrowPositionTable,
      normalizedUser,
      priceMap
    );

    // Store health factor history
    const hfId = `${protocolId}-${normalizedUser}-${blockNumber}`;
    await db.insert(userHealthFactorHistoryTable)
      .values({
        id: hfId,
        protocolId,
        user: normalizedUser,
        healthFactor: result.healthFactor,
        totalCollateralBase: result.totalCollateralBase,
        totalDebtBase: result.totalDebtBase,
        blockNumber,
        timestamp,
      })
      .onConflictDoNothing();

    // Store position breakdown
    const breakdownInserts = [];
    for (const collateral of result.collateralBreakdown) {
      const collateralId = `${protocolId}-${normalizedUser}-${blockNumber}-${collateral.reserveId}-collateral`;
      breakdownInserts.push({
        id: collateralId,
        protocolId,
        reserveId: collateral.reserveId,
        user: normalizedUser,
        blockNumber,
        timestamp,
        positionType: 'collateral' as const,
        amount: collateral.balance,
        price: collateral.price,
        valueUSD: collateral.valueUSD,
        liquidationThreshold: collateral.liquidationThreshold,
      });
    }
    for (const debt of result.debtBreakdown) {
      const debtId = `${protocolId}-${normalizedUser}-${blockNumber}-${debt.reserveId}-debt`;
      breakdownInserts.push({
        id: debtId,
        protocolId,
        reserveId: debt.reserveId,
        user: normalizedUser,
        blockNumber,
        timestamp,
        positionType: 'debt' as const,
        amount: debt.debt,
        price: debt.price,
        valueUSD: debt.valueUSD,
        liquidationThreshold: null,
      });
    }
    if (breakdownInserts.length > 0) {
      await db.insert(userPositionBreakdownTable)
        .values(breakdownInserts)
        .onConflictDoNothing();
    }

    // Update active user status
    const hasPosition = result.totalCollateralBase > 0n || result.totalDebtBase > 0n;
    await updateUserPositionStatus(context, activeUserTable, normalizedUser, hasPosition, blockNumber, timestamp);
  } catch (error: any) {
    console.error(`\n❌ [SNAPSHOT] FAILED for user ${userAddress} at block ${blockNumber}:`, error);
    throw error;
  }
}

/**
 * Snapshot all active users' health factors
 */
export async function snapshotAllActiveUsers(
  context: Context,
  chainId: string,
  protocolId: string,
  userHealthFactorHistoryTable: any,
  userPositionBreakdownTable: any,
  userSupplyPositionTable: any,
  userBorrowPositionTable: any,
  activeUserTable: any,
  blockNumber: bigint,
  timestamp: bigint,
  priceMap: Map<string, bigint>
): Promise<void> {
  console.log(`\n📸 [Block ${blockNumber}] Creating health factor snapshots for all active users...`);

  const activeUsers = await getActiveUsers(context, activeUserTable);
  console.log(`  Found ${activeUsers.length} active users`);

  let successCount = 0;
  let errorCount = 0;

  for (const userAddress of activeUsers) {
    try {
      await snapshotUserHealthFactor(
        context,
        chainId,
        protocolId,
        userHealthFactorHistoryTable,
        userPositionBreakdownTable,
        userSupplyPositionTable,
        userBorrowPositionTable,
        activeUserTable,
        userAddress,
        blockNumber,
        timestamp,
        priceMap
      );
      successCount++;
    } catch (error: any) {
      errorCount++;
      console.error(`  Error for ${userAddress}: ${error?.message}`);
    }
  }

  console.log(`\n✅ Snapshot complete: ${successCount} succeeded, ${errorCount} failed`);
}
