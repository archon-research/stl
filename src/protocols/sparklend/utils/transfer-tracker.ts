import type { Context } from "ponder:registry";
import { eq, desc, and, lte } from "ponder";
import { getUnderlyingAsset } from "./sptoken-mapping";
import { getTokenId, ensureUser, extractAddressFromId } from "@/db/helpers";
import { trackActiveUser } from "./health-factor-snapshot";

const RAY = 10n ** 27n;
const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000" as `0x${string}`;

/**
 * Transfer Tracker - Multi-chain support
 * Handles spToken (aToken) transfers between users
 * 
 * NOTE: This utility is designed to be reusable across Aave V3-like protocols (Sparklend, Aave, etc.)
 * It's currently under @sparklend/utils but can be shared with @aave when aToken tracking is implemented.
 */

/**
 * Handle spToken (aToken) transfer - updates both sender and receiver positions
 */
export async function handleSpTokenTransfer(
  context: Context,
  chainIdentifier: string, // Lowercase chain identifier ("mainnet", "gnosis")
  protocolId: string, // Already computed protocol ID
  userScaledSupplyPositionTable: any,
  userSupplyPositionTable: any,
  reserveDataUpdatedTable: any,
  activeUserTable: any,
  spTokenAddress: `0x${string}`,
  from: `0x${string}`,
  to: `0x${string}`,
  value: bigint,
  blockNumber: bigint,
  timestamp: bigint
): Promise<void> {
  // Skip mints (from=0x0) - handled by Supply event
  if (from.toLowerCase() === ZERO_ADDRESS) {
    return;
  }

  // Skip burns (to=0x0) - handled by Withdraw event
  if (to.toLowerCase() === ZERO_ADDRESS) {
    return;
  }

  // Skip zero-value transfers
  if (value === 0n) {
    return;
  }

  // Skip self-transfers
  if (from.toLowerCase() === to.toLowerCase()) {
    return;
  }

  // Get underlying asset from spToken address
  const underlyingAsset = getUnderlyingAsset(spTokenAddress);
  if (!underlyingAsset) {
    console.warn(`Unknown spToken address: ${spTokenAddress}`);
    return;
  }

  // Get reserveId
  const reserveId = getTokenId(chainIdentifier, underlyingAsset);

  // Get current liquidity index
  const reserveData = await getReserveDataAtBlock(
    context,
    reserveDataUpdatedTable,
    reserveId,
    blockNumber
  );
  const currentIndex = reserveData.liquidityIndex;

  // Calculate scaled amount
  const scaledAmount = (value * RAY) / currentIndex;

  const normalizedFrom = from.toLowerCase() as `0x${string}`;
  const normalizedTo = to.toLowerCase() as `0x${string}`;

  // Update sender's positions (decrease)
  await updateScaledSupplyPosition(
    context,
    chainIdentifier,
    protocolId,
    reserveId,
    userScaledSupplyPositionTable,
    normalizedFrom,
    -scaledAmount,
    currentIndex,
    blockNumber,
    timestamp
  );

  await updateCurrentSupplyPosition(
    context,
    chainIdentifier,
    protocolId,
    reserveId,
    userSupplyPositionTable,
    normalizedFrom,
    -value,
    blockNumber,
    timestamp
  );

  // Update receiver's positions (increase)
  await updateScaledSupplyPosition(
    context,
    chainIdentifier,
    protocolId,
    reserveId,
    userScaledSupplyPositionTable,
    normalizedTo,
    scaledAmount,
    currentIndex,
    blockNumber,
    timestamp
  );

  await updateCurrentSupplyPosition(
    context,
    chainIdentifier,
    protocolId,
    reserveId,
    userSupplyPositionTable,
    normalizedTo,
    value,
    blockNumber,
    timestamp
  );

  // Track both users as active
  await trackActiveUser(context, chainIdentifier, protocolId, activeUserTable, normalizedFrom, blockNumber, timestamp);
  await trackActiveUser(context, chainIdentifier, protocolId, activeUserTable, normalizedTo, blockNumber, timestamp);

  console.log(
    `📤 spToken Transfer: ${normalizedFrom.slice(0, 10)}... -> ${normalizedTo.slice(0, 10)}... | ${value} (scaled: ${scaledAmount})`
  );
}

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
    throw new Error(
      `No reserve data found for ${reserveId} at or before block ${blockNumber}`
    );
  }

  return updates[0];
}

/**
 * Get previous supply snapshot for a user-reserveId
 */
async function getPreviousSupplySnapshot(
  context: Context,
  userScaledSupplyPositionTable: any,
  protocolId: string,
  user: string,
  reserveId: string,
  beforeBlock: bigint
) {
  const snapshots = await context.db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(
      and(
        eq(userScaledSupplyPositionTable.protocolId, protocolId),
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
 * Update scaled supply position
 */
async function updateScaledSupplyPosition(
  context: Context,
  chainIdentifier: string, // Lowercase chain identifier ("mainnet", "gnosis")
  protocolId: string,
  reserveId: string,
  userScaledSupplyPositionTable: any,
  userAddress: `0x${string}`,
  scaledDelta: bigint,
  currentIndex: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  // Extract token address to avoid duplicating chain identifier
  const tokenAddr = extractAddressFromId(reserveId);
  const snapshotId = `${protocolId}-${userAddress}-${tokenAddr}-${blockNumber}`;

  const currentSnapshot = await db.sql
    .select()
    .from(userScaledSupplyPositionTable)
    .where(eq(userScaledSupplyPositionTable.id, snapshotId))
    .limit(1);

  if (currentSnapshot.length > 0 && currentSnapshot[0]) {
    const newScaledBalance = currentSnapshot[0].scaledBalance + scaledDelta;

    await db.sql
      .update(userScaledSupplyPositionTable)
      .set({
        scaledBalance: newScaledBalance >= 0n ? newScaledBalance : 0n,
        lastLiquidityIndex: currentIndex,
      })
      .where(eq(userScaledSupplyPositionTable.id, snapshotId));
  } else {
    const prevSnapshot = await getPreviousSupplySnapshot(
      context,
      userScaledSupplyPositionTable,
      protocolId,
      userAddress,
      reserveId,
      blockNumber - 1n
    );

    const previousBalance = prevSnapshot?.scaledBalance || 0n;
    const previousIsCollateral = prevSnapshot?.isCollateral ?? true;
    const newScaledBalance = previousBalance + scaledDelta;

    const userId = await ensureUser(context, chainIdentifier, userAddress, blockNumber, timestamp);
    await db.insert(userScaledSupplyPositionTable).values({
      id: snapshotId,
      protocolId,
      reserveId,
      userId,
      user: userAddress,
      blockNumber,
      timestamp,
      scaledBalance: newScaledBalance >= 0n ? newScaledBalance : 0n,
      isCollateral: previousIsCollateral,
      lastLiquidityIndex: currentIndex,
    });
  }
}

/**
 * Update current supply position table
 */
async function updateCurrentSupplyPosition(
  context: Context,
  chainIdentifier: string, // Lowercase chain identifier ("mainnet", "gnosis")
  protocolId: string,
  reserveId: string,
  userSupplyPositionTable: any,
  userAddress: `0x${string}`,
  amountDelta: bigint,
  blockNumber: bigint,
  timestamp: bigint
) {
  const { db } = context;
  // Extract token address to avoid duplicating chain identifier
  const tokenAddr = extractAddressFromId(reserveId);
  const positionId = `${protocolId}-${userAddress}-${tokenAddr}`;

  const existing = await db.sql
    .select()
    .from(userSupplyPositionTable)
    .where(eq(userSupplyPositionTable.id, positionId))
    .limit(1);

  if (existing.length > 0 && existing[0]) {
    const newBalance = existing[0].balance + amountDelta;

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
  } else if (amountDelta > 0n) {
    const userId = await ensureUser(context, chainIdentifier, userAddress, blockNumber, timestamp);
    await db.insert(userSupplyPositionTable).values({
      id: positionId,
      protocolId,
      reserveId,
      userId,
      user: userAddress,
      balance: amountDelta,
      isCollateral: true,
      lastUpdateBlockNumber: blockNumber,
      lastUpdateTimestamp: timestamp,
    });
  }
}
