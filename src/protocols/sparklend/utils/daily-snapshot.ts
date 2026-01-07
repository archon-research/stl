import type { Context } from "ponder:registry";
import { gte } from "ponder";

/**
 * Track last snapshotted day per chain to avoid duplicate snapshots
 */
const lastSnapshotDay = new Map<number, number>();

/**
 * Get day number from timestamp (days since Unix epoch)
 * @param timestamp - Unix timestamp in seconds
 * @returns Day number (e.g., 19713 for 2023-12-23)
 */
export function getDayFromTimestamp(timestamp: bigint): number {
  return Math.floor(Number(timestamp) / 86400);
}

/**
 * Check if we need to create daily snapshots
 * Returns the day number if snapshot needed, null otherwise
 *
 * @param chainId - Chain ID (1 for mainnet)
 * @param blockTimestamp - Current block timestamp
 * @returns Day number if new day detected, null if already processed
 */
export function shouldCreateDailySnapshot(
  chainId: number,
  blockTimestamp: bigint
): number | null {
  const currentDay = getDayFromTimestamp(blockTimestamp);
  const lastDay = lastSnapshotDay.get(chainId);

  if (lastDay === undefined || currentDay > lastDay) {
    lastSnapshotDay.set(chainId, currentDay);
    return currentDay;
  }

  return null;
}

/**
 * Get day boundaries (start and end timestamps)
 * @param day - Day number since Unix epoch
 * @returns Start and end timestamps for the day
 */
export function getDayBoundaries(day: number): {
  startTimestamp: bigint;
  endTimestamp: bigint;
} {
  const startTimestamp = BigInt(day * 86400);
  const endTimestamp = BigInt((day + 1) * 86400 - 1);
  return { startTimestamp, endTimestamp };
}

/**
 * Get all users who have ever had positions (for full backfill)
 *
 * Queries UserScaledSupplyPosition and UserScaledBorrowPosition
 * to find all unique users who have ever interacted.
 */
export async function getAllUsersWithPositions(
  context: Context,
  protocolId: string,
  userScaledSupplyPositionTable: any,
  userScaledBorrowPositionTable: any
): Promise<Set<`0x${string}`>> {
  const { db } = context;
  const users = new Set<`0x${string}`>();

  // Get all unique users from scaled supply positions with non-zero balance
  const supplyUsers = await db.sql
    .select({ user: userScaledSupplyPositionTable.user })
    .from(userScaledSupplyPositionTable)
    .where(gte(userScaledSupplyPositionTable.scaledBalance, 1n));

  supplyUsers.forEach((s: any) => users.add(s.user));

  // Get all unique users from scaled borrow positions with non-zero debt
  const borrowUsers = await db.sql
    .select({ user: userScaledBorrowPositionTable.user })
    .from(userScaledBorrowPositionTable)
    .where(gte(userScaledBorrowPositionTable.scaledVariableDebt, 1n));

  borrowUsers.forEach((s: any) => users.add(s.user));

  return users;
}

/**
 * Reset the last snapshot day tracking (useful for testing)
 */
export function resetDayTracking(): void {
  lastSnapshotDay.clear();
}

/**
 * Get current day tracking state (for debugging)
 */
export function getDayTrackingState(): Map<number, number> {
  return new Map(lastSnapshotDay);
}
