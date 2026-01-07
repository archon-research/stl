import type { Context } from "ponder:registry";

/**
 * Get all reserve assets that have been seen in the protocol
 * Queries ReserveDataUpdated events to find all unique reserve addresses
 */
export async function getAllReserveAssets(
  context: Context,
  reserveDataUpdatedTable: any
): Promise<`0x${string}`[]> {
  const { db } = context;
  
  // Get all ReserveDataUpdated events
  const allReserves = await db.sql
    .select()
    .from(reserveDataUpdatedTable);
  
  // Extract unique reserve addresses
  const uniqueReserves = Array.from(
    new Set(allReserves.map((r) => r.reserve.toLowerCase()))
  ) as `0x${string}`[];
  
  return uniqueReserves;
}

