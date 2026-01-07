/**
 * Aave Core Market Event Tables Factory
 * 
 * These events are unique to the Aave V3 Core market.
 * All events reference normalized Protocol and User tables.
 */

import { onchainTable } from "ponder";

export function createAaveCoreEventTables(chainName: string) {
  const prefix = `Aave${chainName}Core`;

  return {
    PositionManagerApproved: onchainTable(`${prefix}PositionManagerApproved`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      positionManager: t.hex().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    PositionManagerRevoked: onchainTable(`${prefix}PositionManagerRevoked`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      positionManager: t.hex().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),
  };
}
