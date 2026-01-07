/**
 * Aave Horizon Market Event Tables Factory
 * 
 * These events are unique to the Aave V3 Horizon market.
 * All events reference normalized Protocol, Token, and User tables.
 */

import { onchainTable } from "ponder";

export function createAaveHorizonEventTables(chainName: string) {
  const prefix = `Aave${chainName}Horizon`;

  return {
    BackUnbacked: onchainTable(`${prefix}BackUnbacked`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      backerId: t.text().notNull(), // FK to User
      backer: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      fee: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    MintUnbacked: onchainTable(`${prefix}MintUnbacked`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User (initiator)
      onBehalfOfId: t.text().notNull(), // FK to User (beneficiary)
      user: t.hex().notNull(), // Raw address for convenience
      onBehalfOf: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      referralCode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),
  };
}
