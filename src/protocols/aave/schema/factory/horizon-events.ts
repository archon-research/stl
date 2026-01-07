/**
 * Aave Horizon Market Event Tables Factory
 * 
 * These events are unique to the Aave V3 Horizon market.
 * All events reference normalized Protocol, Token, and User tables.
 */

import { onchainTable } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";
import { User } from "@/schema/common/user";

export function createAaveHorizonEventTables(chainName: string) {
  const prefix = `Aave${chainName}Horizon`;

  return {
    BackUnbacked: onchainTable(`${prefix}BackUnbacked`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      backerId: t.text().notNull().references(() => User.id), // FK to User
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
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User (initiator)
      onBehalfOfId: t.text().notNull().references(() => User.id), // FK to User (beneficiary)
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
