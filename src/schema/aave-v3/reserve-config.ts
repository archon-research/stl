/**
 * Reserve Configuration Table (Aave V3 Architecture)
 * 
 * Tracks reserve rate and index snapshots for Aave V3-like protocols.
 * Captures data from ReserveDataUpdated events.
 * This allows historical rate/index lookups for calculations.
 * 
 * Used by: Aave, Sparklend, and other Aave V3 forks
 */

import { onchainTable, relations } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";

export const ReserveConfig = onchainTable("ReserveConfig", (t) => ({
  // Primary key: protocolId + tokenId + blockNumber (e.g., "sparklend-mainnet-mainnet-0xc02...cc2-19000000")
  id: t.text().primaryKey(),
  
  // FK to Protocol table (enforced at application level)
  protocolId: t.text().notNull(),
  
  // FK to Token table - the reserve/underlying asset (enforced at application level)
  tokenId: t.text().notNull(),
  
  // Block number when this configuration was set
  blockNumber: t.bigint().notNull(),
  
  // Block timestamp
  timestamp: t.bigint().notNull(),
  
  // Transaction hash
  transactionHash: t.text().notNull(),
  
  // Reserve rates and indexes from ReserveDataUpdated event
  liquidityRate: t.bigint().notNull(),
  stableBorrowRate: t.bigint().notNull(),
  variableBorrowRate: t.bigint().notNull(),
  liquidityIndex: t.bigint().notNull(),
  variableBorrowIndex: t.bigint().notNull(),
}));

export const reserveConfigRelations = relations(ReserveConfig, ({ one }) => ({
  protocol: one(Protocol, {
    fields: [ReserveConfig.protocolId],
    references: [Protocol.id],
  }),
  token: one(Token, {
    fields: [ReserveConfig.tokenId],
    references: [Token.id],
  }),
}));
