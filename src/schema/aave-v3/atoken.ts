/**
 * AToken Reference Table (Aave V3 Architecture)
 * 
 * Tracks interest-bearing tokens in Aave V3-like protocols (Aave, Sparklend, etc.).
 * These are the tokens users receive when depositing assets.
 * 
 * Examples: spWETH (Sparklend), aEthWETH (Aave), etc.
 */

import { onchainTable } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";

export const AToken = onchainTable("AToken", (t) => ({
  // Primary key: protocolId + address (e.g., "sparklend-mainnet-0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db")
  id: t.text().primaryKey(),
  
  // Foreign key to Protocol table
  protocolId: t.text().notNull().references(() => Protocol.id),
  
  // Foreign key to Token table (underlying asset)
  tokenId: t.text().notNull().references(() => Token.id),
  
  // AToken contract address
  address: t.hex().notNull(),
  
  // AToken symbol (e.g., "spWETH", "aEthWETH")
  symbol: t.text().notNull(),
  
  // AToken name (e.g., "Spark Wrapped Ether")
  name: t.text().notNull(),
  
  // AToken decimals (usually same as underlying token)
  decimals: t.integer().notNull(),
  
  // First seen block number
  firstSeenBlock: t.bigint().notNull(),
  
  // First seen timestamp
  firstSeenTimestamp: t.bigint().notNull(),
}));
