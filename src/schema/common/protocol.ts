/**
 * Protocol Reference Table
 * 
 * Tracks all protocol instances (protocol + chain combinations).
 * Universal across all protocols.
 * 
 * Examples: "sparklend-mainnet", "aave-core-mainnet", "aave-horizon-mainnet"
 */

import { onchainTable } from "ponder";
import { Chain } from "./chain";

export const Protocol = onchainTable("Protocol", (t) => ({
  // Primary key: protocol-type + chain (e.g., "sparklend-mainnet", "aave-core-mainnet")
  id: t.text().primaryKey(),
  
  // Display name (e.g., "Sparklend", "Aave Core", "Aave Horizon")
  name: t.text().notNull(),
  
  // Protocol type identifier (e.g., "sparklend", "aave-core", "aave-horizon")
  type: t.text().notNull(),
  
  // Foreign key to Chain table
  chainId: t.text().notNull().references(() => Chain.id),
  
  // Pool contract address for this protocol instance
  poolAddress: t.hex().notNull(),
}));
