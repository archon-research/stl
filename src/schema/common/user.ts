import { onchainTable } from "ponder";
import { Chain } from "./chain";

/**
 * User Table - Shared across all protocols on a chain
 * 
 * Stores unique user addresses per chain. A user who interacts with both
 * Aave and Sparklend on Mainnet will have a single entry here.
 * 
 * ID format: `${chainId}-${address.toLowerCase()}`
 * Example: "mainnet-0x1234...abcd"
 */
export const User = onchainTable("User", (t) => ({
  id: t.text().primaryKey(), // `${chainId}-${address}`
  chainId: t.text().notNull().references(() => Chain.id), // FK to Chain
  address: t.hex().notNull(), // User's Ethereum address
  firstSeenBlock: t.bigint().notNull(), // First block where this user appeared
  firstSeenTimestamp: t.bigint().notNull(), // First timestamp where this user appeared
  lastActivityBlock: t.bigint().notNull(), // Last block of any activity
  lastActivityTimestamp: t.bigint().notNull(), // Last timestamp of any activity
}));

