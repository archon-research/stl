/**
 * Chain Reference Table
 * 
 * Tracks all blockchain networks indexed by this application.
 * Universal across all protocols.
 */

import { onchainTable } from "ponder";

export const Chain = onchainTable("Chain", (t) => ({
  // Primary key: lowercase chain name (e.g., "mainnet", "gnosis")
  id: t.text().primaryKey(),
  
  // Numeric chain ID (e.g., 1, 100)
  chainId: t.integer().notNull().unique(),
  
  // Display name (e.g., "Mainnet", "Gnosis")
  name: t.text().notNull(),
}));

