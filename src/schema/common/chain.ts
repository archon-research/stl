/**
 * Chain Reference Table
 * 
 * Tracks all blockchain networks indexed by this application.
 * Universal across all protocols.
 */

import { onchainTable, relations } from "ponder";
import { Protocol } from "./protocol";
import { Token } from "./token";
import { User } from "./user";

export const Chain = onchainTable("Chain", (t) => ({
  // Primary key: lowercase chain name (e.g., "mainnet", "gnosis")
  id: t.text().primaryKey(),
  
  // Numeric chain ID (e.g., 1, 100)
  chainId: t.integer().notNull(),
  
  // Display name (e.g., "Mainnet", "Gnosis")
  name: t.text().notNull(),
}));

export const chainRelations = relations(Chain, ({ many }) => ({
  protocols: many(Protocol),
  tokens: many(Token),
  users: many(User),
}));

