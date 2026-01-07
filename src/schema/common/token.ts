/**
 * Token Reference Table
 * 
 * Tracks ERC20 tokens on each chain.
 * Universal across all protocols - the same WETH token can be used by multiple protocols.
 * 
 * Examples: WETH, USDC, DAI, etc.
 */

import { onchainTable, relations } from "ponder";
import { Chain } from "./chain";

export const Token = onchainTable("Token", (t) => ({
  // Primary key: chainId + address (e.g., "mainnet-0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
  id: t.text().primaryKey(),
  
  // FK to Chain table (enforced at application level)
  chainId: t.text().notNull(),
  
  // Token contract address
  address: t.hex().notNull(),
  
  // Token symbol (e.g., "WETH", "USDC")
  symbol: t.text().notNull(),
  
  // Token name (e.g., "Wrapped Ether")
  name: t.text().notNull(),
  
  // Token decimals (e.g., 18, 6)
  decimals: t.integer().notNull(),
  
  // First seen block number
  firstSeenBlock: t.bigint().notNull(),
  
  // First seen timestamp
  firstSeenTimestamp: t.bigint().notNull(),
}));

export const tokenRelations = relations(Token, ({ one }) => ({
  chain: one(Chain, {
    fields: [Token.chainId],
    references: [Chain.id],
  }),
}));
