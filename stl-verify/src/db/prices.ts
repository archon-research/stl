/**
 * Database operations for token prices
 */

import type { Database } from "bun:sqlite";
import type { ChainId } from "../config/chains";
import type { TokenPrice } from "../services/prices";

/**
 * Save token prices to the database. ˝
 */
export function savePricesToDatabase(
  db: Database,
  chainId: ChainId,
  prices: TokenPrice[]
): void {
  if (prices.length === 0) {return;}

  const BATCH_SIZE = 100;
  db.transaction(() => {
    for (let i = 0; i < prices.length; i += BATCH_SIZE) {
      const batch = prices.slice(i, i + BATCH_SIZE);
      const placeholders = batch.map(() => "(?, ?, ?, ?, ?, ?)").join(", ");
      const values = batch.flatMap((price) => [
        chainId,
        price.blockNumber,
        price.timestamp,
        price.tokenAddress,
        price.tokenSymbol,
        price.priceUsd,
      ]);

      db.run(
        `INSERT OR IGNORE INTO token_prices 
         (chain_id, block_number, timestamp, token_address, token_symbol, price_usd)
         VALUES ${placeholders}`,
        values
      );
    }
  })();
}

/**
 * Get price at a specific block (or closest before)
 */
export function getPriceAtBlock(
  db: Database,
  chainId: ChainId,
  tokenAddress: string,
  blockNumber: number
): TokenPrice | null {
  const row = db.query(`
    SELECT block_number, timestamp, token_address, token_symbol, price_usd
    FROM token_prices
    WHERE chain_id = ? AND token_address = ? AND block_number <= ?
    ORDER BY block_number DESC
    LIMIT 1
  `).get(chainId, tokenAddress.toLowerCase(), blockNumber) as {
    block_number: number;
    timestamp: number;
    token_address: string;
    token_symbol: string;
    price_usd: string;
  } | null;

  if (!row) return null;

  return {
    blockNumber: row.block_number,
    timestamp: row.timestamp,
    tokenAddress: row.token_address,
    tokenSymbol: row.token_symbol,
    priceUsd: row.price_usd
  };
}
