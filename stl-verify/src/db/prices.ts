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
  if (prices.length === 0) {
    return;
  }

  const BATCH_SIZE = 100;
  db.transaction(() => {
    for (let i = 0; i < prices.length; i += BATCH_SIZE) {
      // Grab BATCH_SIZE amount of prices, create BATCH_SIZE number of placeholders, and flatten the values
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
