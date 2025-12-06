/**
 * Database operations for token prices
 */

import type { Database } from "bun:sqlite";
import type { ChainId } from "../config/chains";
import type { TokenPrice } from "../services/prices";

/**
 * Save token prices to the database
 */
export function savePricesToDatabase(
  db: Database,
  chainId: ChainId,
  prices: TokenPrice[]
): void {
  const insertPrice = db.query(`
    INSERT OR IGNORE INTO token_prices 
    (chain_id, block_number, timestamp, token_address, token_symbol, price_usd)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  db.transaction(() => {
    for (const price of prices) {
      insertPrice.run(
        chainId,
        price.blockNumber,
        price.timestamp,
        price.tokenAddress,
        price.tokenSymbol,
        price.priceUsd
      );
    }
  })();
}

/**
 * Get the latest price for a token from the database
 */
export function getLatestPrice(
  db: Database,
  chainId: ChainId,
  tokenAddress: string
): TokenPrice | null {
  const row = db.query(`
    SELECT block_number, timestamp, token_address, token_symbol, price_usd
    FROM token_prices
    WHERE chain_id = ? AND token_address = ?
    ORDER BY block_number DESC
    LIMIT 1
  `).get(chainId, tokenAddress.toLowerCase()) as {
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

/**
 * Get all latest prices for a chain
 */
export function getAllLatestPrices(
  db: Database,
  chainId: ChainId
): TokenPrice[] {
  const rows = db.query(`
    SELECT DISTINCT token_address, block_number, timestamp, token_symbol, price_usd
    FROM token_prices t1
    WHERE chain_id = ? AND block_number = (
      SELECT MAX(block_number) FROM token_prices t2 
      WHERE t2.chain_id = t1.chain_id AND t2.token_address = t1.token_address
    )
  `).all(chainId) as Array<{
    block_number: number;
    timestamp: number;
    token_address: string;
    token_symbol: string;
    price_usd: string;
  }>;

  return rows.map(row => ({
    blockNumber: row.block_number,
    timestamp: row.timestamp,
    tokenAddress: row.token_address,
    tokenSymbol: row.token_symbol,
    priceUsd: row.price_usd
  }));
}
