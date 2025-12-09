/**
 * Database module using Bun's built-in SQLite
 */

import { Database } from "bun:sqlite";
import { mkdirSync, existsSync } from "fs";
import { dirname } from "path";
import type { ChainId } from "../config/chains";

/**
 * Initialize the database with all required tables
 */
export function initDatabase(dbPath: string = "./data/sparklend_events.db"): Database {
  // Ensure the directory exists before creating the database
  const dir = dirname(dbPath);
  if (dir !== "." && dir !== ":memory:" && !existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  
  const db = new Database(dbPath, { create: true });
  
  // Enable WAL mode for better concurrent performance
  db.exec("PRAGMA journal_mode = WAL");
  
  // Create tables
  db.exec(`
    CREATE TABLE IF NOT EXISTS supply_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      transaction_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      reserve TEXT NOT NULL,
      user TEXT NOT NULL,
      on_behalf_of TEXT NOT NULL,
      amount TEXT NOT NULL,
      referral_code INTEGER NOT NULL,
      UNIQUE(chain_id, transaction_hash, log_index)
    );

    CREATE TABLE IF NOT EXISTS borrow_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      transaction_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      reserve TEXT NOT NULL,
      user TEXT NOT NULL,
      on_behalf_of TEXT NOT NULL,
      amount TEXT NOT NULL,
      interest_rate_mode INTEGER NOT NULL,
      borrow_rate TEXT NOT NULL,
      referral_code INTEGER NOT NULL,
      UNIQUE(chain_id, transaction_hash, log_index)
    );

    CREATE TABLE IF NOT EXISTS liquidation_call_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      transaction_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      collateral_asset TEXT NOT NULL,
      debt_asset TEXT NOT NULL,
      user TEXT NOT NULL,
      debt_to_cover TEXT NOT NULL,
      liquidated_collateral_amount TEXT NOT NULL,
      liquidator TEXT NOT NULL,
      receive_atoken INTEGER NOT NULL,
      UNIQUE(chain_id, transaction_hash, log_index)
    );

    CREATE TABLE IF NOT EXISTS withdraw_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      transaction_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      reserve TEXT NOT NULL,
      user TEXT NOT NULL,
      to_address TEXT NOT NULL,
      amount TEXT NOT NULL,
      UNIQUE(chain_id, transaction_hash, log_index)
    );

    CREATE TABLE IF NOT EXISTS repay_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      transaction_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      reserve TEXT NOT NULL,
      user TEXT NOT NULL,
      repayer TEXT NOT NULL,
      amount TEXT NOT NULL,
      use_atokens INTEGER NOT NULL,
      UNIQUE(chain_id, transaction_hash, log_index)
    );

    CREATE TABLE IF NOT EXISTS sync_state (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL UNIQUE,
      last_synced_block INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS price_sync_state (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL UNIQUE,
      last_synced_block INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS uniswap_sync_state (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL UNIQUE,
      last_synced_block INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS token_prices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      timestamp INTEGER NOT NULL,
      token_address TEXT NOT NULL,
      token_symbol TEXT NOT NULL,
      price_usd TEXT NOT NULL,
      UNIQUE(chain_id, block_number, token_address)
    );

    -- Uniswap V3 raw swaps (one row per Swap log)
    CREATE TABLE IF NOT EXISTS uniswap_v3_swaps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      pool_address TEXT NOT NULL,
      token0 TEXT NOT NULL,
      token1 TEXT NOT NULL,
      fee INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      block_number INTEGER NOT NULL,
      block_timestamp INTEGER NOT NULL,
      sender TEXT NOT NULL,
      recipient TEXT NOT NULL,
      amount0_raw TEXT NOT NULL,
      amount1_raw TEXT NOT NULL,
      sqrt_price_x96 TEXT NOT NULL,
      liquidity TEXT NOT NULL,
      tick INTEGER NOT NULL,
      UNIQUE(chain_id, tx_hash, log_index)
    );

    -- Daily per-token traded volume aggregated from DEX swaps
    CREATE TABLE IF NOT EXISTS token_volume_daily (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      token_address TEXT NOT NULL,
      date TEXT NOT NULL,
      volume_token REAL NOT NULL,
      volume_usd REAL,
      swap_count INTEGER NOT NULL,
      UNIQUE(chain_id, token_address, date)
    );

    -- Indexes for efficient queries
    CREATE INDEX IF NOT EXISTS idx_supply_chain_block ON supply_events(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_supply_user ON supply_events(user);
    CREATE INDEX IF NOT EXISTS idx_borrow_chain_block ON borrow_events(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_borrow_user ON borrow_events(user);
    CREATE INDEX IF NOT EXISTS idx_liquidation_chain_block ON liquidation_call_events(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_liquidation_user ON liquidation_call_events(user);
    CREATE INDEX IF NOT EXISTS idx_withdraw_chain_block ON withdraw_events(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_withdraw_user ON withdraw_events(user);
    CREATE INDEX IF NOT EXISTS idx_repay_chain_block ON repay_events(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_repay_user ON repay_events(user);
    CREATE INDEX IF NOT EXISTS idx_prices_chain_block ON token_prices(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_prices_token ON token_prices(token_address);
    CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON token_prices(timestamp);

    CREATE INDEX IF NOT EXISTS idx_uni_swaps_chain_block ON uniswap_v3_swaps(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_uni_swaps_pool ON uniswap_v3_swaps(pool_address);
    CREATE INDEX IF NOT EXISTS idx_uni_swaps_token0 ON uniswap_v3_swaps(token0);
    CREATE INDEX IF NOT EXISTS idx_uni_swaps_token1 ON uniswap_v3_swaps(token1);

    CREATE INDEX IF NOT EXISTS idx_token_volume_daily_token_date ON token_volume_daily(chain_id, token_address, date);
  `);

  return db;
}

/**
 * Get the last synced block for a chain
 */
export function getLastSyncedBlock(db: Database, chainId: ChainId): number | null {
  const row = db.query("SELECT last_synced_block FROM sync_state WHERE chain_id = ?").get(chainId) as { last_synced_block: number } | null;
  return row?.last_synced_block ?? null;
}

/**
 * Update the last synced block for a chain
 */
export function updateLastSyncedBlock(db: Database, chainId: ChainId, blockNumber: number): void {
  db.query(`
    INSERT INTO sync_state (chain_id, last_synced_block) VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE SET last_synced_block = excluded.last_synced_block
  `).run(chainId, blockNumber);
}

/**
 * Get the last price synced block for a chain
 */
export function getLastPriceSyncedBlock(db: Database, chainId: ChainId): number | null {
  const row = db.query("SELECT last_synced_block FROM price_sync_state WHERE chain_id = ?").get(chainId) as { last_synced_block: number } | null;
  return row?.last_synced_block ?? null;
}

/**
 * Update the last price synced block for a chain
 */
export function updateLastPriceSyncedBlock(db: Database, chainId: ChainId, blockNumber: number): void {
  db.query(`
    INSERT INTO price_sync_state (chain_id, last_synced_block) VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE SET last_synced_block = excluded.last_synced_block
  `).run(chainId, blockNumber);
}

// Optional: dedicated sync state for Uniswap V3 swaps per chain
export function getLastUniswapV3SyncedBlock(db: Database, chainId: ChainId): number | null {
  const row = db.query("SELECT last_synced_block FROM uniswap_sync_state WHERE chain_id = ?").get(chainId) as { last_synced_block: number } | null;
  return row?.last_synced_block ?? null;
}

export function updateLastUniswapV3SyncedBlock(db: Database, chainId: ChainId, blockNumber: number): void {
  db.query(`
    INSERT INTO uniswap_sync_state (chain_id, last_synced_block) VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE SET last_synced_block = MAX(uniswap_sync_state.last_synced_block, excluded.last_synced_block)
  `).run(chainId, blockNumber);
}

export type { Database };
