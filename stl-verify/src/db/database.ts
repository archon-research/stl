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
  db.run("PRAGMA journal_mode = WAL");
  
  // Create tables
  db.run(`
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

    -- Uniswap V3 pools discovered from factory events
    CREATE TABLE IF NOT EXISTS uniswap_v3_pools (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      pool_address TEXT NOT NULL,
      token0 TEXT NOT NULL,
      token1 TEXT NOT NULL,
      fee INTEGER NOT NULL,
      created_block INTEGER NOT NULL,
      UNIQUE(chain_id, pool_address)
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

    -- Failed liquidation call attempts (identified via trace_filter)
    CREATE TABLE IF NOT EXISTS failed_liquidation_calls (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      from_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      error TEXT NOT NULL,
      gas_used TEXT NOT NULL,
      UNIQUE(chain_id, tx_hash)
    );

    -- Sync cursor for failed liquidation call tracing
    CREATE TABLE IF NOT EXISTS failed_transaction_sync_state (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL UNIQUE,
      last_synced_block INTEGER NOT NULL
    );

    -- Reserve configuration snapshots from Spark data provider
    CREATE TABLE IF NOT EXISTS reserve_config_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      token_address TEXT NOT NULL,
      token_symbol TEXT NOT NULL,
      decimals TEXT NOT NULL,
      ltv TEXT NOT NULL,
      liquidation_threshold TEXT NOT NULL,
      liquidation_bonus TEXT NOT NULL,
      reserve_factor TEXT NOT NULL,
      usage_as_collateral_enabled INTEGER NOT NULL,
      borrowing_enabled INTEGER NOT NULL,
      stable_borrow_rate_enabled INTEGER NOT NULL,
      is_active INTEGER NOT NULL,
      is_frozen INTEGER NOT NULL,
      UNIQUE(chain_id, block_number, token_address)
    );

    -- Sync cursor for reserve configuration snapshots
    CREATE TABLE IF NOT EXISTS reserve_config_sync_state (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id TEXT NOT NULL UNIQUE,
      last_synced_block INTEGER NOT NULL
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

    CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pools_chain ON uniswap_v3_pools(chain_id);
    CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pools_token0 ON uniswap_v3_pools(token0);
    CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pools_token1 ON uniswap_v3_pools(token1);

    CREATE INDEX IF NOT EXISTS idx_uni_swaps_chain_block ON uniswap_v3_swaps(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_uni_swaps_pool ON uniswap_v3_swaps(pool_address);
    CREATE INDEX IF NOT EXISTS idx_uni_swaps_token0 ON uniswap_v3_swaps(token0);
    CREATE INDEX IF NOT EXISTS idx_uni_swaps_token1 ON uniswap_v3_swaps(token1);

    CREATE INDEX IF NOT EXISTS idx_token_volume_daily_token_date ON token_volume_daily(chain_id, token_address, date);

    CREATE INDEX IF NOT EXISTS idx_failed_liq_chain_block ON failed_liquidation_calls(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_failed_liq_from ON failed_liquidation_calls(from_address);
    CREATE INDEX IF NOT EXISTS idx_failed_liq_to ON failed_liquidation_calls(to_address);

    CREATE INDEX IF NOT EXISTS idx_reserve_cfg_chain_block ON reserve_config_snapshots(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS idx_reserve_cfg_token ON reserve_config_snapshots(token_address);
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

// Get distinct price blocks for a chain, optionally filtered by min/max block
export function getDistinctPriceBlocks(
  db: Database,
  chainId: ChainId,
  minBlock?: number,
  maxBlock?: number
): number[] {
  let sql = "SELECT DISTINCT block_number FROM token_prices WHERE chain_id = ?";
  const params: (string | number)[] = [chainId];

  if (minBlock !== undefined) {
    sql += " AND block_number >= ?";
    params.push(minBlock);
  }
  if (maxBlock !== undefined) {
    sql += " AND block_number <= ?";
    params.push(maxBlock);
  }

  sql += " ORDER BY block_number";

  const rows = db.query(sql).all(...params) as { block_number: number }[];
  return rows.map((r) => r.block_number);
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

// Sync state helpers for failed liquidation traces
export function getLastFailedTxSyncedBlock(db: Database, chainId: ChainId): number | null {
  const row = db.query("SELECT last_synced_block FROM failed_transaction_sync_state WHERE chain_id = ?").get(chainId) as { last_synced_block: number } | null;
  return row?.last_synced_block ?? null;
}

export function updateLastFailedTxSyncedBlock(db: Database, chainId: ChainId, blockNumber: number): void {
  db.query(`
    INSERT INTO failed_transaction_sync_state (chain_id, last_synced_block) VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE SET last_synced_block = MAX(failed_transaction_sync_state.last_synced_block, excluded.last_synced_block)
  `).run(chainId, blockNumber);
}

// Reserve config sync state helpers
export function getLastReserveConfigSyncedBlock(db: Database, chainId: ChainId): number | null {
  const row = db
    .query("SELECT last_synced_block FROM reserve_config_sync_state WHERE chain_id = ?")
    .get(chainId) as { last_synced_block: number } | null;
  return row?.last_synced_block ?? null;
}

export function updateLastReserveConfigSyncedBlock(db: Database, chainId: ChainId, blockNumber: number): void {
  db.query(`
    INSERT INTO reserve_config_sync_state (chain_id, last_synced_block) VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE SET last_synced_block = MAX(reserve_config_sync_state.last_synced_block, excluded.last_synced_block)
  `).run(chainId, blockNumber);
}

// Insert a batch of reserve configuration snapshots
export function insertReserveConfigSnapshots(
  db: Database,
  chainId: ChainId,
  snapshots: {
    blockNumber: number;
    tokenAddress: string;
    tokenSymbol: string;
    decimals: bigint;
    ltv: bigint;
    liquidationThreshold: bigint;
    liquidationBonus: bigint;
    reserveFactor: bigint;
    usageAsCollateralEnabled: boolean;
    borrowingEnabled: boolean;
    stableBorrowRateEnabled: boolean;
    isActive: boolean;
    isFrozen: boolean;
  }[]
): void {
  const stmt = db.query(`
    INSERT INTO reserve_config_snapshots (
      chain_id,
      block_number,
      token_address,
      token_symbol,
      decimals,
      ltv,
      liquidation_threshold,
      liquidation_bonus,
      reserve_factor,
      usage_as_collateral_enabled,
      borrowing_enabled,
      stable_borrow_rate_enabled,
      is_active,
      is_frozen
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(chain_id, block_number, token_address) DO NOTHING
  `);

  db.transaction(() => {
    for (const s of snapshots) {
      stmt.run(
        chainId,
        s.blockNumber,
        s.tokenAddress,
        s.tokenSymbol,
        s.decimals.toString(),
        s.ltv.toString(),
        s.liquidationThreshold.toString(),
        s.liquidationBonus.toString(),
        s.reserveFactor.toString(),
        s.usageAsCollateralEnabled ? 1 : 0,
        s.borrowingEnabled ? 1 : 0,
        s.stableBorrowRateEnabled ? 1 : 0,
        s.isActive ? 1 : 0,
        s.isFrozen ? 1 : 0
      );
    }
  })();
}

export type { Database };
