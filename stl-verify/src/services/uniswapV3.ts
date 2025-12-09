import type { JsonRpcProvider, Log } from "ethers";
import { Interface, Contract } from "ethers";
import type { Database } from "bun:sqlite";
import type { ChainId } from "../config/chains";
import { createTokenByAddressMap } from "../config/addressbook";
import { getLastUniswapV3SyncedBlock, updateLastUniswapV3SyncedBlock } from "../db/database";
import uniswapV3PoolAbi from "../../abi/uniswap-v3-pool.json";

// Canonical Uniswap V3 factory on Ethereum mainnet
const UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984".toLowerCase();

// Minimal ABI for PoolCreated event on the factory
const uniswapV3FactoryAbi = [
  "event PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)",
];

const poolInterface = new Interface(uniswapV3PoolAbi as any);
const factoryInterface = new Interface(uniswapV3FactoryAbi);
const SWAP_TOPIC = poolInterface.getEvent("Swap")!.topicHash as string;
const POOL_CREATED_TOPIC = factoryInterface.getEvent("PoolCreated")!.topicHash as string;

/**
 * Discover Uniswap V3 pools that are relevant to the configured tokens on a chain.
 *
 * This scans PoolCreated events from the factory and keeps pools where
 * token0 or token1 is in the address book for the given chain.
 * Discovered pools are persisted in the uniswap_v3_pools table.
 */
export async function discoverRelevantUniswapV3Pools(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  fromBlock: number,
  toBlock: number,
  chunkSize = 50_000
): Promise<string[]> {
  const tokenMap = createTokenByAddressMap(chainId);
  const pools = new Set<string>();

  const insertPoolStmt = db.query(`
    INSERT OR IGNORE INTO uniswap_v3_pools (
      chain_id, pool_address, token0, token1, fee, created_block
    ) VALUES (?, ?, ?, ?, ?, ?)
  `);

  let start = fromBlock;
  while (start <= toBlock) {
    const end = Math.min(start + chunkSize - 1, toBlock);

    const logs = (await provider.getLogs({
      address: UNISWAP_V3_FACTORY,
      fromBlock: start,
      toBlock: end,
      topics: [POOL_CREATED_TOPIC],
    })) as Log[];

    for (const log of logs) {
      const parsed = factoryInterface.parseLog(log);
      if (!parsed || parsed.name !== "PoolCreated") continue;

      const { token0, token1, fee, pool } = parsed.args as any;
      const t0 = (token0 as string).toLowerCase();
      const t1 = (token1 as string).toLowerCase();

      if (!tokenMap.has(t0) && !tokenMap.has(t1)) continue;

      const poolAddr = (pool as string).toLowerCase();
      pools.add(poolAddr);

      insertPoolStmt.run(
        chainId,
        poolAddr,
        t0,
        t1,
        Number(fee),
        log.blockNumber
      );
    }

    start = end + 1;
  }

  return Array.from(pools);
}

/**
 * Sync Uniswap V3 Swap events into the uniswap_v3_swaps table.
 *
 * This version:
 * - Optionally takes a list of pool addresses discovered from the factory
 * - If pools are provided, filters logs by address to reduce noise
 * - Filters to swaps where at least one token is in the addressbook
 * - Inserts one row per Swap into uniswap_v3_swaps
 */
export async function syncUniswapV3Swaps(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  startBlock: number,
  endBlock: number,
  chunkSize = 5_000,
  poolAddresses?: string[]
): Promise<void> {
  const tokenMap = createTokenByAddressMap(chainId);

  // todo: wrap in a data layer to separate business logic and data responsibility
  const insertStmt = db.query(`
    INSERT OR IGNORE INTO uniswap_v3_swaps (
      chain_id, pool_address, token0, token1, fee,
      tx_hash, log_index, block_number, block_timestamp,
      sender, recipient, amount0_raw, amount1_raw,
      sqrt_price_x96, liquidity, tick
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let from = startBlock;
  while (from <= endBlock) {
    const to = Math.min(from + chunkSize - 1, endBlock);

    const logs = (await provider.getLogs({
      fromBlock: from,
      toBlock: to,
      topics: [SWAP_TOPIC],
      address: poolAddresses && poolAddresses.length > 0 ? poolAddresses : undefined,
    })) as Log[];

    // Simple caches to avoid repeated RPC calls
    const poolMetaCache = new Map<string, { token0: string; token1: string; fee: number }>();
    const blockTimeCache = new Map<number, number>();

    for (const log of logs) {
      const poolAddress = log.address.toLowerCase();

      let meta = poolMetaCache.get(poolAddress);
      if (!meta) {
        const pool = new Contract(poolAddress, uniswapV3PoolAbi as any, provider);
        const [token0, token1, fee] = await Promise.all([
          pool.token0(),
          pool.token1(),
          pool.fee(),
        ]);
        meta = {
          token0: (token0 as string).toLowerCase(),
          token1: (token1 as string).toLowerCase(),
          fee: Number(fee),
        };
        poolMetaCache.set(poolAddress, meta);
      }

      // Filter: only keep swaps where at least one side is in our addressbook
      if (!tokenMap.has(meta.token0) && !tokenMap.has(meta.token1)) {
        continue;
      }

      const parsed = poolInterface.parseLog(log);
      if (!parsed || parsed.name !== "Swap") continue;

      const { sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick } =
        parsed.args as any;

      let timestamp = blockTimeCache.get(log.blockNumber);
      if (timestamp === undefined) {
        const block = await provider.getBlock(log.blockNumber);
        if (!block) continue;
        timestamp = block.timestamp;
        blockTimeCache.set(log.blockNumber, timestamp);
      }

      insertStmt.run(
        chainId,
        poolAddress,
        meta.token0,
        meta.token1,
        meta.fee,
        (log.transactionHash ?? "").toLowerCase(),
        log.index,
        log.blockNumber,
        timestamp,
        (sender as string).toLowerCase(),
        (recipient as string).toLowerCase(),
        amount0.toString(),
        amount1.toString(),
        sqrtPriceX96.toString(),
        liquidity.toString(),
        Number(tick)
      );
    }

    updateLastUniswapV3SyncedBlock(db, chainId, to);
    from = to + 1;
  }
}

/**
 * Convenience wrapper that discovers relevant pools (if none stored yet)
 * and continues from the last synced block.
 */
export async function syncUniswapV3SwapsFromCursor(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  fromBlockInclusive: number,
  toBlockInclusive: number,
  chunkSize = 5_000
): Promise<void> {
  const last = getLastUniswapV3SyncedBlock(db, chainId);
  const effectiveStart = last !== null ? Math.max(last + 1, fromBlockInclusive) : fromBlockInclusive;
  if (effectiveStart > toBlockInclusive) return;

  // Try to load existing pools from DB first
  const existingPools = db
    .query("SELECT pool_address FROM uniswap_v3_pools WHERE chain_id = ?")
    .all(chainId) as { pool_address: string }[];

  let pools: string[];

  if (existingPools.length > 0) {
    pools = existingPools.map((p) => p.pool_address.toLowerCase());
  } else {
    // If no pools stored yet, discover them once over the full range
    pools = await discoverRelevantUniswapV3Pools(db, provider, chainId, fromBlockInclusive, toBlockInclusive);
  }

  await syncUniswapV3Swaps(db, provider, chainId, effectiveStart, toBlockInclusive, chunkSize, pools);
}
