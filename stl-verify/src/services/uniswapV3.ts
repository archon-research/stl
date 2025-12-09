import type { JsonRpcProvider, Log } from "ethers";
import { Interface, Contract } from "ethers";
import type { Database } from "bun:sqlite";
import type { ChainId } from "../config/chains";
import { createTokenByAddressMap } from "../config/addressbook";
import { getLastUniswapV3SyncedBlock, updateLastUniswapV3SyncedBlock } from "../db/database";
import uniswapV3PoolAbi from "../../abi/uniswap-v3-pool.json";

const poolInterface = new Interface(uniswapV3PoolAbi as any);
const SWAP_TOPIC = poolInterface.getEvent("Swap").topicHash as string;

/**
 * Sync Uniswap V3 Swap events into the uniswap_v3_swaps table.
 *
 * This simple version:
 * - Fetches all Swap logs by topic (no pool address filter)
 * - Filters to swaps where at least one token is in the addressbook
 * - Inserts one row per Swap into uniswap_v3_swaps
 */
export async function syncUniswapV3Swaps(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  startBlock: number,
  endBlock: number,
  chunkSize = 5_000
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
 * Convenience wrapper that continues from the last synced block if available.
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

  await syncUniswapV3Swaps(db, provider, chainId, effectiveStart, toBlockInclusive, chunkSize);
}
