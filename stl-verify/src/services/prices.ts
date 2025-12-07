/**
 * Token price fetching and historical sync service
 */

import { ethers } from "ethers";
import type { ChainId } from "../config/chains";
import { ChainProtocols, getProtocolAddresses, getTokens } from "../config/addressbook";
import { createMulticall, createProvider } from "../providers/rpc";
import aaveOracleAbi from "../../abi/aaveoracle.json";

export interface TokenPrice {
  blockNumber: number;
  timestamp: number;
  tokenAddress: string;
  tokenSymbol: string;
  priceUsd: string;
}


export async function fetchSparkLendTokenPrices(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  blockNumber?: number,
): Promise<TokenPrice[]> {
  return fetchTokenPrices(provider, chainId, "sparklend", blockNumber);
}

/**
 * Fetch current prices for all tokens on a chain from the Oracle
 */
async function fetchTokenPrices(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  protocol: keyof ChainProtocols,
  blockNumber?: number,
): Promise<TokenPrice[]> {
  const protocolAddresses = getProtocolAddresses(chainId, protocol)
  
  if (!protocolAddresses?.oracle) {
    throw new Error(`Oracle not configured for chain: ${chainId}`);
  }
  
  const oracleAddress = protocolAddresses.oracle;
  const oracle = new ethers.Contract(oracleAddress, aaveOracleAbi, provider);
  const tokens = getTokens(chainId);

  if (tokens.length === 0) {
    console.warn(`No tokens configured for chain: ${chainId}`);
    return [];
  }

  const tokenAddresses = tokens.map(t => t.address);
  
  // Get block info for timestamp
  const blockTag = blockNumber || "latest";
  const block = await provider.getBlock(blockTag);
  if (!block) {
    throw new Error("Could not fetch block");
  }

  // Fetch BASE_CURRENCY_UNIT to determine decimals dynamically
  const baseCurrencyUnit: bigint = await oracle.BASE_CURRENCY_UNIT({ blockTag });
  const decimals = Math.log10(Number(baseCurrencyUnit));

  // Fetch all prices in a single call
  const prices: bigint[] = await oracle.getAssetsPrices(tokenAddresses, { blockTag });

  const tokenPrices: TokenPrice[] = [];

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    const priceRaw = prices[i];

    // Convert to USD decimal string
    const priceUsd = ethers.formatUnits(priceRaw, decimals);

    tokenPrices.push({
      blockNumber: block.number,
      timestamp: block.timestamp,
      tokenAddress: token.address.toLowerCase(),
      tokenSymbol: token.symbol,
      priceUsd
    });
  }

  return tokenPrices;
}

/**
 * Custom timeout error for cleaner error handling
 */
class TimeoutError extends Error {
  constructor(public readonly context: string, public readonly ms: number) {
    super(`Timeout after ${ms}ms: ${context}`);
    this.name = "TimeoutError";
  }
}

/**
 * Wrap a promise with a timeout
 */
function withTimeout<T>(promise: Promise<T>, ms: number, context: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new TimeoutError(context, ms)), ms)
    )
  ]);
}

/**
 * Retry a function with exponential backoff
 */
async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelayMs: number = 1000
): Promise<T> {
  let lastError: Error | undefined;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < maxRetries) {
        const delay = baseDelayMs * Math.pow(2, attempt);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

/**
 * Process a single block and return prices
 */
async function fetchPricesAtBlock(
  provider: ethers.JsonRpcProvider,
  multicall: ethers.Contract,
  oracleInterface: ethers.Interface,
  calls: { target: string; allowFailure: boolean; callData: string }[],
  tokens: { address: string; symbol: string }[],
  decimals: number,
  blockNumber: number,
  timeoutMs: number = 30000
): Promise<TokenPrice[] | null> {
  // Wrap RPC calls with timeout and retry
  const block = await withRetry(
    () => withTimeout(provider.getBlock(blockNumber), timeoutMs, `getBlock(${blockNumber})`),
    3
  );
  if (!block) {
    return null;
  }

  const results: { success: boolean; returnData: string }[] = await withRetry(
    () => withTimeout(
      multicall.aggregate3(calls, { blockTag: blockNumber }),
      timeoutMs,
      `multicall at block ${blockNumber}`
    ),
    3
  );
  const tokenPrices: TokenPrice[] = [];

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    const result = results[i];

    if (!result.success || result.returnData === "0x") {
      continue;
    }

    try {
      const decoded = oracleInterface.decodeFunctionResult("getAssetPrice", result.returnData);
      const priceRaw = decoded[0] as bigint;

      if (priceRaw === 0n) {
        continue;
      }

      const priceUsd = ethers.formatUnits(priceRaw, decimals);

      tokenPrices.push({
        blockNumber: block.number,
        timestamp: block.timestamp,
        tokenAddress: token.address.toLowerCase(),
        tokenSymbol: token.symbol,
        priceUsd
      });
    } catch {
      // Decode error, skip this token
    }
  }

  return tokenPrices;
}

/**
 * Sync historical prices using Multicall for efficiency with concurrent fetching
 */
export async function syncHistoricalPrices(
  initialProvider: ethers.JsonRpcProvider,
  chainId: ChainId,
  startBlock: number,
  endBlock: number,
  blockInterval: number = 100,
  onSnapshot?: (prices: TokenPrice[], blockNumber: number) => void,
  concurrency: number = 5
): Promise<{ snapshotsProcessed: number; errors: number }> {
  // Mutable provider/multicall - can be recreated on connection issues
  let provider = initialProvider;
  
  // Setup oracle
  const protocolAddresses = getProtocolAddresses(chainId, "sparklend") || getProtocolAddresses(chainId, "aave");
  if (!protocolAddresses?.oracle) {
    throw new Error(`Oracle not configured for chain: ${chainId}`);
  }  
  const oracleAddress = protocolAddresses.oracle;
  const oracle = new ethers.Contract(oracleAddress, aaveOracleAbi, provider);

  // Setup tokens
  const tokens = getTokens(chainId);
  if (tokens.length === 0) {
    console.warn(`No tokens configured for chain: ${chainId}`);
    return { snapshotsProcessed: 0, errors: 0 };
  }
  
  // Get BASE_CURRENCY_UNIT once (it doesn't change)
  const baseCurrencyUnit: bigint = await oracle.BASE_CURRENCY_UNIT();
  const decimals = Math.log10(Number(baseCurrencyUnit));
  
  // Prepare calldata for each token's getAssetPrice call (reusable across provider recreations)
  let multicall = createMulticall(provider);
  const oracleInterface = new ethers.Interface(aaveOracleAbi);
  const calls = tokens.map(token => ({
    target: oracleAddress,
    allowFailure: true,
    callData: oracleInterface.encodeFunctionData("getAssetPrice", [token.address])
  }));
  
  // Build list of blocks to process
  const blockNumbers: number[] = [];
  for (let b = startBlock; b <= endBlock; b += blockInterval) {
    blockNumbers.push(b);
  }
  
  const totalSnapshots = blockNumbers.length;
  console.log(`\nSyncing historical prices on ${chainId} from block ${startBlock} to ${endBlock}`);
  console.log(`Interval: every ${blockInterval} blocks, Concurrency: ${concurrency}`);
  console.log(`Total snapshots: ${totalSnapshots}`);

  let snapshotsProcessed = 0;
  let errors = 0;
  let consecutiveErrorBatches = 0;
  let currentConcurrency = concurrency;
  const MIN_CONCURRENCY = 1;

  // Process blocks in concurrent batches with adaptive concurrency
  let i = 0;
  while (i < blockNumbers.length) {
    const batch = blockNumbers.slice(i, i + currentConcurrency);
    
    const results = await Promise.allSettled(
      batch.map(blockNumber =>
        fetchPricesAtBlock(provider, multicall, oracleInterface, calls, tokens, decimals, blockNumber)
          .then(prices => ({ blockNumber, prices }))
      )
    );

    let batchSuccesses = 0;
    const failedBlocks: number[] = [];

    // Process results in order
    for (let j = 0; j < results.length; j++) {
      const result = results[j];
      const blockNumber = batch[j];
      
      if (result.status === "rejected") {
        failedBlocks.push(blockNumber);
        const reason = result.reason;
        // Clean, concise error logging
        if (errors < 10) {
          if (reason instanceof TimeoutError) {
            console.error(`⚠ Timeout: ${reason.context}`);
          } else {
            const msg = reason instanceof Error ? reason.message : String(reason);
            console.error(`⚠ Error: ${msg}`);
          }
        } else if (errors === 10) {
          console.error("⚠ Too many errors, suppressing further messages...");
        }
        errors++;
        continue;
      }

      const { prices } = result.value;
      if (prices === null) {
        failedBlocks.push(blockNumber);
        errors++;
        continue;
      }

      batchSuccesses++;
      snapshotsProcessed++;
      if (prices.length > 0 && onSnapshot) {
        onSnapshot(prices, blockNumber);
      }
    }

    // Progress logging
    const progress = ((i + batch.length) / totalSnapshots * 100).toFixed(1);
    const lastBlock = batch[batch.length - 1];
    const concurrencyInfo = currentConcurrency !== concurrency ? ` (c=${currentConcurrency})` : "";
    console.log(`[${progress}%] Block ${lastBlock} - ${batchSuccesses}/${batch.length} ok${concurrencyInfo}`);

    // Adaptive concurrency based on batch results
    if (failedBlocks.length === batch.length) {
      // Entire batch failed - likely RPC is overwhelmed or connection stale
      consecutiveErrorBatches++;
      
      if (currentConcurrency > MIN_CONCURRENCY) {
        currentConcurrency = Math.max(MIN_CONCURRENCY, Math.floor(currentConcurrency / 2));
        console.log(`↓ Reducing concurrency to ${currentConcurrency}, pausing 5s...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
      } else if (consecutiveErrorBatches >= 3) {
        // Multiple consecutive failures at min concurrency - connection is likely stale
        // Recreate provider to get fresh connections
        console.log(`🔄 Recreating RPC connection...`);
        provider = createProvider(chainId);
        multicall = createMulticall(provider);
        consecutiveErrorBatches = 0;
        await new Promise(resolve => setTimeout(resolve, 2000));
      } else {
        // Already at minimum concurrency, just wait longer
        console.log(`⏸ RPC unresponsive, waiting 10s before retry...`);
        await new Promise(resolve => setTimeout(resolve, 10000));
      }
      
      // Don't advance i - retry the same batch
      continue;
    } else if (failedBlocks.length > 0) {
      // Partial failure - some succeeded
      consecutiveErrorBatches = 0;
      // Add a small delay to ease pressure
      await new Promise(resolve => setTimeout(resolve, 1000));
    } else {
      // All succeeded
      consecutiveErrorBatches = 0;
      // Gradually restore concurrency if we've been running at reduced levels
      if (currentConcurrency < concurrency) {
        currentConcurrency = Math.min(concurrency, currentConcurrency + 1);
        if (currentConcurrency > 1) {
          console.log(`↑ Increasing concurrency to ${currentConcurrency}`);
        }
      }
    }

    // Move to next batch
    i += currentConcurrency;
  }

  console.log(`\nPrice sync complete! Snapshots: ${snapshotsProcessed}, Errors: ${errors}`);

  return { snapshotsProcessed, errors };
}
