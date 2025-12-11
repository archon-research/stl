/**
 * STL-Verify: Multi-chain blockchain data fetching service
 * 
 * This service fetches and stores blockchain data from multiple chains
 */

import type { Database } from "bun:sqlite";
import type { JsonRpcProvider } from "ethers";
import { type ChainId, getChainConfig } from "./config/chains";
import { getBlockMarkers, getProtocolAddresses } from "./config/addressbook";
import { initDatabase, getLastSyncedBlock, updateLastSyncedBlock, getLastPriceSyncedBlock, updateLastPriceSyncedBlock, getLastFailedTxSyncedBlock, updateLastFailedTxSyncedBlock } from "./db/database";
import { saveEventsToDatabase, getTopLiquidators } from "./db/events";
import { saveFailedLiquidations } from "./db/failedTransactions";
import { savePricesToDatabase } from "./db/prices";
import { createProvider, testRpcConnection } from "./providers/rpc";
import { syncHistoricalEvents } from "./services/events";
import { syncHistoricalPrices } from "./services/prices";
import { fetchFailedLiquidations } from "./services/failedTransactions";
import { syncUniswapV3SwapsFromCursor } from "./services/uniswapV3";

// Global error handlers to prevent silent crashes
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});

process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  process.exit(1);
});

// Parse command line arguments
const args = process.argv.slice(2);
const chainArg = args.find(arg => arg.startsWith("--chain="))?.split("=")[1] as ChainId | undefined;
const concurrencyArg = args.find(arg => arg.startsWith("--concurrency="))?.split("=")[1];
const concurrency = concurrencyArg ? parseInt(concurrencyArg, 10) : 1;
const syncEventsFlag = args.includes("--sync-events");
const syncPricesFlag = args.includes("--sync-prices");
const syncUniswapFlag = args.includes("--sync-uniswap-v3");
const syncFailedLiquidationsFlag = args.includes("--sync-failed-liquidations");
const testConnectionFlag = args.includes("--test");

/**
 * Handle --sync-events: Sync historical SparkLend events
 */
async function syncEvents(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  currentBlock: number
): Promise<void> {
  const sparklendAddresses = getProtocolAddresses(chainId, "sparklend");
  if (!sparklendAddresses?.pool) {
    console.log("SparkLend not configured, skipping event sync.");
    return;
  }

  const blockMarkers = getBlockMarkers(chainId);
  const lastSyncedBlock = getLastSyncedBlock(db, chainId);
  const startBlock = lastSyncedBlock ? lastSyncedBlock + 1 : (blockMarkers.poolCreation || currentBlock - 10000);

  if (startBlock <= currentBlock) {
    await syncHistoricalEvents(
      provider,
      chainId,
      startBlock,
      currentBlock,
      10000,
      (events, chunkEnd) => {
        saveEventsToDatabase(db, chainId, events);
        updateLastSyncedBlock(db, chainId, chunkEnd);
      }
    );
  } else {
    console.log("Events already synced to current block.");
  }
}

/**
 * Handle --sync-prices: Sync historical token prices
 */
async function syncPrices(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  currentBlock: number,
  concurrency: number = 5
): Promise<void> {
  const blockMarkers = getBlockMarkers(chainId);
  const lastPriceSyncedBlock = getLastPriceSyncedBlock(db, chainId);
  const oracleStart = blockMarkers.oracleOperational || blockMarkers.poolCreation || currentBlock - 10000;
  const startBlock = lastPriceSyncedBlock ? lastPriceSyncedBlock + 1 : oracleStart;

  if (startBlock <= currentBlock) {
    // Batch prices and persist less frequently for better performance
    const BATCH_COMMIT_SIZE = 20; // Commit every N snapshots
    let accumulatedPrices: import("./services/prices").TokenPrice[] = [];
    let lastBlockInBatch = startBlock;
    let snapshotCount = 0;

    await syncHistoricalPrices(
      provider,
      chainId,
      startBlock,
      currentBlock,
      50,
      (prices, blockNumber) => {
        accumulatedPrices.push(...prices);
        lastBlockInBatch = blockNumber;
        snapshotCount++;

        // Commit batch periodically or if we've accumulated many prices
        if (snapshotCount >= BATCH_COMMIT_SIZE) {
          savePricesToDatabase(db, chainId, accumulatedPrices);
          updateLastPriceSyncedBlock(db, chainId, lastBlockInBatch);
          accumulatedPrices = [];
          snapshotCount = 0;
        }
      },
      concurrency
    );

    // Flush any remaining prices
    if (accumulatedPrices.length > 0) {
      savePricesToDatabase(db, chainId, accumulatedPrices);
      updateLastPriceSyncedBlock(db, chainId, lastBlockInBatch);
    }
  } else {
    console.log("Prices already synced to current block.");
  }
}

/**
 * Handle --sync-uniswap-v3: Sync Uniswap V3 swap events for tracked tokens
 */
async function syncUniswapV3(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  currentBlock: number
): Promise<void> {
  const blockMarkers = getBlockMarkers(chainId);
  const startBlock = blockMarkers.poolCreation ?? Math.max(currentBlock - 10000, 0);

  console.log(`\nSyncing Uniswap V3 swaps from block ${startBlock} to ${currentBlock}...`);
  await syncUniswapV3SwapsFromCursor(db, provider, chainId, startBlock, currentBlock);
  console.log("Uniswap V3 swap sync completed.");
}


async function syncFailedLiquidations(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  currentBlock: number
): Promise<void> {
    const sparklendAddresses = getProtocolAddresses(chainId, "sparklend");
    if (!sparklendAddresses?.pool) {
      console.log("SparkLend not configured, skipping failed liquidations fetch.");
      return;
    }

    const blockMarkers = getBlockMarkers(chainId);
    if (!blockMarkers.poolCreation) {
      console.log("Pool creation block not set, cannot fetch failed liquidations.");
      return;
    }
    const lastFailedTxSyncedBlock = getLastFailedTxSyncedBlock(db, chainId);
    const startBlock = lastFailedTxSyncedBlock ? lastFailedTxSyncedBlock + 1 : blockMarkers.poolCreation;
    if (startBlock >= currentBlock) {
      console.log("Failed liquidations already synced to current block.");
      return;
    }

    const topLiquidators = getTopLiquidators(db, chainId, 20);
    const liquidatorAddresses = topLiquidators.map(l => l.liquidator);
    if (liquidatorAddresses.length === 0) {
      console.log("No liquidator addresses available, skipping failed liquidations fetch.");
      return;
    }
    
    await fetchFailedLiquidations(
        provider,
        sparklendAddresses.pool,
        liquidatorAddresses,
        startBlock,
        currentBlock,
        5000,
        concurrency,
        (rows, chunkEnd) => {
          console.log(`Fetched ${rows.length} failed liquidations up to block ${chunkEnd}`);
          if (rows.length > 0) {
            saveFailedLiquidations(db, chainId, rows);
          }
          updateLastFailedTxSyncedBlock(db, chainId, chunkEnd);
        }
    );
}

/**
 * Show usage help
 */
function showUsageHelp(): void {
  console.log("\n=== Usage ===");
  console.log("  bun run src/index.ts [options]");
  console.log("\nOptions:");
  console.log("  --chain=<chain>        Target chain (ethereum, base, arbitrum, optimism)");
  console.log("  --concurrency=<n>      Number of concurrent RPC requests (1-20, default: 5)");
  console.log("  --sync-events          Sync historical SparkLend events");
  console.log("  --sync-prices          Sync historical token prices");
  console.log("  --sync-uniswap-v3      Sync historical Uniswap V3 swaps for tracked tokens");
  console.log("  --sync-failed-liquidations  Sync failed SparkLend liquidation attempts (trace_filter)");
  console.log("  --test                 Test RPC connection only");
  console.log("\nExamples:");
  console.log("  bun run src/index.ts --chain=ethereum --sync-events");
  console.log("  bun run src/index.ts --chain=ethereum --sync-prices --concurrency=3");
  console.log("  bun run src/index.ts --chain=ethereum --sync-uniswap-v3");
  console.log("  bun run src/index.ts --chain=base --show-prices");
}

async function main() {
  console.log("=".repeat(60));
  console.log("STL-Verify: Multi-chain Blockchain Data Service");
  console.log("=".repeat(60));

  // Determine which chain to use
  const chainId: ChainId = chainArg || "ethereum";
  const chainConfig = getChainConfig(chainId);

  console.log(`\nTarget chain: ${chainConfig.name} (Chain ID: ${chainConfig.chainId})`);

  // Test RPC connection
  const connected = await testRpcConnection(chainId);
  if (!connected) {
    console.error("Failed to connect to RPC. Exiting.");
    process.exit(1);
  }

  if (testConnectionFlag) {
    console.log("\nConnection test successful!");
    return;
  }

  // Initialize database
  const db = initDatabase();
  console.log("\n✓ Database initialized");

  // Create provider
  const provider = createProvider(chainId);
  const currentBlock = await provider.getBlockNumber();
  console.log(`Current block: ${currentBlock}`);

  // Handle CLI commands
  if (syncEventsFlag) {
    await syncEvents(db, provider, chainId, currentBlock);
  }

  if (syncPricesFlag) {
    await syncPrices(db, provider, chainId, currentBlock, concurrency);
  }

  if (syncUniswapFlag) {
    await syncUniswapV3(db, provider, chainId, currentBlock);
  }

  if (syncFailedLiquidationsFlag) {
    await syncFailedLiquidations(db, provider, chainId, currentBlock);
  }

  if (!syncEventsFlag && !syncPricesFlag && !syncUniswapFlag && !testConnectionFlag && !syncFailedLiquidationsFlag) {
    showUsageHelp();
  }

  db.close();
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
