/**
 * STL-Verify: Multi-chain blockchain data fetching service
 * 
 * This service fetches and stores blockchain data from multiple chains
 * including Ethereum, Base, Arbitrum, and Optimism.
 */

import type { Database } from "bun:sqlite";
import type { JsonRpcProvider } from "ethers";
import { type ChainId, getChainConfig, getSupportedChains } from "./config/chains";
import { getTokens, getBlockMarkers, getProtocolAddresses } from "./config/addressbook";
import { initDatabase, getLastSyncedBlock, updateLastSyncedBlock, getLastPriceSyncedBlock, updateLastPriceSyncedBlock } from "./db/database";
import { saveEventsToDatabase } from "./db/events";
import { savePricesToDatabase, getLatestPrice, getAllLatestPrices } from "./db/prices";
import { createProvider, testRpcConnection } from "./providers/rpc";
import { querySparkLendEvents, syncHistoricalEvents } from "./services/events";
import { fetchSparkLendTokenPrices, syncHistoricalPrices } from "./services/prices";

// Parse command line arguments
const args = process.argv.slice(2);
const chainArg = args.find(arg => arg.startsWith("--chain="))?.split("=")[1] as ChainId | undefined;
const syncEvents = args.includes("--sync-events");
const syncPrices = args.includes("--sync-prices");
const showPrices = args.includes("--show-prices");
const testConnection = args.includes("--test");

/**
 * Handle --sync-events: Sync historical SparkLend events
 */
async function handleSyncEvents(
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
async function handleSyncPrices(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  currentBlock: number
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
      }
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
 * Handle --show-prices: Display current token prices
 */
async function handleShowPrices(
  db: Database,
  provider: JsonRpcProvider,
  chainId: ChainId,
  chainName: string
): Promise<void> {
  const tokens = getTokens(chainId);
  
  if (tokens.length === 0) {
    return;
  }

  console.log(`\n=== Current Token Prices on ${chainName} ===`);
  console.log("-".repeat(50));

  try {
    const livePrices = await fetchSparkLendTokenPrices(provider, chainId);
    for (const price of livePrices) {
      const formattedPrice = parseFloat(price.priceUsd).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 6
      });
      console.log(`${price.tokenSymbol.padEnd(8)} $${formattedPrice}`);
    }
  } catch (error) {
    console.log("Could not fetch live prices:", error instanceof Error ? error.message : error);
    console.log("Showing cached prices:");
    const cachedPrices = getAllLatestPrices(db, chainId);
    for (const price of cachedPrices) {
      const formattedPrice = parseFloat(price.priceUsd).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 6
      });
      console.log(`${price.tokenSymbol.padEnd(8)} $${formattedPrice}`);
    }
  }
  
  console.log("-".repeat(50));
}

/**
 * Show usage help
 */
function showUsageHelp(): void {
  console.log("\n=== Usage ===");
  console.log("  bun run src/index.ts [options]");
  console.log("\nOptions:");
  console.log("  --chain=<chain>    Target chain (ethereum, base, arbitrum, optimism)");
  console.log("  --sync-events      Sync historical SparkLend events");
  console.log("  --sync-prices      Sync historical token prices");
  console.log("  --show-prices      Display current token prices");
  console.log("  --test             Test RPC connection only");
  console.log("\nExamples:");
  console.log("  bun run src/index.ts --chain=ethereum --sync-events");
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

  if (testConnection) {
    console.log("\nConnection test successful!");
    return;
  }

  // Check if SparkLend is available on this chain
  const sparklendAddresses = getProtocolAddresses(chainId, "sparklend");
  if (!sparklendAddresses?.pool) {
    console.warn(`\n⚠ SparkLend is not configured for ${chainConfig.name}`);
    console.log("Available protocols may be limited.");
  }

  // Initialize database
  const db = initDatabase();
  console.log("\n✓ Database initialized");

  // Create provider
  const provider = createProvider(chainId);
  const currentBlock = await provider.getBlockNumber();
  console.log(`Current block: ${currentBlock}`);

  // Handle CLI commands
  if (syncEvents) {
    await handleSyncEvents(db, provider, chainId, currentBlock);
  }

  if (syncPrices) {
    await handleSyncPrices(db, provider, chainId, currentBlock);
  }

  if (showPrices || (!syncEvents && !syncPrices)) {
    await handleShowPrices(db, provider, chainId, chainConfig.name);
  }

  if (!syncEvents && !syncPrices && !showPrices && !testConnection) {
    showUsageHelp();
  }

  db.close();
  console.log("\n✓ Done");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

// Re-export for module usage
export {
  // Config
  type ChainId,
  getChainConfig,
  getSupportedChains,
  getTokens,
  getBlockMarkers,
  getProtocolAddresses,
  
  // Database
  initDatabase,
  getLastSyncedBlock,
  updateLastSyncedBlock,
  getLastPriceSyncedBlock,
  updateLastPriceSyncedBlock,
  saveEventsToDatabase,
  savePricesToDatabase,
  getLatestPrice,
  getAllLatestPrices,
  
  // Providers
  createProvider,
  testRpcConnection,
  
  // Services
  querySparkLendEvents,
  syncHistoricalEvents,
  fetchSparkLendTokenPrices,
  syncHistoricalPrices
};
