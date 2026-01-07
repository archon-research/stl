#!/usr/bin/env tsx
/**
 * Job Runner Script
 * 
 * Runs Sparklend jobs (price capture, snapshot calculation) via CLI.
 * Uses Ponder's database context for direct database access.
 * 
 * Usage:
 *   npm run job price-capture mainnet 16776401 17000000
 *   npm run job snapshot-calculation mainnet 16776401 17000000
 *   npm run job price-capture gnosis 29817457 30000000
 */

import { createClient } from "viem";
import { http } from "viem";
import { mainnet, gnosis } from "viem/chains";
// @ts-ignore - pg types are in devDependencies, may need: npm install
import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";

// Import job functions and schemas
import { runPriceCaptureJob } from "../protocols/sparklend/jobs/price-capture";
import { runSnapshotCalculationJob } from "../protocols/sparklend/jobs/snapshot-calculation";
import * as mainnetSchema from "../protocols/sparklend/schema/chains/mainnet";
import * as gnosisSchema from "../protocols/sparklend/schema/chains/gnosis";

// Create context with real database connection
async function createJobContext(chainName: string) {
  // Get RPC URL from environment
  const rpcUrl = chainName === "mainnet" 
    ? process.env.PONDER_RPC_URL_1 
    : process.env.PONDER_RPC_URL_100;

  if (!rpcUrl) {
    throw new Error(`Missing RPC URL for ${chainName}. Set PONDER_RPC_URL_${chainName === "mainnet" ? "1" : "100"}`);
  }

  const chainConfig = chainName === "mainnet" ? mainnet : gnosis;

  // Create viem client
  const client = createClient({
    chain: chainConfig,
    transport: http(rpcUrl),
  });

  // Create database connection
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL environment variable is required");
  }

  const pool = new Pool({
    connectionString: databaseUrl,
  });

  const db = drizzle(pool);

  return {
    db: db as any,
    client,
    contracts: {}, // Empty, not needed for jobs
    chain: chainConfig,
    _cleanup: async () => {
      await pool.end();
    },
  } as any; // Cast to Context type
}

// Run the job
async function main() {
  // Parse CLI arguments
  const args = process.argv.slice(2);
  const [jobType, chain, startBlock, endBlock, interval] = args;

  if (!jobType || !chain || !startBlock || !endBlock) {
    console.error(`
Usage: npm run job <job-type> <chain> <startBlock> <endBlock> [interval]

Job Types:
  - price-capture: Capture asset prices at block intervals
  - snapshot-calculation: Calculate user health factor snapshots

Chains:
  - mainnet: Ethereum Mainnet
  - gnosis: Gnosis Chain

Examples:
  npm run job price-capture mainnet 16776401 17000000 300
  npm run job snapshot-calculation mainnet 16776401 17000000 7200
  npm run job price-capture gnosis 29817457 30000000 300
`);
    process.exit(1);
  }

  const start = parseInt(startBlock);
  const end = parseInt(endBlock);
  const intervalBlocks = interval ? parseInt(interval) : (jobType === "price-capture" ? 300 : 7200);

  console.log(`\n🚀 Starting ${jobType} job for ${chain}`);
  console.log(`   Blocks: ${start} → ${end}`);
  console.log(`   Interval: ${intervalBlocks}\n`);

  const context = await createJobContext(chain);

  try {
    if (jobType === "price-capture") {
      if (chain === "mainnet") {
        await runPriceCaptureJob(context, {
          chainId: "mainnet",
          chainName: "Mainnet",
          protocolType: "sparklend",
          startBlock: start,
          endBlock: end,
          interval: intervalBlocks,
          assetPriceSnapshotTable: mainnetSchema.SparklendMainnetAssetPriceSnapshot,
          reserveDataUpdatedTable: mainnetSchema.SparklendMainnetReserveDataUpdated,
        });
      } else if (chain === "gnosis") {
        await runPriceCaptureJob(context, {
          chainId: "gnosis",
          chainName: "Gnosis",
          protocolType: "sparklend",
          startBlock: start,
          endBlock: end,
          interval: intervalBlocks,
          assetPriceSnapshotTable: gnosisSchema.SparklendGnosisAssetPriceSnapshot,
          reserveDataUpdatedTable: gnosisSchema.SparklendGnosisReserveDataUpdated,
        });
      }
    } else if (jobType === "snapshot-calculation") {
      if (chain === "mainnet") {
        await runSnapshotCalculationJob(context, {
          chainId: "mainnet",
          chainName: "Mainnet",
          protocolType: "sparklend",
          startBlock: start,
          endBlock: end,
          interval: intervalBlocks,
          userHealthFactorHistoryTable: mainnetSchema.SparklendMainnetUserHealthFactorHistory,
          userPositionBreakdownTable: mainnetSchema.SparklendMainnetUserPositionBreakdown,
          userSupplyPositionTable: mainnetSchema.SparklendMainnetUserSupplyPosition,
          userBorrowPositionTable: mainnetSchema.SparklendMainnetUserBorrowPosition,
          activeUserTable: mainnetSchema.SparklendMainnetActiveUser,
          assetPriceSnapshotTable: mainnetSchema.SparklendMainnetAssetPriceSnapshot,
          reserveDataUpdatedTable: mainnetSchema.SparklendMainnetReserveDataUpdated,
        });
      } else if (chain === "gnosis") {
        await runSnapshotCalculationJob(context, {
          chainId: "gnosis",
          chainName: "Gnosis",
          protocolType: "sparklend",
          startBlock: start,
          endBlock: end,
          interval: intervalBlocks,
          userHealthFactorHistoryTable: gnosisSchema.SparklendGnosisUserHealthFactorHistory,
          userPositionBreakdownTable: gnosisSchema.SparklendGnosisUserPositionBreakdown,
          userSupplyPositionTable: gnosisSchema.SparklendGnosisUserSupplyPosition,
          userBorrowPositionTable: gnosisSchema.SparklendGnosisUserBorrowPosition,
          activeUserTable: gnosisSchema.SparklendGnosisActiveUser,
          assetPriceSnapshotTable: gnosisSchema.SparklendGnosisAssetPriceSnapshot,
          reserveDataUpdatedTable: gnosisSchema.SparklendGnosisReserveDataUpdated,
        });
      }
    } else {
      throw new Error(`Unknown job type: ${jobType}`);
    }

    console.log(`\n✅ Job completed successfully!\n`);
  } catch (error) {
    console.error(`\n❌ Job failed:`, error);
    process.exit(1);
  } finally {
    await context._cleanup();
  }
}

// Only run if this script is executed directly (not imported)
const isMainModule = process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/'));
if (isMainModule || process.argv[1]?.includes('run-job')) {
  main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
}

