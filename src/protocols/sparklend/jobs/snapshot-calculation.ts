import type { Context } from "ponder:registry";
import { snapshotAllActiveUsers } from "@sparklend/utils/health-factor-snapshot";
import { getPricesAtBlock } from "@sparklend/utils/daily-price-cache";

/**
 * Snapshot Calculation Job
 * 
 * Calculates historical daily snapshots of user positions and health factors
 * at specified block intervals for a given chain.
 * Can be run manually via CLI or scheduled as a cron job.
 */

export interface SnapshotCalculationJobConfig {
  chainId: string;
  chainName: string;
  protocolType: string; // e.g., "sparklend"
  startBlock: number;
  endBlock: number;
  interval: number; // blocks between snapshots (e.g., ~1 day = 7200 blocks on mainnet)
  // Schema tables needed for calculation
  userHealthFactorHistoryTable: any;
  userPositionBreakdownTable: any;
  userSupplyPositionTable: any;
  userBorrowPositionTable: any;
  activeUserTable: any;
  assetPriceSnapshotTable: any;
  reserveDataUpdatedTable: any;
}

export async function runSnapshotCalculationJob(
  context: Context,
  config: SnapshotCalculationJobConfig
): Promise<void> {
  const { chainId, chainName, protocolType, startBlock, endBlock, interval } = config;
  const PROTOCOL_ID = `${protocolType}-${chainId}`;

  console.log(`\n🎯 Starting Snapshot Calculation Job for ${chainName}`);
  console.log(`   Chain: ${chainId} (${chainName})`);
  console.log(`   Protocol: ${protocolType}`);
  console.log(`   Block Range: ${startBlock} → ${endBlock}`);
  console.log(`   Interval: ${interval} blocks (~daily)\n`);

  let snapshotsCalculated = 0;

  try {
    for (let blockNumber = startBlock; blockNumber <= endBlock; blockNumber += interval) {
      try {
        console.log(`  Processing block ${blockNumber}...`);
        
        // Fetch prices from database or RPC
        const priceMap = await getPricesAtBlock(
          context,
          PROTOCOL_ID,
          config.assetPriceSnapshotTable,
          config.reserveDataUpdatedTable,
          BigInt(blockNumber)
        );
        
        // Get timestamp from block
        const block = await context.client.getBlock({ blockNumber: BigInt(blockNumber) });
        const timestamp = block.timestamp;
        
        await snapshotAllActiveUsers(
          context,
          chainId,
          PROTOCOL_ID,
          config.userHealthFactorHistoryTable,
          config.userPositionBreakdownTable,
          config.userSupplyPositionTable,
          config.userBorrowPositionTable,
          config.activeUserTable,
          BigInt(blockNumber),
          timestamp,
          priceMap
        );
        
        snapshotsCalculated++;
        
        if (snapshotsCalculated % 10 === 0) {
          console.log(`  Progress: ${snapshotsCalculated} snapshots calculated`);
        }
      } catch (error: any) {
        console.error(`  ❌ Error at block ${blockNumber}: ${error?.message}`);
        // Continue with next block
      }
    }

    console.log(`\n✅ Snapshot Calculation Job Complete for ${chainName}`);
    console.log(`   Total snapshots calculated: ${snapshotsCalculated}\n`);
  } catch (error) {
    console.error(`\n❌ Snapshot Calculation Job Failed for ${chainName}:`, error);
    throw error;
  }
}
