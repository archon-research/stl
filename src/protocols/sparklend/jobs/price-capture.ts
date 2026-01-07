import type { Context } from "ponder:registry";
import { getAssetsPrices } from "@sparklend/utils/oracle";
import { getAllReserveAssets } from "@sparklend/utils/reserve-assets";
import { getTokenId, getProtocolId } from "@/db/helpers";

/**
 * Price Capture Job
 * 
 * Captures asset prices at specified block intervals for a given chain.
 * Can be run manually via CLI or scheduled as a cron job.
 */

export interface PriceCaptureJobConfig {
  chainId: string;
  chainName: string;
  protocolType: string; // e.g., "sparklend"
  startBlock: number;
  endBlock: number;
  interval: number; // blocks between captures
  assetPriceSnapshotTable: any;
  reserveDataUpdatedTable: any;
}

export async function runPriceCaptureJob(
  context: Context,
  config: PriceCaptureJobConfig
): Promise<void> {
  const { db } = context;
  const { chainId, chainName, protocolType, startBlock, endBlock, interval, assetPriceSnapshotTable, reserveDataUpdatedTable } = config;
  const protocolId = getProtocolId(protocolType, chainName);

  console.log(`\n💰 Starting Price Capture Job for ${chainName}`);
  console.log(`   Chain: ${chainId} (${chainName})`);
  console.log(`   Block Range: ${startBlock} → ${endBlock}`);
  console.log(`   Interval: ${interval} blocks\n`);

  let capturedBlocks = 0;
  let totalPricesCaptured = 0;

  try {
    // Get all reserve assets once
    const allAssets = await getAllReserveAssets(context, reserveDataUpdatedTable);
    
    if (allAssets.length === 0) {
      console.log(`  ⚠️ No reserve assets found for ${chainName} - skipping`);
      return;
    }

    console.log(`  Found ${allAssets.length} assets to track`);

    // Iterate through blocks at specified interval
    for (let blockNumber = startBlock; blockNumber <= endBlock; blockNumber += interval) {
      try {
        // Get block timestamp
        const block = await context.client.getBlock({ blockNumber: BigInt(blockNumber) });
        const timestamp = block.timestamp;
        
        // Fetch prices for all assets at this block
        const prices = await getAssetsPrices(allAssets, BigInt(blockNumber));

        const priceInserts: Array<{
          id: string;
          protocolId: string;
          reserveId: string;
          blockNumber: bigint;
          timestamp: bigint;
          priceUSD: bigint;
        }> = [];

        for (let i = 0; i < allAssets.length; i++) {
          const asset = allAssets[i];
          const price = prices[i];

          if (!asset || price === undefined || price === 0n) {
            continue;
          }

          const reserveId = getTokenId(chainName, asset);
          const id = `${protocolId}-${reserveId}-${blockNumber}`;
          priceInserts.push({
            id,
            protocolId,
            reserveId,
            blockNumber: BigInt(blockNumber),
            timestamp,
            priceUSD: price,
          });
        }

        // Batch insert all prices for this block
        if (priceInserts.length > 0) {
          await db
            .insert(assetPriceSnapshotTable)
            .values(priceInserts)
            .onConflictDoNothing();

          totalPricesCaptured += priceInserts.length;
        }

        capturedBlocks++;
        
        if (capturedBlocks % 10 === 0) {
          console.log(`  Progress: ${capturedBlocks} blocks processed, ${totalPricesCaptured} prices captured`);
        }
      } catch (error: any) {
        console.error(`  ❌ Error at block ${blockNumber}: ${error?.message}`);
        // Continue with next block
      }
    }

    console.log(`\n✅ Price Capture Job Complete for ${chainName}`);
    console.log(`   Blocks Processed: ${capturedBlocks}`);
    console.log(`   Total Prices Captured: ${totalPricesCaptured}\n`);
  } catch (error) {
    console.error(`\n❌ Price Capture Job Failed for ${chainName}:`, error);
    throw error;
  }
}

