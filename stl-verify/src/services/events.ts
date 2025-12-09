/**
 * SparkLend event fetching and processing service
 */

import { ethers } from "ethers";
import type { ChainId } from "../config/chains";
import { getProtocolAddresses } from "../config/addressbook";
import sparkLendPoolAbi from "../../abi/sparklendpool.json";

// Event types
export interface SupplyEvent {
  blockNumber: number;
  transactionHash: string;
  logIndex: number;
  reserve: string;
  user: string;
  onBehalfOf: string;
  amount: string;
  referralCode: number;
}

export interface BorrowEvent {
  blockNumber: number;
  transactionHash: string;
  logIndex: number;
  reserve: string;
  user: string;
  onBehalfOf: string;
  amount: string;
  interestRateMode: number;
  borrowRate: string;
  referralCode: number;
}

export interface LiquidationCallEvent {
  blockNumber: number;
  transactionHash: string;
  logIndex: number;
  collateralAsset: string;
  debtAsset: string;
  user: string;
  debtToCover: string;
  liquidatedCollateralAmount: string;
  liquidator: string;
  receiveAToken: boolean;
}

export interface WithdrawEvent {
  blockNumber: number;
  transactionHash: string;
  logIndex: number;
  reserve: string;
  user: string;
  to: string;
  amount: string;
}

export interface RepayEvent {
  blockNumber: number;
  transactionHash: string;
  logIndex: number;
  reserve: string;
  user: string;
  repayer: string;
  amount: string;
  useATokens: boolean;
}

export interface SparkLendEvents {
  supply: SupplyEvent[];
  borrow: BorrowEvent[];
  liquidationCall: LiquidationCallEvent[];
  withdraw: WithdrawEvent[];
  repay: RepayEvent[];
}

/**
 * Query SparkLend Pool events from a chain
 */
export async function querySparkLendEvents(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  fromBlock: number,
  toBlock: number | string = "latest"
): Promise<SparkLendEvents> {
  const protocolAddresses = getProtocolAddresses(chainId, "sparklend");

  if (!protocolAddresses?.pool) {
    throw new Error(`SparkLend pool not configured for chain: ${chainId}`);
  }

  const poolAddress = protocolAddresses.pool;
  const poolContract = new ethers.Contract(poolAddress, sparkLendPoolAbi, provider);

  console.log(`Querying SparkLend events on ${chainId} from block ${fromBlock} to ${toBlock}...`);
  console.log(`Pool address: ${poolAddress}`);

  const events: SparkLendEvents = {
    supply: [],
    borrow: [],
    liquidationCall: [],
    withdraw: [],
    repay: []
  };

  try {
    // Query Supply events
    const supplyFilter = poolContract.filters.Supply();
    const supplyLogs = await poolContract.queryFilter(supplyFilter, fromBlock, toBlock);
    events.supply = supplyLogs.map((log) => {
      const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
      return {
        blockNumber: log.blockNumber,
        transactionHash: log.transactionHash,
        logIndex: log.index,
        reserve: parsed?.args.reserve,
        user: parsed?.args.user,
        onBehalfOf: parsed?.args.onBehalfOf,
        amount: parsed?.args.amount.toString(),
        referralCode: Number(parsed?.args.referralCode)
      } as SupplyEvent;
    });
    console.log(`Found ${events.supply.length} Supply events`);

    // Query Borrow events
    const borrowFilter = poolContract.filters.Borrow();
    const borrowLogs = await poolContract.queryFilter(borrowFilter, fromBlock, toBlock);
    events.borrow = borrowLogs.map((log) => {
      const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
      return {
        blockNumber: log.blockNumber,
        transactionHash: log.transactionHash,
        logIndex: log.index,
        reserve: parsed?.args.reserve,
        user: parsed?.args.user,
        onBehalfOf: parsed?.args.onBehalfOf,
        amount: parsed?.args.amount.toString(),
        interestRateMode: Number(parsed?.args.interestRateMode),
        borrowRate: parsed?.args.borrowRate.toString(),
        referralCode: Number(parsed?.args.referralCode)
      } as BorrowEvent;
    });
    console.log(`Found ${events.borrow.length} Borrow events`);

    // Query LiquidationCall events
    const liquidationFilter = poolContract.filters.LiquidationCall();
    const liquidationLogs = await poolContract.queryFilter(liquidationFilter, fromBlock, toBlock);
    events.liquidationCall = liquidationLogs.map((log) => {
      const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
      return {
        blockNumber: log.blockNumber,
        transactionHash: log.transactionHash,
        logIndex: log.index,
        collateralAsset: parsed?.args.collateralAsset,
        debtAsset: parsed?.args.debtAsset,
        user: parsed?.args.user,
        debtToCover: parsed?.args.debtToCover.toString(),
        liquidatedCollateralAmount: parsed?.args.liquidatedCollateralAmount.toString(),
        liquidator: parsed?.args.liquidator,
        receiveAToken: parsed?.args.receiveAToken
      } as LiquidationCallEvent;
    });
    console.log(`Found ${events.liquidationCall.length} LiquidationCall events`);

    // Query Withdraw events
    const withdrawFilter = poolContract.filters.Withdraw();
    const withdrawLogs = await poolContract.queryFilter(withdrawFilter, fromBlock, toBlock);
    events.withdraw = withdrawLogs.map((log) => {
      const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
      return {
        blockNumber: log.blockNumber,
        transactionHash: log.transactionHash,
        logIndex: log.index,
        reserve: parsed?.args.reserve,
        user: parsed?.args.user,
        to: parsed?.args.to,
        amount: parsed?.args.amount.toString()
      } as WithdrawEvent;
    });
    console.log(`Found ${events.withdraw.length} Withdraw events`);

    // Query Repay events
    const repayFilter = poolContract.filters.Repay();
    const repayLogs = await poolContract.queryFilter(repayFilter, fromBlock, toBlock);
    events.repay = repayLogs.map((log) => {
      const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
      return {
        blockNumber: log.blockNumber,
        transactionHash: log.transactionHash,
        logIndex: log.index,
        reserve: parsed?.args.reserve,
        user: parsed?.args.user,
        repayer: parsed?.args.repayer,
        amount: parsed?.args.amount.toString(),
        useATokens: parsed?.args.useATokens
      } as RepayEvent;
    });
    console.log(`Found ${events.repay.length} Repay events`);

  } catch (error) {
    console.error("Error querying SparkLend events:", error);
    throw error;
  }

  return events;
}

/**
 * Sync historical SparkLend events for a chain
 */
export async function syncHistoricalEvents(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  startBlock: number,
  endBlock: number,
  chunkSize: number = 10000,
  onChunkComplete?: (events: SparkLendEvents, chunkEnd: number) => void
): Promise<{ totalEvents: SparkLendEvents; chunksProcessed: number }> {
  const totalChunks = Math.ceil((endBlock - startBlock + 1) / chunkSize);

  console.log(`\n=== Syncing Historical SparkLend Events on ${chainId} ===`);
  console.log(`From block: ${startBlock} to ${endBlock}`);
  console.log(`Total chunks: ${totalChunks}`);

  const totalEvents: SparkLendEvents = {
    supply: [],
    borrow: [],
    liquidationCall: [],
    withdraw: [],
    repay: []
  };

  let chunksProcessed = 0;

  for (let chunkStart = startBlock; chunkStart <= endBlock; chunkStart += chunkSize) {
    const chunkEnd = Math.min(chunkStart + chunkSize - 1, endBlock);
    chunksProcessed++;

    console.log(`\n[Chunk ${chunksProcessed}/${totalChunks}] Blocks ${chunkStart} to ${chunkEnd}...`);

    try {
      const events = await querySparkLendEvents(provider, chainId, chunkStart, chunkEnd);

      // Accumulate totals
      totalEvents.supply.push(...events.supply);
      totalEvents.borrow.push(...events.borrow);
      totalEvents.liquidationCall.push(...events.liquidationCall);
      totalEvents.withdraw.push(...events.withdraw);
      totalEvents.repay.push(...events.repay);

      // Callback for saving to database
      if (onChunkComplete) {
        onChunkComplete(events, chunkEnd);
      }

      console.log(`Running totals - Supply: ${totalEvents.supply.length}, Borrow: ${totalEvents.borrow.length}, Liquidation: ${totalEvents.liquidationCall.length}`);
    } catch (error) {
      console.error(`Error processing chunk ${chunksProcessed}:`, error);
    }
  }

  console.log("\n=== SparkLend Events Summary ===");
  console.log(`Supply events: ${totalEvents.supply.length}`);
  console.log(`Borrow events: ${totalEvents.borrow.length}`);
  console.log(`LiquidationCall events: ${totalEvents.liquidationCall.length}`);
  console.log(`Withdraw events: ${totalEvents.withdraw.length}`);
  console.log(`Repay events: ${totalEvents.repay.length}`);

  return { totalEvents, chunksProcessed };
}
