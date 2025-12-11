import { ethers } from "ethers";
import sparkLendPoolAbi from "../../abi/sparklendpool.json";

const iface = new ethers.Interface(sparkLendPoolAbi);
const LIQUIDATION_SELECTOR = iface.getFunction("liquidationCall").selector;



type Trace = {
  action: { from: string; to?: string; input?: string; gas?: string };
  result?: { gasUsed?: string };
  blockNumber: string;
  transactionHash: string;
  error?: string;
  type: string;
};

export type FailedLiquidationRow = {
  blockNumber: number;
  txHash: string;
  from: string;
  to: string;
  error: string;
  gasUsed: string;
};

export async function fetchFailedLiquidations(
  provider: ethers.JsonRpcProvider,
  poolAddress: string,
  liquidators: string[],
  fromBlock: number,
  toBlock: number,
  chunkSize = 5000,
  concurrency = 3,
  onChunkComplete?: (rows: FailedLiquidationRow[], chunkEnd: number) => void
) {
  const results: FailedLiquidationRow[] = [];

  const chunks: Array<[number, number]> = [];
  for (let start = fromBlock; start <= toBlock; start += chunkSize) {
    chunks.push([start, Math.min(start + chunkSize - 1, toBlock)]);
  }

  console.log(`Fetching failed liquidations from block ${fromBlock} to ${toBlock} in ${chunks.length} chunks...`);

  // simple bounded parallelism
  for (let i = 0; i < chunks.length; i += concurrency) {
    const batch = chunks.slice(i, i + concurrency);
    const tracesArrays = await Promise.all(
      batch.map(([start, end]) =>
        provider.send("trace_filter", [
          {
            fromBlock: ethers.toQuantity(start),
            toBlock: ethers.toQuantity(end),
            fromAddress: liquidators,
          },
        ]) as Promise<Trace[]>
      )
    );

    let chunkRows: FailedLiquidationRow[] = [];

    for (const traces of tracesArrays) {
      for (const t of traces) {
        if (
          t.type === "call" &&
          t.action.to?.toLowerCase() === poolAddress.toLowerCase() &&
          t.action.input?.startsWith(LIQUIDATION_SELECTOR) &&
          t.error // only failed calls
        ) {
          const row: FailedLiquidationRow = {
            blockNumber: Number(t.blockNumber),
            txHash: t.transactionHash,
            from: t.action.from,
            to: t.action.to!,
            error: t.error,
            gasUsed: ethers.toBigInt(t.result?.gasUsed ?? t.action?.gas ?? "0").toString(),
          };

          results.push(row);
          chunkRows.push(row);
        }
      }
    }

    if (onChunkComplete) {
      const chunkEnd = Math.min(batch[batch.length - 1][1], toBlock);
      onChunkComplete(chunkRows, chunkEnd);
      chunkRows = [];
    }
  }

  return results;
}