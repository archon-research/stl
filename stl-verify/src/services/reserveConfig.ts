import { ethers } from "ethers";
import sparkDataProviderAbi from "../../abi/spark_data_provider.json";
import type { ChainId } from "../config/chains";
import {
  getBlockMarkers,
  getProtocolAddresses,
  getTokens,
} from "../config/addressbook";
import { createMulticall } from "../providers/rpc";
import { withRetry, withTimeout } from "../utils/retry";

export interface ReserveTokenData {
  symbol: string;
  tokenAddress: string;
}

export interface ReserveConfigSnapshot {
  blockNumber: number;
  asset: string;
  symbol: string;
  decimals: bigint;
  ltv: bigint;
  liquidationThreshold: bigint;
  liquidationBonus: bigint;
  reserveFactor: bigint;
  usageAsCollateralEnabled: boolean;
  borrowingEnabled: boolean;
  stableBorrowRateEnabled: boolean;
  isActive: boolean;
  isFrozen: boolean;
}

// Typed tuple for getReserveConfigurationData return value
type ReserveConfigTuple = [
  bigint, // decimals
  bigint, // ltv
  bigint, // liquidationThreshold
  bigint, // liquidationBonus
  bigint, // reserveFactor
  boolean, // usageAsCollateralEnabled
  boolean, // borrowingEnabled
  boolean, // stableBorrowRateEnabled
  boolean, // isActive
  boolean // isFrozen
];

async function _getReserveConfig(
  dataProviderAddress: string,
  tokens: string[],
  blockTag: number,
  provider: ethers.JsonRpcProvider
): Promise<(ReserveConfigTuple | null)[]> {
  const multicall = createMulticall(provider);
  const iface = new ethers.Interface(sparkDataProviderAbi as any);

  const calls = tokens.map((token) => ({
    target: dataProviderAddress,
    allowFailure: true,
    callData: iface.encodeFunctionData("getReserveConfigurationData", [token]),
  }));

  type Aggregate3Response = { success: boolean; returnData: string };

  const results: Aggregate3Response[] = await withRetry(
    () =>
      withTimeout(
        multicall.aggregate3.staticCall(calls, { blockTag }),
        30000,
        `reserveConfig multicall at block ${blockTag}`
      ),
    3
  );

  return results.map((res) => {
    if (!res.success || res.returnData === "0x") {
      return null;
    }
    return iface.decodeFunctionResult(
      "getReserveConfigurationData",
      res.returnData
    ) as unknown as ReserveConfigTuple;
  });
}

export async function getReserveConfigsAtBlock(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  blockNumber: number
): Promise<ReserveConfigSnapshot[]> {
  const protocol = getProtocolAddresses(chainId, "sparklend");
  if (!protocol?.dataProvider) {
    throw new Error(
      `SparkLend data provider not configured for chain: ${chainId}`
    );
  }

  const tokensMeta = getTokens(chainId);
  const tokenAddresses = tokensMeta.map((t) => t.address);

  const decodedResults = await _getReserveConfig(
    protocol.dataProvider,
    tokenAddresses,
    blockNumber,
    provider
  );

  const snapshots: ReserveConfigSnapshot[] = [];

  decodedResults.forEach((result, idx) => {
    if (!result) {
      // Token not yet listed at this block; skip
      return;
    }

    const [
      decimals,
      ltv,
      liquidationThreshold,
      liquidationBonus,
      reserveFactor,
      usageAsCollateralEnabled,
      borrowingEnabled,
      stableBorrowRateEnabled,
      isActive,
      isFrozen,
    ] = result as unknown as ReserveConfigTuple;

    const token = tokensMeta[idx];

    snapshots.push({
      blockNumber,
      asset: token.address,
      symbol: token.symbol,
      decimals,
      ltv,
      liquidationThreshold,
      liquidationBonus,
      reserveFactor,
      usageAsCollateralEnabled,
      borrowingEnabled,
      stableBorrowRateEnabled,
      isActive,
      isFrozen,
    });
  });

  return snapshots;
}

export async function getReserveConfigTimeSeries(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  fromBlock: number,
  toBlock: number,
  step: number
): Promise<ReserveConfigSnapshot[][]> {
  const series: ReserveConfigSnapshot[][] = [];
  for (let block = fromBlock; block <= toBlock; block += step) {
    const snapshots = await getReserveConfigsAtBlock(provider, chainId, block);
    series.push(snapshots);
  }
  return series;
}

export async function getAllReservesTokensOnChain(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  blockTag?: number
): Promise<ReserveTokenData[]> {
  const protocol = getProtocolAddresses(chainId, "sparklend");
  if (!protocol?.dataProvider) {
    throw new Error(
      `SparkLend data provider not configured for chain: ${chainId}`
    );
  }

  const dataProvider = new ethers.Contract(
    protocol.dataProvider,
    sparkDataProviderAbi,
    provider
  );

  const reserves = (await dataProvider.getAllReservesTokens({ blockTag })) as {
    symbol: string;
    tokenAddress: string;
  }[];

  return reserves.map((r) => ({
    symbol: r.symbol,
    tokenAddress: r.tokenAddress,
  }));
}
