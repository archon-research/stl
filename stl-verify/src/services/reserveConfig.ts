import { ethers } from "ethers";
import sparkDataProviderAbi from "../../abi/spark_data_provider.json";
import type { ChainId } from "../config/chains";
import { getBlockMarkers, getProtocolAddresses, getTokens } from "../config/addressbook";

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

export async function getReserveConfigsAtBlock(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  blockNumber: number
): Promise<ReserveConfigSnapshot[]> {
  const protocol = getProtocolAddresses(chainId, "sparklend");
  if (!protocol?.dataProvider) {
    throw new Error(`SparkLend data provider not configured for chain: ${chainId}`);
  }

  const dataProvider = new ethers.Contract(protocol.dataProvider, sparkDataProviderAbi, provider);
  const tokens = getTokens(chainId);

  const snapshots: ReserveConfigSnapshot[] = [];

  for (const token of tokens) {
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
      isFrozen
    ] = await dataProvider.getReserveConfigurationData(token.address, { blockTag: blockNumber });

    snapshots.push({
      blockNumber,
      asset: token.address,
      symbol: token.symbol,
      decimals: decimals as bigint,
      ltv: ltv as bigint,
      liquidationThreshold: liquidationThreshold as bigint,
      liquidationBonus: liquidationBonus as bigint,
      reserveFactor: reserveFactor as bigint,
      usageAsCollateralEnabled: usageAsCollateralEnabled as boolean,
      borrowingEnabled: borrowingEnabled as boolean,
      stableBorrowRateEnabled: stableBorrowRateEnabled as boolean,
      isActive: isActive as boolean,
      isFrozen: isFrozen as boolean
    });
  }

  return snapshots;
}

export async function getInitialReserveConfigs(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId
): Promise<ReserveConfigSnapshot[]> {
  const blocks = getBlockMarkers(chainId);
  if (!blocks.dataProviderCreation) {
    throw new Error(`dataProviderCreation block not configured for chain: ${chainId}`);
  }
  return getReserveConfigsAtBlock(provider, chainId, blocks.dataProviderCreation);
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
