/**
 * Token price fetching and historical sync service
 */

import { ethers } from "ethers";
import type { ChainId } from "../config/chains";
import { ChainProtocols, getProtocolAddresses, getTokens } from "../config/addressbook";
import { createMulticall } from "../providers/rpc";
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
 * Sync historical prices using Multicall for efficiency
 */
export async function syncHistoricalPrices(
  provider: ethers.JsonRpcProvider,
  chainId: ChainId,
  startBlock: number,
  endBlock: number,
  blockInterval: number = 100,
  onSnapshot?: (prices: TokenPrice[], blockNumber: number) => void
): Promise<{ snapshotsProcessed: number; errors: number }> {
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
  
  
  // Prepare calldata for each token's getAssetPrice call
  const multicall = createMulticall(provider);
  const oracleInterface = new ethers.Interface(aaveOracleAbi);
  const calls = tokens.map(token => ({
    target: oracleAddress,
    allowFailure: true,
    callData: oracleInterface.encodeFunctionData("getAssetPrice", [token.address])
  }));
  
  const totalSnapshots = Math.ceil((endBlock - startBlock + 1) / blockInterval);
  console.log(`\nSyncing historical prices on ${chainId} from block ${startBlock} to ${endBlock}`);
  console.log(`Interval: every ${blockInterval} blocks`);
  console.log(`Total snapshots: ${totalSnapshots}`);

  let snapshotsProcessed = 0;
  let errors = 0;

  for (let blockNumber = startBlock; blockNumber <= endBlock; blockNumber += blockInterval) {
    snapshotsProcessed++;

    try {
      const block = await provider.getBlock(blockNumber);
      if (!block) {
        console.warn(`Could not fetch block ${blockNumber}, skipping...`);
        errors++;
        continue;
      }

      // Batch all price queries via multicall
      const results: { success: boolean; returnData: string }[] = await multicall.aggregate3(calls, { blockTag: blockNumber });
      const tokenPrices: TokenPrice[] = [];

      for (let i = 0; i < tokens.length; i++) {
        const token = tokens[i];
        const result = results[i];

        if (!result.success || result.returnData === "0x"){
           continue;
        }

        try {
          const decoded = oracleInterface.decodeFunctionResult("getAssetPrice", result.returnData);
          const priceRaw = decoded[0] as bigint;

           // Skip zero prices;
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

      if (tokenPrices.length > 0 && onSnapshot) {
        onSnapshot(tokenPrices, blockNumber);
      }

      // Progress logging
      if (snapshotsProcessed % 100 === 0 || blockNumber + blockInterval > endBlock) {
        const progress = ((blockNumber - startBlock) / (endBlock - startBlock) * 100).toFixed(1);
        console.log(`[${progress}%] Block ${blockNumber} - ${tokenPrices.length} prices`);
      }

    } catch (error) {
      errors++;
      if (errors <= 10) {
        console.error(`Error at block ${blockNumber}:`, error);
      } else if (errors === 11) {
        console.error("Too many errors, suppressing further messages...");
      }
    }
  }

  console.log(`\nPrice sync complete! Snapshots: ${snapshotsProcessed}, Errors: ${errors}`);

  return { snapshotsProcessed, errors };
}
