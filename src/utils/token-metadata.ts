/**
 * ERC20 Token Metadata Fetcher
 * 
 * Fetches ERC20 token metadata (symbol, name, decimals) from on-chain contracts.
 */

import type { Context } from "ponder:registry";

const ERC20_ABI = [
  {
    inputs: [],
    name: "symbol",
    outputs: [{ type: "string" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "name",
    outputs: [{ type: "string" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "decimals",
    outputs: [{ type: "uint8" }],
    stateMutability: "view",
    type: "function",
  },
] as const;

export async function getTokenMetadata(
  context: Context,
  address: `0x${string}`
): Promise<{ symbol: string; name: string; decimals: number }> {
  try {
    const client = context.client;

    // Fetch symbol, name, and decimals from the contract
    // Context client is already configured for the specific chain
    const [symbol, name, decimals] = await Promise.all([
      client.readContract({
        address,
        abi: ERC20_ABI,
        functionName: "symbol",
      }),
      client.readContract({
        address,
        abi: ERC20_ABI,
        functionName: "name",
      }),
      client.readContract({
        address,
        abi: ERC20_ABI,
        functionName: "decimals",
      }),
    ]);

    return {
      symbol: symbol as string,
      name: name as string,
      decimals: Number(decimals),
    };
  } catch (error) {
    // Fallback if contract calls fail (non-standard ERC20, etc.)
    console.warn(`Failed to fetch metadata for token ${address}:`, error);
    return {
      symbol: "UNKNOWN",
      name: `Unknown Token ${address.slice(0, 10)}...`,
      decimals: 18, // Assume 18 as default
    };
  }
}

