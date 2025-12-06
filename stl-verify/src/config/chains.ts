/**
 * Chain configurations and types for multi-chain support
 */

export type ChainId = "ethereum" | "base" | "arbitrum" | "optimism" | "gnosis";

export interface ChainConfig {
  chainId: number;
  name: string;
  rpcEnvVar: string; // Environment variable name for RPC URL
  blockExplorer: string;
  avgBlockTimeSeconds: number;
}

export const chainConfigs: Record<ChainId, ChainConfig> = {
  ethereum: {
    chainId: 1,
    name: "Ethereum Mainnet",
    rpcEnvVar: "ETHEREUM_RPC_URL",
    blockExplorer: "https://etherscan.io",
    avgBlockTimeSeconds: 12
  },
  base: {
    chainId: 8453,
    name: "Base",
    rpcEnvVar: "BASE_RPC_URL",
    blockExplorer: "https://basescan.org",
    avgBlockTimeSeconds: 2
  },
  arbitrum: {
    chainId: 42161,
    name: "Arbitrum One",
    rpcEnvVar: "ARBITRUM_RPC_URL",
    blockExplorer: "https://arbiscan.io",
    avgBlockTimeSeconds: 0.25
  },
  optimism: {
    chainId: 10,
    name: "Optimism",
    rpcEnvVar: "OPTIMISM_RPC_URL",
    blockExplorer: "https://optimistic.etherscan.io",
    avgBlockTimeSeconds: 2
  },
  gnosis: {
    chainId: 100,
    name: "Gnosis Chain",
    rpcEnvVar: "GNOSIS_RPC_URL",
    blockExplorer: "https://gnosisscan.io",
    avgBlockTimeSeconds: 5
  }
};

/**
 * Get chain configuration by chain ID
 */
export function getChainConfig(chainId: ChainId): ChainConfig {
  const config = chainConfigs[chainId];
  if (!config) {
    throw new Error(`Unknown chain: ${chainId}`);
  }
  return config;
}

/**
 * Get all supported chain IDs
 */
export function getSupportedChains(): ChainId[] {
  return Object.keys(chainConfigs) as ChainId[];
}
