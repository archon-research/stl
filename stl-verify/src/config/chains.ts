/**
 * Chain configurations and types for multi-chain support
 */

export type ChainId = "ethereum" | "gnosis";

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
