type ChainMetadata = {
  name: string;
  explorerUrl: string;
  nativeSymbol: string;
};

/**
 * Names, explorers and native symbols for the chains we index. Transcribed from
 * viem's definitions, which cost ~180 KiB of formatters and serializers to read.
 */
export const CHAIN_METADATA: Record<number, ChainMetadata | undefined> = {
  1: {
    name: 'Ethereum',
    explorerUrl: 'https://etherscan.io',
    nativeSymbol: 'ETH',
  },
  10: {
    name: 'OP Mainnet',
    explorerUrl: 'https://optimistic.etherscan.io',
    nativeSymbol: 'ETH',
  },
  130: {
    name: 'Unichain',
    explorerUrl: 'https://uniscan.xyz',
    nativeSymbol: 'ETH',
  },
  137: {
    name: 'Polygon',
    explorerUrl: 'https://polygonscan.com',
    nativeSymbol: 'POL',
  },
  324: {
    name: 'ZKsync Era',
    explorerUrl: 'https://explorer.zksync.io/',
    nativeSymbol: 'ETH',
  },
  8453: {
    name: 'Base',
    explorerUrl: 'https://basescan.org',
    nativeSymbol: 'ETH',
  },
  42161: {
    name: 'Arbitrum One',
    explorerUrl: 'https://arbiscan.io',
    nativeSymbol: 'ETH',
  },
  43114: {
    name: 'Avalanche',
    explorerUrl: 'https://snowtrace.io',
    nativeSymbol: 'AVAX',
  },
};

/**
 * Get human-readable name for a chain ID
 * @param chainId - Chain ID (e.g., 1 for Ethereum)
 * @returns Display name, or fallback format
 */
export function getChainName(chainId: number): string {
  return CHAIN_METADATA[chainId]?.name ?? `Chain ${chainId}`;
}

/**
 * Get official block explorer URL for a chain
 * @param chainId - Chain ID
 * @returns Official explorer URL, or null if not available
 */
export function getChainExplorerUrl(
  chainId: number | null | undefined,
): string | null {
  if (chainId === null || chainId === undefined) {
    return null;
  }
  return CHAIN_METADATA[chainId]?.explorerUrl ?? null;
}

/**
 * Get native currency symbol for a chain
 * @param chainId - Chain ID
 * @returns Native currency symbol (e.g., "ETH", "AVAX"), or null if not available
 */
export function getNativeSymbol(chainId: number | null): string | null {
  if (chainId === null) {
    return null;
  }
  return CHAIN_METADATA[chainId]?.nativeSymbol ?? null;
}
