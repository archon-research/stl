import {
  arbitrum,
  avalanche,
  base,
  mainnet,
  optimism,
  polygon,
  unichain,
  zksync,
} from 'viem/chains';

/**
 * Centralized chain metadata derived from viem
 * Single source of truth for all chain-related data (names, explorers, native symbols)
 */
export const VIEM_CHAINS = {
  1: mainnet,
  10: optimism,
  137: polygon,
  324: zksync,
  130: unichain,
  8453: base,
  42161: arbitrum,
  43114: avalanche,
} as const;

/**
 * Get human-readable name for a chain ID
 * @param chainId - Chain ID (e.g., 1 for Ethereum)
 * @returns Display name from viem, or fallback format
 */
export function getChainName(chainId: number): string {
  const chain = VIEM_CHAINS[chainId as keyof typeof VIEM_CHAINS];
  return chain?.name ?? `Chain ${chainId}`;
}

/**
 * Get official block explorer URL for a chain
 * @param chainId - Chain ID
 * @returns Official explorer URL from viem, or null if not available
 */
export function getChainExplorerUrl(chainId: number): string | null {
  const chain = VIEM_CHAINS[chainId as keyof typeof VIEM_CHAINS];
  return chain?.blockExplorers?.default?.url ?? null;
}

/**
 * Get native currency symbol for a chain
 * @param chainId - Chain ID
 * @returns Native currency symbol (e.g., "ETH", "AVAX"), or null if not available
 */
export function getNativeSymbol(chainId: number): string | null {
  const chain = VIEM_CHAINS[chainId as keyof typeof VIEM_CHAINS];
  return chain?.nativeCurrency?.symbol ?? null;
}
