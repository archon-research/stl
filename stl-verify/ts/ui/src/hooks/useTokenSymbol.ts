import { useEffect, useState } from 'react';
import {
  arbitrum,
  avalanche,
  base,
  mainnet,
  optimism,
  polygon,
} from 'viem/chains';

/**
 * Token metadata from Trust Wallet API
 */
export interface TokenMetadata {
  symbol: string;
  name?: string;
  decimals?: number;
  logoURI?: string;
}

/**
 * Map of chain IDs to viem chain objects for native currency lookups
 */
const CHAIN_ID_TO_VIEM_CHAIN = {
  1: mainnet,
  10: optimism,
  137: polygon,
  8453: base,
  42161: arbitrum,
  43114: avalanche,
} as const;

/**
 * In-memory cache for token metadata to avoid redundant API calls
 */
const tokenMetadataCache = new Map<string, TokenMetadata>();

/**
 * Fetch token metadata from Trust Wallet Assets API
 * Falls back to generating logo URL from 1inch CDN
 *
 * NOTE: Trust Wallet Assets CDN was deprecated (returns 404s).
 * Logo URLs are now generated from 1inch Tokens Data API which provides consistent coverage
 * See: TOKEN_CDN_RESEARCH.md for CDN analysis and alternatives
 */
async function fetchTokenMetadata(
  chainId: number,
  tokenAddress: string,
): Promise<TokenMetadata | null> {
  const cacheKey = `${chainId}:${tokenAddress.toLowerCase()}`;

  // Check cache first
  if (tokenMetadataCache.has(cacheKey)) {
    return tokenMetadataCache.get(cacheKey) || null;
  }

  try {
    const chainKey = getChainKeyForTrustWallet(chainId);
    if (!chainKey) {
      // Generate minimal metadata with logo from 1inch
      return generateMinimalMetadata(tokenAddress);
    }

    // Try to fetch from Trust Wallet (may be restored in future)
    try {
      const response = await fetch(
        `https://assets.trustwallet.com/blockchains/${chainKey}/tokens.json`,
        { signal: AbortSignal.timeout(5000) }, // 5 second timeout
      );

      if (response.ok) {
        const tokenList = await response.json() as { tokens: Array<{ address: string; symbol: string; decimals?: number; name?: string }> };
        const token = tokenList.tokens.find(
          (t) => t.address.toLowerCase() === tokenAddress.toLowerCase(),
        );

        if (token) {
          const metadata: TokenMetadata = {
            symbol: token.symbol,
            name: token.name,
            decimals: token.decimals,
            // Use 1inch CDN for logos (replaces deprecated Trust Wallet CDN)
            logoURI: `https://tokens-data.1inch.io/images/${tokenAddress.toLowerCase()}.png`,
          };
          tokenMetadataCache.set(cacheKey, metadata);
          return metadata;
        }
      }
    } catch (trustWalletError) {
      // Trust Wallet API failed, fall back to minimal metadata
      console.debug('Trust Wallet API fetch failed, using fallback metadata', trustWalletError);
    }

    // Fallback: generate minimal metadata with 1inch logo URL
    const metadata = generateMinimalMetadata(tokenAddress);
    tokenMetadataCache.set(cacheKey, metadata);
    return metadata;
  } catch {
    return null;
  }
}

/**
 * Generate minimal token metadata with 1inch logo URL
 * Used as fallback when Trust Wallet API is unavailable
 */
function generateMinimalMetadata(tokenAddress: string): TokenMetadata {
  return {
    symbol: '???', // Fallback symbol - should be overridden by user
    logoURI: `https://tokens-data.1inch.io/images/${tokenAddress.toLowerCase()}.png`,
  };
}

/**
 * Get chain key for Trust Wallet Assets API
 */
function getChainKeyForTrustWallet(chainId: number): string | null {
  const chainMap: Record<number, string> = {
    1: 'ethereum',
    10: 'optimism',
    137: 'polygon',
    324: 'era',
    8453: 'base',
    42161: 'arbitrumone',
    43114: 'avalanche-2',
    56: 'binance',
    250: 'fantom',
    1101: 'polygon-zkevm',
  };

  return chainMap[chainId] || null;
}

/**
 * Get native currency symbol for a chain
 */
export function getNativeSymbol(chainId: number): string | null {
  const viemChain = CHAIN_ID_TO_VIEM_CHAIN[chainId as keyof typeof CHAIN_ID_TO_VIEM_CHAIN];
  return viemChain?.nativeCurrency?.symbol || null;
}

/**
 * React hook to fetch token symbol and metadata
 *
 * @param chainId - Chain ID (e.g., 1 for Ethereum)
 * @param tokenAddress - Token contract address
 * @returns Object with symbol, metadata, loading state, and error
 */
export function useTokenSymbol(chainId: number, tokenAddress: string) {
  const [symbol, setSymbol] = useState<string | null>(null);
  const [metadata, setMetadata] = useState<TokenMetadata | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!tokenAddress) {
      setSymbol(null);
      setMetadata(null);
      return;
    }

    setLoading(true);
    setError(null);

    fetchTokenMetadata(chainId, tokenAddress)
      .then((result) => {
        if (result) {
          setSymbol(result.symbol);
          setMetadata(result);
        } else {
          setSymbol(null);
          setMetadata(null);
        }
      })
      .catch((err) => {
        setError(err instanceof Error ? err : new Error('Failed to fetch token metadata'));
        setSymbol(null);
        setMetadata(null);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [chainId, tokenAddress]);

  return { symbol, metadata, loading, error };
}

/**
 * Get combined resolver for both native and token symbols
 * Useful when you need to handle both native currency and ERC20 tokens
 */
export function useSymbolResolver(
  chainId: number,
  tokenAddress: string | null,
): {
  symbol: string | null;
  isNative: boolean;
  loading: boolean;
  error: Error | null;
} {
  const { symbol: tokenSymbol, loading, error } = useTokenSymbol(
    chainId,
    tokenAddress || '',
  );

  // If no token address provided, return native symbol
  if (!tokenAddress) {
    const nativeSymbol = getNativeSymbol(chainId);
    return {
      symbol: nativeSymbol,
      isNative: true,
      loading: false,
      error: null,
    };
  }

  return {
    symbol: tokenSymbol,
    isNative: false,
    loading,
    error,
  };
}
