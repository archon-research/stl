import { useEffect, useState } from 'react';

import { getNativeSymbol } from '../lib/chain-metadata';
import { buildTokenLogoUrl } from '../lib/logo-cdn';

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
 * In-memory cache for token metadata to avoid redundant API calls
 */
const tokenMetadataCache = new Map<string, TokenMetadata>();

/**
 * Fetch token metadata from Trust Wallet Assets API (if available) or generate minimal metadata
 * Falls back to generating logo URL from 1inch CDN
 *
 * NOTE: Trust Wallet Assets API was deprecated (returns 404s).
 * Logo URLs are now generated from 1inch Tokens Data API which provides consistent coverage
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
    // Trust Wallet API is deprecated, skip to fallback
    // Generate minimal metadata with 1inch logo URL
    const metadata = generateMinimalMetadata(chainId, tokenAddress);
    tokenMetadataCache.set(cacheKey, metadata);
    return metadata;
  } catch {
    return null;
  }
}

/**
 * Generate minimal token metadata with 1inch logo URL
 * Used as fallback when token metadata is unavailable
 */
function generateMinimalMetadata(
  chainId: number,
  tokenAddress: string,
): TokenMetadata {
  return {
    symbol: '???', // Fallback symbol - should be overridden by user
    logoURI: buildTokenLogoUrl(chainId, tokenAddress),
  };
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
      setError(null);
      setLoading(false);
      return;
    }

    const controller = new AbortController();

    setLoading(true);
    setError(null);

    fetchTokenMetadata(chainId, tokenAddress)
      .then((result) => {
        if (controller.signal.aborted) {
          return;
        }

        if (result) {
          setSymbol(result.symbol);
          setMetadata(result);
        } else {
          setSymbol(null);
          setMetadata(null);
        }
      })
      .catch((err) => {
        if (controller.signal.aborted) {
          return;
        }

        setError(
          err instanceof Error
            ? err
            : new Error('Failed to fetch token metadata'),
        );
        setSymbol(null);
        setMetadata(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });

    return () => controller.abort();
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
  const {
    symbol: tokenSymbol,
    loading,
    error,
  } = useTokenSymbol(chainId, tokenAddress || '');

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
