const LLAMA_CDN_BASE = 'https://icons.llamao.fi/icons';
const ONE_INCH_TOKEN_CDN_BASE = 'https://tokens-data.1inch.io/images';

/**
 * Chain ID to llama.fi slug mapping
 * Source: https://defillama.com/chains
 * Used to build URLs like: https://icons.llamao.fi/icons/chains/rsz_{slug}.jpg
 */
const LLAMA_CHAIN_SLUGS: Record<number, string> = {
  1: 'ethereum',
  10: 'optimism',
  56: 'binance',
  100: 'gnosis',
  130: 'unichain',
  137: 'polygon',
  250: 'fantom',
  324: 'zksync',
  8453: 'base',
  42161: 'arbitrum',
  43114: 'avalanche',
};

const PROTOCOL_SLUG_OVERRIDES: Record<string, string | null> = {
  'aave v2': 'aave-v2',
  'aave v3': 'aave-v3',
  'aave v3 lido': 'aave-v3',
  'aave v3 rwa': 'aave-v3',
  'morpho blue': 'morpho',
  sparklend: 'spark',
  grove: null,
};

export function buildTokenLogoUrl(_chainId: number, address: string): string {
  return `${ONE_INCH_TOKEN_CDN_BASE}/${address.toLowerCase()}.png`;
}

export function buildChainLogoUrl(chainId: number): string | null {
  const slug = LLAMA_CHAIN_SLUGS[chainId];
  if (!slug) {
    return null;
  }

  return `${LLAMA_CDN_BASE}/chains/rsz_${slug}.jpg`;
}

function toProtocolSlug(protocolName: string): string | null {
  const normalized = protocolName.trim().toLowerCase();
  if (normalized in PROTOCOL_SLUG_OVERRIDES) {
    return PROTOCOL_SLUG_OVERRIDES[normalized];
  }

  return normalized.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

export function buildProtocolLogoUrl(protocolName: string): string | null {
  if (!protocolName.trim()) {
    return null;
  }

  const slug = toProtocolSlug(protocolName);
  if (!slug) {
    return null;
  }

  return `${LLAMA_CDN_BASE}/protocols/${slug}`;
}
