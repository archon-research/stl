import type { Allocation } from '../types/allocation';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';
import { logging } from './logging';

export type FilterOption = {
  value: string;
  label: string;
  count: number;
};

type BadDebtTone = 'green' | 'yellow' | 'red' | 'neutral';

export type ChainLabelLookup = ReadonlyMap<number, string>;

const CHAIN_NAMES: Record<number, string> = {
  1: 'Ethereum',
  10: 'Optimism',
  137: 'Polygon',
  324: 'zkSync Era',
  130: 'Unichain',
  8453: 'Base',
  42161: 'Arbitrum',
  43114: 'Avalanche',
};

const PROTOCOL_LABELS: Record<string, string> = {
  grove: 'Grove',
  spark: 'SparkLend',
};

const COMPACT_NUMBER_FORMAT = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 2,
  notation: 'compact',
});

const TOKEN_NUMBER_FORMAT = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 2,
});

const TOKEN_SMALL_FORMAT = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 6,
});

const CURRENCY_FORMAT = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumFractionDigits: 2,
  style: 'currency',
});

const COMPACT_CURRENCY_FORMAT = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumFractionDigits: 2,
  notation: 'compact',
  style: 'currency',
});

const DATE_TIME_FORMAT = new Intl.DateTimeFormat('en-US', {
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  month: 'short',
});

function titleCase(value: string): string {
  return value
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .map((word) =>
      word.length > 0 ? `${word[0].toUpperCase()}${word.slice(1)}` : word,
    )
    .join(' ');
}

function normalizeLabel(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, '');
}

export function parseNumericValue(
  value: number | string | null | undefined,
  context?: string,
): number | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    logging.warn(
      `Failed to parse numeric value: "${value}"`,
      context ? { context } : undefined,
    );
    return null;
  }

  return numeric;
}

export function buildChainLabelLookup(
  chains: LocalChainRow[],
): ChainLabelLookup {
  return new Map(chains.map((chain) => [chain.chain_id, chain.name] as const));
}

function getProtocolMatchScore(
  protocol: string,
  localProtocol: LocalProtocolRow,
  chainId?: number,
): number {
  const normalizedProtocol = normalizeLabel(protocol);
  const normalizedName = normalizeLabel(localProtocol.name);
  let score = 0;

  if (chainId !== undefined && localProtocol.chain_id === chainId) {
    score += 3;
  }

  if (normalizedName === normalizedProtocol) {
    score += 10;
  }

  if (
    normalizedName.includes(normalizedProtocol) ||
    normalizedProtocol.includes(normalizedName)
  ) {
    score += 6;
  }

  if (
    (normalizedProtocol === 'spark' && normalizedName === 'sparklend') ||
    (normalizedProtocol === 'morpho' && normalizedName === 'morphoblue')
  ) {
    score += 8;
  }

  return score;
}

export function findProtocolMetadata(
  protocol: string,
  localProtocols?: LocalProtocolRow[],
  chainId?: number,
): LocalProtocolRow | null {
  if (!localProtocols || localProtocols.length === 0) {
    return null;
  }

  const matches = localProtocols
    .map((localProtocol) => ({
      localProtocol,
      score: getProtocolMatchScore(protocol, localProtocol, chainId),
    }))
    .filter((candidate) => candidate.score > 0)
    .sort((left, right) => right.score - left.score);

  return matches[0]?.localProtocol ?? null;
}

export function getChainLabel(
  chainId: number,
  chainLabels?: ChainLabelLookup,
): string {
  return (
    chainLabels?.get(chainId) ?? CHAIN_NAMES[chainId] ?? `Chain ${chainId}`
  );
}

export function getProtocolLabel(
  protocol: string,
  localProtocols?: LocalProtocolRow[],
  chainId?: number,
): string {
  const normalized = normalizeLabel(protocol);
  return (
    findProtocolMetadata(protocol, localProtocols, chainId)?.name ??
    PROTOCOL_LABELS[normalized] ??
    titleCase(protocol)
  );
}

export function getAllocationKey(allocation: Allocation): string {
  return String(allocation.receipt_token_id);
}

export function buildNetworkOptions(
  allocations: Allocation[],
  chainLabels?: ChainLabelLookup,
): FilterOption[] {
  const counts = new Map<number, number>();

  for (const allocation of allocations) {
    counts.set(allocation.chain_id, (counts.get(allocation.chain_id) ?? 0) + 1);
  }

  return [...counts.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([chainId, count]) => ({
      count,
      label: getChainLabel(chainId, chainLabels),
      value: String(chainId),
    }));
}

export function buildProtocolOptions(
  allocations: Allocation[],
  localProtocols?: LocalProtocolRow[],
): FilterOption[] {
  const counts = new Map<string, number>();

  for (const allocation of allocations) {
    counts.set(
      allocation.protocol_name,
      (counts.get(allocation.protocol_name) ?? 0) + 1,
    );
  }

  return [...counts.entries()]
    .sort((left, right) =>
      getProtocolLabel(left[0], localProtocols).localeCompare(
        getProtocolLabel(right[0], localProtocols),
      ),
    )
    .map(([protocol, count]) => ({
      count,
      label: getProtocolLabel(protocol, localProtocols),
      value: protocol,
    }));
}

export function formatTokenAmount(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return '—';
  }

  const absolute = Math.abs(numeric);

  if (absolute >= 1_000_000) {
    return COMPACT_NUMBER_FORMAT.format(numeric);
  }

  if (absolute >= 1) {
    return TOKEN_NUMBER_FORMAT.format(numeric);
  }

  if (absolute === 0) {
    return '0';
  }

  return TOKEN_SMALL_FORMAT.format(numeric);
}

export function formatUsdValue(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return '—';
  }

  return Math.abs(numeric) >= 1_000_000
    ? COMPACT_CURRENCY_FORMAT.format(numeric)
    : CURRENCY_FORMAT.format(numeric);
}

export function formatPercentValue(
  value: number | string | null | undefined,
  digits = 2,
): string {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return '—';
  }

  return `${numeric.toFixed(digits)}%`;
}

export function formatRatioPercent(
  value: number | string | null | undefined,
  digits = 2,
): string {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return '—';
  }

  return `${(numeric * 100).toFixed(digits)}%`;
}

export function formatMultiplier(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return '—';
  }

  return `${numeric.toFixed(3)}x`;
}

export function formatDeltaSign(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return '—';
  }

  const sign = numeric >= 0 ? '+' : '−';
  const formattedAmount =
    Math.abs(numeric) >= 1_000_000
      ? COMPACT_NUMBER_FORMAT.format(Math.abs(numeric))
      : TOKEN_NUMBER_FORMAT.format(Math.abs(numeric));

  return `${sign}${formattedAmount}`;
}

export function formatFreshnessLabel(isoTimestamp: string): string {
  try {
    const date = new Date(isoTimestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.max(0, Math.floor(diffMs / 60000));
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 60) {
      return diffMins === 0 ? 'Just now' : `${diffMins}m ago`;
    } else if (diffHours < 24) {
      return `${diffHours}h ago`;
    } else if (diffDays < 7) {
      return `${diffDays}d ago`;
    }

    return date.toLocaleDateString();
  } catch {
    return isoTimestamp;
  }
}

export function formatDateTime(value: string): string {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return DATE_TIME_FORMAT.format(date);
}

/**
 * Get human-readable label for allocation category.
 */
export function getCategoryLabel(
  category: 'allocation' | 'pol' | 'psm3' | 'asset' | '' | undefined,
  fallback: string = 'Unknown',
): string {
  const labels: Record<string, string> = {
    allocation: 'Allocation',
    pol: 'Protocol Owned Liquidity',
    psm3: 'PSM3',
    asset: 'Asset',
  };
  return category ? (labels[category] ?? fallback) : fallback;
}

export function getBadDebtTone(
  value: number | string | null | undefined,
): BadDebtTone {
  const numeric = parseNumericValue(value);

  if (numeric === null) {
    return 'neutral';
  }

  if (numeric <= 1_000) {
    return 'green';
  }

  if (numeric <= 1_000_000) {
    return 'yellow';
  }

  return 'red';
}

export function sortAllocations(allocations: Allocation[]): Allocation[] {
  return [...allocations].sort((left, right) => {
    const balanceDelta =
      (parseNumericValue(right.balance) ?? 0) -
      (parseNumericValue(left.balance) ?? 0);

    if (balanceDelta !== 0) {
      return balanceDelta;
    }

    return left.symbol.localeCompare(right.symbol);
  });
}

const CHAIN_EXPLORERS: Record<number, string> = {
  1: 'https://etherscan.io',
  10: 'https://optimistic.etherscan.io',
  137: 'https://polygonscan.com',
  324: 'https://explorer.zksync.io',
  130: 'https://unichain.blockscout.com',
  8453: 'https://basescan.org',
  42161: 'https://arbiscan.io',
  43114: 'https://snowscan.xyz',
};

/**
 * Returns an Etherscan/block-explorer URL for the given chain + address,
 * or null if the chain is not recognised.
 */
export function getExplorerUrl(
  chainId: number,
  address: string,
  type: 'address' | 'tx' = 'address',
): string | null {
  const base = CHAIN_EXPLORERS[chainId];
  if (!base) {
    return null;
  }
  return `${base}/${type}/${address}`;
}

/**
 * Lookup table of well-known contract addresses to human-readable labels.
 * Keys are lowercased hex addresses (without checksum).
 * Extend as needed — used as a best-effort enrichment layer on top of
 * protocol/symbol fields already available from the API.
 */
const KNOWN_ADDRESS_LABELS: Record<string, string> = {};

/**
 * Returns a human-readable label for a known contract address, or null
 * if the address is not in the local dictionary.
 */
export function getAddressLabel(address: string): string | null {
  return KNOWN_ADDRESS_LABELS[address.toLowerCase()] ?? null;
}
