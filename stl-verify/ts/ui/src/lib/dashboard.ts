import type {
  AllocationPosition,
  ReceiptTokenPosition,
} from '../types/allocation';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';

export type FilterOption = {
  value: string;
  label: string;
  count: number;
};

type BadDebtTone = 'green' | 'yellow' | 'red';

export type ChainLabelLookup = ReadonlyMap<number, string>;

const CHAIN_NAMES: Record<number, string> = {
  1: 'Ethereum',
  10: 'Optimism',
  137: 'Polygon',
  324: 'zkSync Era',
  1301: 'Unichain',
  8453: 'Base',
  42161: 'Arbitrum',
  43114: 'Avalanche',
};

const PROTOCOL_LABELS: Record<string, string> = {
  grove: 'Grove',
  spark: 'SparkLend',
};

const PROTOCOL_HINTS: Record<string, string[]> = {
  grove: ['grove'],
  spark: ['spark', 'sparklend'],
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
): number | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
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

export function getAllocationKey(allocation: AllocationPosition): string {
  return [
    allocation.chain_id,
    allocation.token_address,
    allocation.tx_hash,
    allocation.log_index,
  ].join(':');
}

export function buildNetworkOptions(
  allocations: AllocationPosition[],
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
  allocations: AllocationPosition[],
  localProtocols?: LocalProtocolRow[],
): FilterOption[] {
  const counts = new Map<string, number>();

  for (const allocation of allocations) {
    counts.set(allocation.name, (counts.get(allocation.name) ?? 0) + 1);
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

export function formatDateTime(value: string): string {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return DATE_TIME_FORMAT.format(date);
}

export function getBadDebtTone(
  value: number | string | null | undefined,
): BadDebtTone {
  const numeric = parseNumericValue(value) ?? 0;

  if (numeric <= 1_000) {
    return 'green';
  }

  if (numeric <= 1_000_000) {
    return 'yellow';
  }

  return 'red';
}

export function sortReceiptTokens(
  receiptTokens: ReceiptTokenPosition[],
): ReceiptTokenPosition[] {
  return [...receiptTokens].sort((left, right) => {
    const balanceDelta =
      (parseNumericValue(right.balance) ?? 0) -
      (parseNumericValue(left.balance) ?? 0);

    if (balanceDelta !== 0) {
      return balanceDelta;
    }

    return left.symbol.localeCompare(right.symbol);
  });
}

function getProtocolHints(protocol: string): string[] {
  const normalized = normalizeLabel(protocol);
  return PROTOCOL_HINTS[normalized] ?? [normalized];
}

export function matchReceiptToken(
  selectedAllocation: AllocationPosition | null,
  receiptTokens: ReceiptTokenPosition[],
): ReceiptTokenPosition | null {
  if (!selectedAllocation || receiptTokens.length === 0) {
    return null;
  }

  const allocationSymbol = normalizeLabel(
    selectedAllocation.token_symbol ?? '',
  );

  if (allocationSymbol.length === 0) {
    return null;
  }

  const protocolHints = getProtocolHints(selectedAllocation.name);

  const candidates = receiptTokens
    .map((token) => {
      let score = 0;
      const underlyingSymbol = normalizeLabel(token.underlying_symbol);
      const receiptSymbol = normalizeLabel(token.symbol);

      if (underlyingSymbol === allocationSymbol) {
        score += 4;
      }

      if (receiptSymbol === allocationSymbol) {
        score += 2;
      }

      const normalizedProtocol = normalizeLabel(token.protocol_name);
      if (protocolHints.some((hint) => normalizedProtocol.includes(hint))) {
        score += 3;
      }

      score += Math.min(
        (parseNumericValue(token.balance) ?? 0) / 1_000_000_000,
        0.99,
      );

      return { score, token };
    })
    .filter((candidate) => candidate.score >= 4)
    .sort((left, right) => right.score - left.score);

  return candidates[0]?.token ?? null;
}
