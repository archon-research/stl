import type { Allocation, Prime } from '../types/allocation';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';
import { getChainExplorerUrl, getChainName } from './chain-metadata';
import { logging } from './logging';

export type FilterOption = {
  value: string;
  label: string;
  count: number;
};

export type ChainLabelLookup = ReadonlyMap<number, string>;

// Sentinel filter value for allocations with no registered protocol wrapper
// (direct asset holdings). Distinguishes "show only direct holdings" from
// "show all protocols" (which uses selectedProtocol === null).
export const DIRECT_PROTOCOL_FILTER_VALUE = '__direct__';

const PROTOCOL_LABELS: Record<string, string> = {
  grove: 'Grove',
  spark: 'SparkLend',
  maple: 'Maple',
};

const COMPACT_NUMBER_FORMAT = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 2,
  notation: 'compact',
});

// Compact with 2 significant digits, for chart axes and tooltips (e.g. "1.5B",
// "36M"). Significant digits keep the precision consistent across magnitudes.
const COMPACT_SIGNIFICANT_FORMAT = new Intl.NumberFormat('en-US', {
  maximumSignificantDigits: 2,
  notation: 'compact',
});

const COMPACT_SIGNIFICANT_CURRENCY_FORMAT = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumSignificantDigits: 2,
  notation: 'compact',
  style: 'currency',
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
    .map((word) => `${word.slice(0, 1).toUpperCase()}${word.slice(1)}`)
    .join(' ');
}

function normalizeLabel(value: string | null | undefined): string {
  if (!value) return '';
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

export type PrimeGroup = {
  key: string;
  name: string;
  vaultAddress: string | null;
  primaryProxyAddress: string;
  proxyAddresses: string[];
  chainCount: number;
};

// A prime allocates through one ALM proxy per chain, so `/v1/primes` returns
// one row per (prime, chain) pair. `prime_vault_address` is the same across
// every row of a prime; a row whose prime has no vault address on record
// falls back to `name` so it still groups rather than getting its own row.
export function getPrimeGroupKey(prime: Prime): string {
  return prime.prime_vault_address ?? prime.name;
}

export function groupPrimesByVault(primes: Prime[]): PrimeGroup[] {
  // Non-empty by construction, and typed that way so the representative row and
  // the address fallback below need no guard for a case the loop cannot produce.
  const rowsByKey = new Map<string, [Prime, ...Prime[]]>();

  for (const prime of primes) {
    const key = getPrimeGroupKey(prime);
    const rows = rowsByKey.get(key);
    if (rows) {
      rows.push(prime);
    } else {
      rowsByKey.set(key, [prime]);
    }
  }

  return [...rowsByKey.entries()].map(([key, rows]) => {
    const sortedByAddress = [...rows].sort((left, right) =>
      left.address.localeCompare(right.address),
    );
    // `mainnet` is the prime's canonical chain for aggregate figures (e.g.
    // risk-capital); fall back to a deterministic pick when no row is on it.
    const mainnetRow = rows.find((row) => row.chain === 'mainnet');
    // Seedless: `rows` is non-empty by construction, which is what makes this
    // the lowest address rather than a possibly-absent first element.
    const lowestByAddress = rows.reduce((lowest, row) =>
      row.address.localeCompare(lowest.address) < 0 ? row : lowest,
    );

    return {
      key,
      name: rows[0].name,
      vaultAddress: rows[0].prime_vault_address ?? null,
      primaryProxyAddress: mainnetRow?.address ?? lowestByAddress.address,
      // Deduped because `/v1/primes` is `DISTINCT ON (proxy_address, chain_id)`,
      // so one address holding positions on two chains yields two rows. Passing
      // it twice to `getAllocationsForProxies` would fetch it twice and
      // double-count every one of its rows in the grid and in `summary.totalUsd`
      // — `getAllocationKey` gives the copies identical keys, so nothing
      // downstream would catch it.
      proxyAddresses: [...new Set(sortedByAddress.map((row) => row.address))],
      chainCount: new Set(rows.map((row) => row.chain_id)).size,
    };
  });
}

/**
 * The prime group a URL's prime segment names, or `null` if none does.
 *
 * The segment is a group key — `prime_vault_address` — but the addresses a
 * reader has to hand are usually not that. `/v1/primes` keys its rows by ALM
 * proxy, one per chain, and an explorer link names a proxy too, so a deep link
 * built from either misses a key comparison while naming a prime the app holds.
 * Matching those aliases resolves to the same prime the link meant, which is
 * strictly better than falling back to the first prime in the list.
 *
 * Case-insensitive because a checksummed address is the form an explorer hands
 * over, while `/v1/primes` reports addresses lowercased; the two denote the same
 * account. Keys are matched before proxies so a group's own key always wins.
 */
export function findPrimeGroup(
  groups: PrimeGroup[],
  requested: string,
): PrimeGroup | null {
  const wanted = requested.toLowerCase();

  return (
    groups.find((group) => group.key === requested) ??
    groups.find((group) => group.key.toLowerCase() === wanted) ??
    groups.find((group) =>
      group.proxyAddresses.some((address) => address.toLowerCase() === wanted),
    ) ??
    null
  );
}

function getProtocolMatchScore(
  protocol: string,
  localProtocol: LocalProtocolRow,
  chainId?: number | null,
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

function findProtocolMetadata(
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

// chain_id 0 is the off-chain sentinel (e.g. Anchorage BTC custody), which has
// no EVM chain and so no name in the chain registry or logo CDN.
const OFFCHAIN_CHAIN_ID = 0;

export function getChainLabel(
  chainId: number | null | undefined,
  chainLabels?: ChainLabelLookup,
  network?: string | null,
): string {
  if (chainId === OFFCHAIN_CHAIN_ID) return 'Off-chain';
  // Title-cased so an upstream slug reads like a registry label.
  if (chainId === null || chainId === undefined) {
    return network === null || network === undefined || network.length === 0
      ? 'Unknown chain'
      : titleCase(network);
  }
  return chainLabels?.get(chainId) ?? getChainName(chainId);
}

export function getProtocolLabel(
  protocol: string | null | undefined,
  localProtocols?: LocalProtocolRow[],
  chainId?: number | null,
): string {
  if (!protocol || protocol === DIRECT_PROTOCOL_FILTER_VALUE) return 'Direct';
  const normalized = normalizeLabel(protocol);
  return (
    findProtocolMetadata(protocol, localProtocols, chainId ?? undefined)
      ?.name ??
    PROTOCOL_LABELS[normalized] ??
    titleCase(protocol)
  );
}

/**
 * A row's network, as a filter/grouping key.
 *
 * Chain id where there is one, else the upstream network name: two rows on
 * different unindexed chains would otherwise share the key `null` and be
 * treated as one network.
 */
export function allocationNetworkKey(allocation: Allocation): string {
  return allocation.chain_id === null
    ? `net:${allocation.network ?? 'unknown'}`
    : String(allocation.chain_id);
}

export function getAllocationKey(allocation: Allocation): string {
  const identityKey = getAllocationIdentityKey(allocation);
  const referenceSuffix = getReferenceDisambiguator(allocation);
  return referenceSuffix ? `${identityKey}#${referenceSuffix}` : identityKey;
}

/**
 * Extra identity only a reference row can carry, appended to keep same-symbol
 * or same-wallet reference rows from colliding on `getAllocationIdentityKey`.
 *
 * Two collisions show up there, both only on `source: 'reference'` rows: the
 * same token under two proxy wallets (VEC-NA, grove's split Uni V3 position),
 * and several distinct vault-share token addresses sharing one display symbol
 * (grove's `grove-bbqUSDC` family — unresolved against STL's registry, so
 * `symbol` is all the identity fallback has to key on). Gated on `source`
 * rather than on the fields' mere presence: an indexed row can legitimately
 * carry a `receipt_token_address` too (a resolved receipt-token position),
 * but it already keys uniquely off `receipt_token_id`/`underlying_token_id`
 * before reaching that fallback, so folding the address in for it as well
 * would needlessly change every indexed row's key — and any URL/selection
 * built from it.
 */
function getReferenceDisambiguator(allocation: Allocation): string {
  if (allocation.source !== 'reference') return '';
  return [allocation.receipt_token_address, allocation.wallet_address]
    .filter((part): part is string => Boolean(part))
    .join(':');
}

function getAllocationIdentityKey(allocation: Allocation): string {
  if (allocation.receipt_token_id != null) {
    return String(allocation.receipt_token_id);
  }
  // Direct holdings have no receipt token; identify by chain + underlying.
  // Off-chain custody rows (Anchorage BTC) carry a null underlying id, so fall
  // back to the symbol to keep the key unique and stable.
  const underlyingKey = allocation.underlying_token_id ?? allocation.symbol;
  return `direct:${allocationNetworkKey(allocation)}:${underlyingKey}`;
}

/**
 * Chains STL indexes first, in chain-id order, then the rest by name.
 *
 * A chain with no id has no number to sort by, and sorting the whole list by
 * label instead would stop mainnet leading for every prime.
 */
function compareNetworkOptions(
  left: { label: string; chainId: number | null },
  right: { label: string; chainId: number | null },
): number {
  if (left.chainId === null || right.chainId === null) {
    return left.chainId === right.chainId
      ? left.label.localeCompare(right.label)
      : Number(left.chainId === null) - Number(right.chainId === null);
  }
  return left.chainId - right.chainId;
}

export function buildNetworkOptions(
  allocations: Allocation[],
  chainLabels?: ChainLabelLookup,
): FilterOption[] {
  const counts = new Map<
    string,
    { count: number; label: string; chainId: number | null }
  >();

  for (const allocation of allocations) {
    const key = allocationNetworkKey(allocation);
    const existing = counts.get(key);
    counts.set(key, {
      count: (existing?.count ?? 0) + 1,
      chainId: existing?.chainId ?? allocation.chain_id,
      label:
        existing?.label ??
        getChainLabel(allocation.chain_id, chainLabels, allocation.network),
    });
  }

  // The key is `allocationNetworkKey`, so it is the option value verbatim.
  return [...counts.entries()]
    .sort(([, left], [, right]) => compareNetworkOptions(left, right))
    .map(([value, { count, label }]) => ({ count, label, value }));
}

export function buildProtocolOptions(
  allocations: Allocation[],
  localProtocols?: LocalProtocolRow[],
): FilterOption[] {
  const counts = new Map<string, number>();

  for (const allocation of allocations) {
    const key = allocation.protocol_name ?? DIRECT_PROTOCOL_FILTER_VALUE;
    counts.set(key, (counts.get(key) ?? 0) + 1);
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

// The Activities view spans every prime, so its protocol/network filters are
// sourced from the full registries rather than a single prime's allocations
// (which is all the allocation-scoped builders can see). Per-option counts are
// meaningless across primes here, so they are set to 0 and hidden by the
// dropdown. The activity API matches `protocol.name` (== `LocalProtocolRow.name`)
// and `chain_id`, so option values map verbatim.
export function buildProtocolOptionsFromMetadata(
  localProtocols: LocalProtocolRow[],
): FilterOption[] {
  const names = new Set<string>();

  for (const protocol of localProtocols) {
    const name = protocol.name?.trim();
    if (name) {
      names.add(name);
    }
  }

  return [...names]
    .sort((left, right) => left.localeCompare(right))
    .map((name) => ({ count: 0, label: name, value: name }));
}

export function buildNetworkOptionsFromMetadata(
  localChains: LocalChainRow[],
): FilterOption[] {
  const seen = new Set<number>();

  return localChains
    .filter((chain) => {
      if (seen.has(chain.chain_id)) {
        return false;
      }
      seen.add(chain.chain_id);
      return true;
    })
    .sort((left, right) => left.chain_id - right.chain_id)
    .map((chain) => ({
      count: 0,
      label: chain.name,
      value: String(chain.chain_id),
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

  // Cents on a four-digit figure read as more precision than a magnitude
  // surface has, and break column scanning. Exact prices: formatUsdPrice.
  return Math.abs(numeric) >= 1_000
    ? COMPACT_CURRENCY_FORMAT.format(numeric)
    : CURRENCY_FORMAT.format(numeric);
}

/**
 * Never compacts: a price display exists so a reader can check the exact
 * figure a model priced off — $118,432.55 must not become $118.43K.
 */
export function formatUsdPrice(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);
  return numeric === null ? '—' : CURRENCY_FORMAT.format(numeric);
}

// Compact, 2-significant-digit formatters for chart axes and tooltips.
export function formatCompactUsd(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);
  return numeric === null
    ? '—'
    : COMPACT_SIGNIFICANT_CURRENCY_FORMAT.format(numeric);
}

export function formatCompactNumber(
  value: number | string | null | undefined,
): string {
  const numeric = parseNumericValue(value);
  return numeric === null ? '—' : COMPACT_SIGNIFICANT_FORMAT.format(numeric);
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

// Trailing methodology tag for a "Model-derived (<model>...)" caption.
// gap_sweep is a fixed 15% collateral-price stress; core_model charges
// capital off a Monte Carlo expected loss instead (app/risk_engine/core_model
// README, CoreModelDetails.crr_el_pct), so the same "stress" wording would
// misdescribe it. An unrecognized model gets no suffix — just its bare name.
export function riskModelCaptionSuffix(model: string | null): string {
  switch (model) {
    case 'gap_sweep':
      return ', 15% stress';
    case 'core_model':
      return ', expected-loss based';
    default:
      return '';
  }
}

// Encumbrance breach thresholds, as the Sky Atlas defines them rather than as a
// number chosen here: a Low Severity Breach is a ratio at or above 100% and
// below 103%, a High Severity Breach is above 103%.
//
// https://sky-atlas.io/#1981fd65-a9a5-4e5a-a9f8-aa8e85342d7c (low)
// https://sky-atlas.io/#363e2bb5-47e2-4eb8-950d-eafd0f1392c7 (high)
export const ENCUMBRANCE_LOW_SEVERITY_THRESHOLD = 1;
export const ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD = 1.03;
// A product choice, not an Atlas figure: the pre-breach warning band starts
// here so a ratio drifting toward 100% reads as "at risk" before it breaches.
export const ENCUMBRANCE_AT_RISK_THRESHOLD = 0.8;

export type EncumbranceSeverity = 'healthy' | 'at-risk' | 'low' | 'high';

/**
 * Classifies an encumbrance ratio against the Atlas breach thresholds, with a
 * pre-breach warning band below them.
 *
 * Exactly 103% falls outside both written definitions — "below 103%" excludes
 * it and "above 103%" excludes it — so it is read as high here. On a risk
 * surface the conservative side of an ambiguity is the safe one, and a ratio at
 * the high boundary is plainly not the lesser breach. An unknown ratio reads as
 * healthy only in colour: callers gate on the value being present.
 */
export function encumbranceSeverity(
  ratio: number | null | undefined,
): EncumbranceSeverity {
  if (ratio === null || ratio === undefined || !Number.isFinite(ratio)) {
    return 'healthy';
  }
  if (ratio >= ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD) {
    return 'high';
  }
  if (ratio >= ENCUMBRANCE_LOW_SEVERITY_THRESHOLD) {
    return 'low';
  }
  return ratio >= ENCUMBRANCE_AT_RISK_THRESHOLD ? 'at-risk' : 'healthy';
}

/**
 * Columns that spread `count` cards evenly over the fewest rows `maxColumns`
 * allows — 6 cards in 4 columns becomes 3 and 3, not 4 and 2.
 *
 * A row that cannot be filled is left short rather than stretched: five cards
 * over three columns is 3 then 2, and those two keep the width of the three
 * above them instead of growing to half the container each.
 */
/**
 * Projects gap-filled buckets onto chart points, dropping those whose figure is
 * absent.
 *
 * A dropped bucket leaves a hole the line is then drawn straight across, so this
 * reads a discontinuous series as continuous. That is tolerable because every
 * caller's figure is LOCF-carried server-side — an interior gap means the whole
 * feed stopped, which the cronjob alerts already cover — but it is the reason
 * absence is dropped rather than plotted as zero.
 */
// `timestamp` is what the synced cursor is keyed on, so it carries the bucket's
// own instant rather than the formatted label: sibling cards bucket at different
// resolutions, and only the instant means the same thing in all of them.
export function toChartSeries<T extends { bucket_start: string }>(
  buckets: readonly T[],
  read: (bucket: T) => number | null,
): { label: string; value: number; timestamp: number }[] {
  return buckets
    .map((bucket) => ({
      label: formatChartTimestampLabel(bucket.bucket_start),
      value: read(bucket) ?? Number.NaN,
      timestamp: Date.parse(bucket.bucket_start),
    }))
    .filter(
      (point) =>
        Number.isFinite(point.value) && Number.isFinite(point.timestamp),
    );
}

export function balancedColumns(count: number, maxColumns: number): number {
  if (count <= 1) {
    return 1;
  }

  const rows = Math.ceil(count / maxColumns);
  return Math.ceil(count / rows);
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

export function truncateMiddle(
  value: string | null | undefined,
  prefixLength = 8,
  suffixLength = 6,
): string {
  if (!value) {
    return '—';
  }

  if (value.length <= prefixLength + suffixLength + 3) {
    return value;
  }

  return `${value.slice(0, prefixLength)}...${value.slice(-suffixLength)}`;
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

export function formatFreshnessLabel(isoTimestamp: string): string {
  const date = new Date(isoTimestamp);
  const timestamp = date.getTime();

  if (Number.isNaN(timestamp)) {
    return isoTimestamp;
  }

  const diffMs = Math.max(0, Date.now() - timestamp);
  const diffMins = Math.floor(diffMs / 60000);
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
}

export function formatDateTime(value: string): string {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return DATE_TIME_FORMAT.format(date);
}

export function formatDurationFromSeconds(
  seconds: number | null | undefined,
): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) {
    return 'Unknown';
  }

  if (seconds < 60) {
    return `${Math.max(0, Math.floor(seconds))}s`;
  }

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ${minutes % 60}m`;
  }

  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

// Expand exponential notation ("2.5707140E+27") into a plain decimal string
// ("2570714000000000000000000000"). BigInt rejects exponents, so a wad value
// serialized in scientific form would otherwise parse to just its leading digit
// and render as 0. The API contract promises plain strings; this is a
// defense-in-depth guard against a Decimal that slips through in scientific
// form. Returns null for non-numeric input.
function toPlainDecimalString(value: string): string | null {
  const trimmed = value.trim();
  if (!/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(trimmed)) {
    return null;
  }

  const exponentAt = trimmed.search(/[eE]/);
  if (exponentAt === -1) {
    return trimmed;
  }

  // A real 1e18-scaled wad has a tiny exponent; a wildly out-of-range one would
  // only make `'0'.repeat(...)` below throw. Reject rather than expand it.
  const exponent = Number(trimmed.slice(exponentAt + 1));
  if (Math.abs(exponent) > 1000) {
    return null;
  }

  const mantissa = trimmed.slice(0, exponentAt);
  const negative = mantissa.startsWith('-');
  const unsigned = mantissa.replace(/^[+-]/, '');
  // Defaulted, not guarded: `.5e19` is a valid mantissa with no integer digits,
  // so an empty leading part is data rather than a missing value.
  const [intRaw = '', fracRaw = ''] = unsigned.split('.');
  const digits = intRaw + fracRaw;
  // Where the decimal point lands, counted in digits from the left of `digits`.
  const pointPos = intRaw.length + exponent;

  let result: string;
  if (pointPos <= 0) {
    result = `0.${'0'.repeat(-pointPos)}${digits}`;
  } else if (pointPos >= digits.length) {
    result = digits + '0'.repeat(pointPos - digits.length);
  } else {
    result = `${digits.slice(0, pointPos)}.${digits.slice(pointPos)}`;
  }

  return negative ? `-${result}` : result;
}

export function formatWadValue(
  value: number | string | null | undefined,
): string {
  if (value === null || value === undefined || value === '') {
    return '—';
  }

  const plain = toPlainDecimalString(String(value));
  if (plain === null) {
    logging.warn(`Failed to parse WAD value: "${value}"`, {
      context: 'formatWadValue',
    });
    return '—';
  }

  try {
    const wei = BigInt(plain.split('.')[0] || '0');
    const wad = 10n ** 18n;
    const whole = wei / wad;
    const fraction = wei % wad;
    const fraction6 = ((fraction * 1_000_000n) / wad)
      .toString()
      .padStart(6, '0');

    return formatTokenAmount(`${whole.toString()}.${fraction6}`);
  } catch {
    logging.warn(`Failed to parse WAD value: "${value}"`, {
      context: 'formatWadValue',
    });
    return '—';
  }
}

// Float conversion for charting only; use formatWadValue for displayed amounts,
// which keeps full precision via BigInt.
export function wadToUnits(
  value: number | string | null | undefined,
): number | null {
  const numeric = parseNumericValue(value, 'wadToUnits');
  return numeric === null ? null : numeric / 1e18;
}

export function formatChartTimestampLabel(value: string): string {
  return new Date(value).toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function toTimestampMs(timestamp: string): number {
  const value = new Date(timestamp).getTime();
  return Number.isFinite(value) ? value : 0;
}

// Returns a new array of time-series buckets sorted oldest-first by
// `bucket_start`. The backend does not guarantee bucket order, and the charts
// assume ascending time, so callers must sort before rendering.
export function sortByBucketStart<T extends { bucket_start: string }>(
  buckets: readonly T[],
): T[] {
  return [...buckets].sort(
    (a, b) => toTimestampMs(a.bucket_start) - toTimestampMs(b.bucket_start),
  );
}

/**
 * Get human-readable label for allocation category.
 */
export function getCategoryLabel(
  category:
    | 'allocation'
    | 'pol'
    | 'psm3'
    | 'asset'
    | 'custody'
    | ''
    | undefined,
  fallback: string = 'Unknown',
): string {
  const labels: Record<string, string> = {
    allocation: 'Allocation',
    pol: 'Protocol Owned Liquidity',
    psm3: 'PSM3',
    asset: 'Asset',
    custody: 'Custody',
  };
  return category ? (labels[category] ?? fallback) : fallback;
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

/**
 * Returns an Etherscan/block-explorer URL for the given chain + address,
 * or null if the chain is not recognised.
 */
export function getExplorerUrl(
  chainId: number | null | undefined,
  address: string,
  type: 'address' | 'tx' = 'address',
): string | null {
  const base = getChainExplorerUrl(chainId);
  if (!base) {
    return null;
  }
  return `${base.replace(/\/+$/, '')}/${type}/${address}`;
}
