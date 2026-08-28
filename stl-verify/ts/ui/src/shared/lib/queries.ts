import type {
  AllocationActivityBucket,
  AllocationActivityEnvelope,
  AllocationActivityResponse,
  DataSourcesResponse,
  ExposureBucket,
  ExposureEnvelope,
  PrimeDebtBucket,
  PrimeDebtEnvelope,
  PrimeDebtSnapshot,
  ProtocolEventsEnvelope,
  ProtocolEventsResponse,
  TimeSeriesResolution,
  TokensResponse,
  TotalCapitalBucket,
  TotalCapitalEnvelope,
} from '../types/allocation';
import { api } from './api-client';
import { sortByBucketStart } from './dashboard';
import { logging } from './logging';
import { sourceQuery } from './provenance';

/**
 * Stand-ins for the params of a query `enabled` has switched off, which still
 * has to build a key. Neither is a real chain or address, and no enabled query
 * ever names them — widening an `enabled` predicate is what would make one real.
 */
export const DISABLED_CHAIN_ID = -1;
export const DISABLED_ADDRESS = '';

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;

/**
 * How long each class of endpoint stays fresh, and how long it survives with no
 * observer. Both are claims about the *data* rather than about the screen, so
 * they are stated once here rather than at each call site.
 */
const CACHE = {
  /** `/v1/chains`, `/v1/protocols`: rows that change on a backend deploy, and a
   * deploy reloads the tab — so within one session they cannot go stale. */
  registry: { staleTime: Infinity, gcTime: 24 * HOUR },
  /** Provenance coverage changes on deploy, not on block. */
  provenance: { staleTime: 30 * MINUTE, gcTime: HOUR },
  /** The prime list is near-static. */
  primes: { staleTime: 5 * MINUTE, gcTime: 30 * MINUTE },
  /** The token catalogue, read for a filter's options and the methodology
   * panel's row count — never for a figure anything is decided on. */
  tokenList: { staleTime: 10 * MINUTE, gcTime: 30 * MINUTE },
  /** The screen's primary per-block data: allocations, risk capital, debt. */
  position: { staleTime: 30_000, gcTime: 10 * MINUTE },
  /** One minute is the finest bucket the range picker can ask for (`PT1M`, at
   * the 1h preset — see `getResolutionForRange`), so a shorter `staleTime`
   * could not change the line that gets drawn. Revisit if a finer preset lands. */
  series: { staleTime: MINUTE, gcTime: 30 * MINUTE },
  /** A daily upstream feed seeded by a one-shot backfill; see the lookback below. */
  referenceSeries: { staleTime: 6 * HOUR, gcTime: 24 * HOUR },
  /** The drawer's own reads. The long `gcTime` is the point: re-opening a row
   * renders from cache immediately, though a refetch may still run behind it. */
  drawer: { staleTime: MINUTE, gcTime: 30 * MINUTE },
  /** Decimals and symbol never change; the row can still be re-published with
   * corrected metadata, which an hour is long enough to be worth catching. */
  tokenMeta: { staleTime: HOUR, gcTime: 24 * HOUR },
  /** A price is the one genuinely live figure here. */
  tokenPrice: { staleTime: 30_000, gcTime: 5 * MINUTE },
  /** A settled transaction's events change only if it is reorged out, so the
   * hour is a reorg-depth bound rather than a freshness one. `gcTime` matches
   * `staleTime` deliberately: the panel unmounts when a row collapses, so
   * `gcTime` is what has to survive the gap between expansions. */
  settledTx: { staleTime: HOUR, gcTime: HOUR },
} as const;

// `sources` is the one genuinely optional envelope field in this file, so it
// gets a stable fallback rather than a fresh array per select run.
const NO_DATA_SOURCES: DataSourcesResponse['sources'] = [];

/** The window every bucketed series is fetched over. */
export type SeriesWindow = {
  fromTimestamp: string | undefined;
  toTimestamp: string | undefined;
  resolution: TimeSeriesResolution;
};

// limit 500 (the per-prime max) so the longest ranges (e.g. 365d at P1D) return
// every bucket rather than being truncated to the default page.
function bucketQuery(range: SeriesWindow) {
  return {
    from_timestamp: range.fromTimestamp,
    to_timestamp: range.toTimestamp,
    resolution: range.resolution,
    aggregate: true,
    limit: 500,
  };
}

/**
 * Unwraps an envelope's rows, rejecting one that did not hold up: a `mode`
 * other than the one the request asked for, or a `data` that is not an array.
 *
 * Both are backend contract violations rather than "no data" — the rows of each
 * mode have incompatible shapes, and `data` is required and non-nullable on
 * every envelope — so surface them instead of handing back mis-typed rows or
 * drawing an empty view over a broken payload. Thrown from a `select`, which
 * react-query reports as the query's own error.
 */
function requireEnvelopeRows<TEnvelope extends { mode: string; data: unknown }>(
  envelope: TEnvelope,
  expected: TEnvelope['mode'],
  label: string,
): TEnvelope['data'] {
  const { data, mode } = envelope;
  if (mode === expected && Array.isArray(data)) {
    return data as TEnvelope['data'];
  }

  const fault = mode === expected ? 'a non-array `data`' : `"${mode}"`;
  // The cache's `onError` only ever sees a rejected `queryFn`; a throwing
  // `select` is caught by the observer, so this would otherwise log nowhere.
  logging.error('API envelope contract violation', {
    label,
    expected,
    mode,
    fault,
  });

  throw new Error(`${label} returned ${fault} for an ${expected} request`);
}

// Selects are module-level so their identity is stable: react-query re-runs a
// select whose reference changed, even when the data behind it did not.

const selectLatestDebtSnapshot = (
  envelope: PrimeDebtEnvelope,
): PrimeDebtSnapshot | null => {
  const snapshots = requireEnvelopeRows(
    envelope,
    'raw',
    'GET /v1/primes/{prime_id}/debt',
  ) as PrimeDebtSnapshot[];
  return snapshots[0] ?? null;
};

const selectLatestDebtBucket = (
  envelope: PrimeDebtEnvelope,
): PrimeDebtBucket | null => {
  const buckets = requireEnvelopeRows(
    envelope,
    'aggregated',
    'GET /v1/primes/{prime_id}/debt',
  ) as PrimeDebtBucket[];
  return buckets[0] ?? null;
};

const selectDebtBuckets = (envelope: PrimeDebtEnvelope): PrimeDebtBucket[] =>
  sortByBucketStart(
    requireEnvelopeRows(
      envelope,
      'aggregated',
      'GET /v1/primes/{prime_id}/debt',
    ) as PrimeDebtBucket[],
  );

const selectActivityBuckets = (
  envelope: AllocationActivityEnvelope,
): AllocationActivityBucket[] => {
  if (envelope.mode === 'aggregated') {
    return sortByBucketStart(
      requireEnvelopeRows(
        envelope,
        'aggregated',
        'GET /v1/allocations/activity',
      ) as AllocationActivityBucket[],
    );
  }

  // Both series ask for `aggregate=true`, so this is the same violation the
  // debt series throws on — coerced because its card degrades, not ignored.
  logging.warn('Allocation activity envelope was not aggregated', {
    mode: envelope.mode,
  });
  return [];
};

const selectTotalCapitalBuckets = (
  envelope: TotalCapitalEnvelope,
): TotalCapitalBucket[] =>
  sortByBucketStart(
    requireEnvelopeRows(
      envelope,
      'aggregated',
      'GET /v1/primes/{prime_id}/total-capital',
    ),
  );

const selectExposureBuckets = (envelope: ExposureEnvelope): ExposureBucket[] =>
  sortByBucketStart(
    requireEnvelopeRows(
      envelope,
      'aggregated',
      'GET /v1/primes/{prime_id}/exposure',
    ),
  );

const selectDataSources = (response: DataSourcesResponse) =>
  response.sources ?? NO_DATA_SOURCES;

const selectRawActivity = (
  envelope: AllocationActivityEnvelope,
): AllocationActivityResponse =>
  requireEnvelopeRows(
    envelope,
    'raw',
    'GET /v1/allocations/activity',
  ) as AllocationActivityResponse;

const selectProtocolEvents = (
  envelope: ProtocolEventsEnvelope,
): ProtocolEventsResponse =>
  requireEnvelopeRows(
    envelope,
    'raw',
    'GET /v1/protocol-events',
  ) as ProtocolEventsResponse;

const selectTokenSymbols = (tokens: TokensResponse): string[] =>
  Array.from(
    new Set(
      tokens
        .map((token) => token.symbol?.trim().toUpperCase() ?? '')
        .filter((symbol) => symbol.length > 0),
    ),
  ).sort((a, b) => a.localeCompare(b));

export const chainsQuery = () =>
  api.queryOptions('get', '/v1/chains', undefined, {
    ...CACHE.registry,
    meta: { logMessage: 'Failed to load the chain registry' },
  });

export const protocolsQuery = () =>
  api.queryOptions('get', '/v1/protocols', undefined, {
    ...CACHE.registry,
    meta: { logMessage: 'Failed to load the protocol registry' },
  });

export const primesQuery = () =>
  api.queryOptions('get', '/v1/primes', undefined, {
    ...CACHE.primes,
    meta: { logMessage: 'Failed to load primes' },
  });

export const dataSourcesQuery = () =>
  api.queryOptions('get', '/v1/data-sources', undefined, {
    ...CACHE.provenance,
    select: selectDataSources,
    meta: { logMessage: 'Failed to fetch data sources' },
  });

export const provenanceAvailabilityQuery = () =>
  api.queryOptions('get', '/v1/provenance/available', undefined, {
    ...CACHE.provenance,
    meta: {
      logLevel: 'warn',
      logMessage: 'Provenance coverage unavailable; offering every source',
    },
  });

/**
 * One ALM proxy's allocations.
 *
 * Keyed per proxy on purpose: a prime allocates through one proxy per chain, so
 * the fan-out is several of these and each chain's rows cache on their own.
 */
export const allocationsQuery = (proxyAddress: string) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/allocations',
    {
      params: {
        path: { prime_id: proxyAddress },
        query: { ...sourceQuery },
      },
    },
    {
      ...CACHE.position,
      meta: { logMessage: 'Failed to load allocations' },
    },
  );

/**
 * The prime's risk capital.
 *
 * The `prime_*` fields are aggregated prime-wide server-side, so one call
 * against the primary proxy carries the same figures every other proxy would.
 */
export const riskCapitalQuery = (primeId: string) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/risk-capital',
    {
      params: {
        path: { prime_id: primeId },
        query: { ...sourceQuery },
      },
    },
    {
      ...CACHE.position,
      meta: {
        logLevel: 'warn',
        logMessage: 'Risk capital unavailable for selected prime',
      },
    },
  );

export const latestDebtSnapshotQuery = (primeId: string) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/debt',
    { params: { path: { prime_id: primeId }, query: { limit: 1 } } },
    {
      ...CACHE.position,
      select: selectLatestDebtSnapshot,
      meta: {
        logLevel: 'warn',
        logMessage: 'Prime debt snapshot unavailable for selected prime',
      },
    },
  );

/**
 * How far back to look for the newest reference debt bucket. The endpoint
 * defaults to the last 24h, but this series is a daily upstream feed seeded by
 * a one-shot backfill, so the most recent bucket is routinely older than that
 * and the default window returns nothing at all.
 */
const REFERENCE_DEBT_LOOKBACK_DAYS = 90;

/**
 * The lower bound, quantised to the UTC day.
 *
 * `Date.now()` here would be a fresh bound on every render, and the bound is
 * part of the cache key — so the query would refetch forever. The feed is
 * daily, so a finer boundary buys nothing anyway.
 */
function referenceDebtLookbackStart(): string {
  const start = new Date();
  start.setUTCHours(0, 0, 0, 0);
  start.setUTCDate(start.getUTCDate() - REFERENCE_DEBT_LOOKBACK_DAYS);
  return start.toISOString();
}

/**
 * The newest reference debt bucket.
 *
 * Reference debt is aggregate-only: upstream reports one figure per prime per
 * day and carries no ilk or block identity, so the API rejects a raw request
 * rather than inventing them.
 */
export const latestReferenceDebtQuery = (primeId: string) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/debt',
    {
      params: {
        path: { prime_id: primeId },
        query: {
          aggregate: true,
          limit: 1,
          source: 'reference' as const,
          from_timestamp: referenceDebtLookbackStart(),
        },
      },
    },
    {
      ...CACHE.referenceSeries,
      select: selectLatestDebtBucket,
      meta: {
        logLevel: 'warn',
        logMessage: 'Reference debt bucket unavailable for selected prime',
      },
    },
  );

/**
 * The prime-debt series, which is the metric band's primary one: its card
 * surfaces an error rather than degrading, so this asks for `aggregated` and
 * refuses anything else.
 */
export const debtSeriesQuery = (primeId: string, window: SeriesWindow) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/debt',
    {
      params: {
        path: { prime_id: primeId },
        // The metric beside this chart reads the same provenance; leaving the
        // chart on self data would put both in one card.
        query: { ...bucketQuery(window), ...sourceQuery },
      },
    },
    {
      ...CACHE.series,
      select: selectDebtBuckets,
      meta: { logMessage: 'Failed to load chart buckets' },
    },
  );

// The three supplementary series. Each degrades to its card's current-value
// fallback on failure rather than blanking the view, which is why they log at
// `warn` and why nothing reads their errors.

export const activitySeriesQuery = (primeId: string, window: SeriesWindow) =>
  api.queryOptions(
    'get',
    '/v1/allocations/activity',
    { params: { query: { prime_id: primeId, ...bucketQuery(window) } } },
    {
      ...CACHE.series,
      select: selectActivityBuckets,
      meta: {
        logLevel: 'warn',
        logMessage:
          'Allocation activity history unavailable; using current value',
      },
    },
  );

export const totalCapitalSeriesQuery = (
  primeId: string,
  window: SeriesWindow,
) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/total-capital',
    {
      params: {
        path: { prime_id: primeId },
        query: { ...bucketQuery(window), ...sourceQuery },
      },
    },
    {
      ...CACHE.series,
      select: selectTotalCapitalBuckets,
      meta: {
        logLevel: 'warn',
        logMessage: 'Total capital history unavailable; using current value',
      },
    },
  );

export const exposureSeriesQuery = (primeId: string, window: SeriesWindow) =>
  api.queryOptions(
    'get',
    '/v1/primes/{prime_id}/exposure',
    {
      params: {
        path: { prime_id: primeId },
        query: { ...bucketQuery(window), ...sourceQuery },
      },
    },
    {
      ...CACHE.series,
      select: selectExposureBuckets,
      meta: {
        logLevel: 'warn',
        logMessage: 'Exposure history unavailable; using current value',
      },
    },
  );

export const riskBreakdownQuery = (
  chainId: number,
  tokenAddress: string,
  primeId: string | null,
) =>
  api.queryOptions(
    'get',
    '/v1/risk/{chain_id}/{token_address}/breakdown',
    {
      params: {
        path: { chain_id: chainId, token_address: tokenAddress },
        query: primeId ? { prime_id: primeId } : undefined,
      },
    },
    {
      ...CACHE.drawer,
      meta: { logMessage: 'Failed to load risk breakdown' },
    },
  );

export const rrcQuery = (
  chainId: number,
  tokenAddress: string,
  primeAddress: string,
) =>
  api.queryOptions(
    'get',
    '/v1/risk/rrc',
    {
      params: {
        query: {
          chain_id: chainId,
          prime_id: primeAddress,
          token_address: tokenAddress,
        },
      },
    },
    {
      ...CACHE.drawer,
      meta: { logMessage: 'Failed to load required risk capital (RRC)' },
    },
  );

export const tokenQuery = (chainId: number, tokenAddress: string) =>
  api.queryOptions(
    'get',
    '/v1/tokens/{chain_id}/{token_address}',
    { params: { path: { chain_id: chainId, token_address: tokenAddress } } },
    {
      ...CACHE.tokenMeta,
      // `warn`, not `error`, at both call sites: the drawer degrades silently
      // and the methodology panel says so on screen, and one shared cache entry
      // cannot carry two levels.
      meta: {
        logLevel: 'warn',
        logMessage: 'Token catalog metadata unavailable',
      },
    },
  );

export const tokenPriceQuery = (chainId: number, tokenAddress: string) =>
  api.queryOptions(
    'get',
    '/v1/tokens/{chain_id}/{token_address}/price',
    { params: { path: { chain_id: chainId, token_address: tokenAddress } } },
    {
      ...CACHE.tokenPrice,
      meta: {
        logLevel: 'warn',
        logMessage: 'Token price metadata unavailable',
      },
    },
  );

/**
 * The activity feed's rows, which are raw events rather than buckets — the same
 * endpoint the metric band reads with `aggregate=true`, so the two share no
 * cache entry and neither can serve the other's shape.
 */
export const activityQuery = (filters: {
  prime_id?: string;
  chain_id?: number;
  protocol_name?: string;
  action_type?: string;
  token_symbol?: string;
  from_timestamp?: string;
  to_timestamp?: string;
  limit?: number;
}) =>
  api.queryOptions(
    'get',
    '/v1/allocations/activity',
    { params: { query: filters } },
    {
      ...CACHE.position,
      select: selectRawActivity,
      meta: { logMessage: 'Failed to fetch allocation activity' },
    },
  );

/** The rows the generic protocol-events filter returns before truncating. */
export const FALLBACK_TX_EVENT_LIMIT = 200;

export const txProtocolEventsQuery = (txHash: string) =>
  api.queryOptions(
    'get',
    '/v1/tx/{tx_hash}/events',
    { params: { path: { tx_hash: txHash } } },
    {
      ...CACHE.settledTx,
      // A 404 here is the fallback's trigger rather than an outage, and it is
      // the common case on a deployment without the endpoint — so `warn`, and
      // a message that says what happens next rather than "failed".
      meta: {
        logLevel: 'warn',
        logMessage:
          'Dedicated tx-events endpoint unavailable; trying the generic filter',
      },
    },
  );

/**
 * The same events off the generic filter, for a deployment without the
 * dedicated endpoint. Its caller enables it only once that one has answered
 * 404, so a real outage of it still surfaces as an error.
 */
export const txProtocolEventsFallbackQuery = (txHash: string) =>
  api.queryOptions(
    'get',
    '/v1/protocol-events',
    { params: { query: { tx_hash: txHash, limit: FALLBACK_TX_EVENT_LIMIT } } },
    {
      ...CACHE.settledTx,
      select: selectProtocolEvents,
      meta: {
        logMessage: 'Failed to fetch tx protocol events from the filter',
      },
    },
  );

export type TokenFilters = {
  chain_id?: number;
  symbol?: string;
  limit?: number;
};

export const tokensQuery = (filters: TokenFilters) =>
  api.queryOptions(
    'get',
    '/v1/tokens',
    { params: { query: filters } },
    {
      ...CACHE.tokenList,
      meta: { logLevel: 'warn', logMessage: 'Token catalogue unavailable' },
    },
  );

/**
 * The activity view's token filter options.
 *
 * The catalogue is read only to derive this list, so the projection is the
 * query: nothing downstream wants the rows themselves.
 */
export const tokenSymbolsQuery = () =>
  api.queryOptions(
    'get',
    '/v1/tokens',
    { params: { query: { limit: 500 } } },
    {
      ...CACHE.tokenList,
      select: selectTokenSymbols,
      meta: {
        logLevel: 'warn',
        logMessage: 'Failed to load token options for activities view',
      },
    },
  );
