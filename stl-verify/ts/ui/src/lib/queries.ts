import {
  createApiClient,
  createQueryApi,
} from '@archon-research/http-client-react';

import type { paths } from '../generated/openapi-types';
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
  ProtocolEventsResponse,
  TimeSeriesResolution,
  TokensResponse,
  TotalCapitalBucket,
  TotalCapitalEnvelope,
} from '../types/allocation';
import { sortByBucketStart } from './dashboard';
import { sourceQuery } from './provenance';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';

// `openapi-typescript` emits `paths` as an interface, and an interface carries
// no implicit index signature — so it does not satisfy `createQueryApi`'s
// `Record<string, …>` bound, and inference silently falls back to it.
type ApiPaths = { [Path in keyof paths]: paths[Path] };

const apiClient = createApiClient<ApiPaths>(API_BASE_URL);

/**
 * The typed query surface, derived from the generated `paths`.
 *
 * No tag vocabulary: tags exist to be invalidated by mutations, and this app
 * issues none. Add one here when the first write lands, not before.
 */
const api = createQueryApi(apiClient);

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;

/**
 * How long each class of endpoint stays fresh, and how long it survives with no
 * observer. Both are claims about the *data* rather than about the screen, so
 * they are stated once here rather than at each call site.
 */
const CACHE = {
  /** `/v1/chains`, `/v1/protocols`: a static registry, per `lib/chain-metadata`. */
  registry: { staleTime: Infinity, gcTime: 24 * HOUR },
  /** Provenance coverage changes on deploy, not on block. */
  provenance: { staleTime: 30 * MINUTE, gcTime: HOUR },
  /** The prime list is near-static. */
  primes: { staleTime: 5 * MINUTE, gcTime: 30 * MINUTE },
  /** The token catalogue, read only to populate a filter's options. */
  tokenList: { staleTime: 10 * MINUTE, gcTime: 30 * MINUTE },
  /** The screen's primary per-block data: allocations, risk capital, debt. */
  position: { staleTime: 30_000, gcTime: 10 * MINUTE },
  /** Bucketed to PT15M and coarser, so sub-minute refetching cannot change the answer. */
  series: { staleTime: MINUTE, gcTime: 30 * MINUTE },
  /** A daily upstream feed seeded by a one-shot backfill; see the lookback below. */
  referenceSeries: { staleTime: 6 * HOUR, gcTime: 24 * HOUR },
  /** The drawer's own reads. The long `gcTime` is the point: re-opening a row is free. */
  drawer: { staleTime: MINUTE, gcTime: 30 * MINUTE },
  /** Token metadata is immutable once published. */
  tokenMeta: { staleTime: HOUR, gcTime: 24 * HOUR },
  /** A price is the one genuinely live figure here. */
  tokenPrice: { staleTime: 30_000, gcTime: 5 * MINUTE },
  /** A settled transaction's decoded events do not change absent a reorg. */
  settledTx: { staleTime: HOUR, gcTime: HOUR },
} as const;

/** The window every bucketed series is fetched over. */
export type SeriesWindow = {
  fromTimestamp: string | undefined;
  toTimestamp: string | undefined;
  resolution: TimeSeriesResolution;
};

// limit 500 (the per-prime max) so the longest ranges (e.g. 365d at P1D) return
// every bucket rather than being truncated to the default page.
function bucketQuery(window: SeriesWindow) {
  return {
    from_timestamp: window.fromTimestamp,
    to_timestamp: window.toTimestamp,
    resolution: window.resolution,
    aggregate: true,
    limit: 500,
  };
}

/**
 * Rejects an envelope whose `mode` is not the one the request asked for.
 *
 * The rows of each mode have incompatible shapes, so a disagreement is a
 * backend contract violation rather than "no data" — surface it instead of
 * handing back mis-typed rows. Thrown from a `select`, which react-query
 * reports as the query's own error.
 */
function requireEnvelopeMode(
  envelope: { mode: string },
  expected: string,
  label: string,
): void {
  if (envelope.mode !== expected) {
    throw new Error(
      `${label} returned "${envelope.mode}" for an ${expected} request`,
    );
  }
}

// Selects are module-level so their identity is stable: react-query re-runs a
// select whose reference changed, even when the data behind it did not.

const selectLatestDebtSnapshot = (
  envelope: PrimeDebtEnvelope,
): PrimeDebtSnapshot | null => {
  requireEnvelopeMode(envelope, 'raw', 'GET /v1/primes/{prime_id}/debt');
  return ((envelope.data ?? []) as PrimeDebtSnapshot[])[0] ?? null;
};

const selectLatestDebtBucket = (
  envelope: PrimeDebtEnvelope,
): PrimeDebtBucket | null =>
  ((envelope.data ?? []) as PrimeDebtBucket[])[0] ?? null;

const selectDebtBuckets = (envelope: PrimeDebtEnvelope): PrimeDebtBucket[] => {
  requireEnvelopeMode(envelope, 'aggregated', 'GET /v1/primes/{prime_id}/debt');
  return sortByBucketStart(envelope.data as PrimeDebtBucket[]);
};

// A non-aggregated activity envelope means "no data" here, unlike the debt
// series above, where `aggregate=true` is the whole request.
const selectActivityBuckets = (
  envelope: AllocationActivityEnvelope,
): AllocationActivityBucket[] =>
  envelope.mode === 'aggregated'
    ? sortByBucketStart(envelope.data as AllocationActivityBucket[])
    : [];

const selectTotalCapitalBuckets = (
  envelope: TotalCapitalEnvelope,
): TotalCapitalBucket[] =>
  sortByBucketStart(envelope.data as TotalCapitalBucket[]);

const selectExposureBuckets = (envelope: ExposureEnvelope): ExposureBucket[] =>
  sortByBucketStart(envelope.data as ExposureBucket[]);

const selectDataSources = (response: DataSourcesResponse) =>
  response.sources ?? [];

const selectRawActivity = (
  envelope: AllocationActivityEnvelope,
): AllocationActivityResponse => {
  requireEnvelopeMode(envelope, 'raw', 'GET /v1/allocations/activity');
  return (envelope.data ?? []) as AllocationActivityResponse;
};

const selectProtocolEvents = (envelope: {
  data?: unknown;
}): ProtocolEventsResponse => (envelope.data ?? []) as ProtocolEventsResponse;

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

// How far back to look for the newest reference debt bucket. The endpoint
// defaults to the last 24h, but this series is a daily upstream feed seeded by
// a one-shot backfill, so the most recent bucket is routinely older than that
// and the default window returns nothing at all.
const REFERENCE_DEBT_LOOKBACK_DAYS = 90;

// Quantised to the UTC day. `Date.now()` here would be a fresh bound on every
// render, and the bound is part of the cache key — so the query would refetch
// forever. The feed is daily, so a finer boundary buys nothing anyway.
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
        logMessage: 'Prime debt snapshot unavailable for selected prime',
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
      meta: { logMessage: 'Failed to fetch tx protocol events' },
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
      meta: { logMessage: 'Failed to fetch tx protocol events' },
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
