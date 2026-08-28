import {
  createApiClient,
  createQueryApi,
} from '@archon-research/http-client-react';

import type { paths } from '../generated/openapi-types';
import type {
  PrimeDebtBucket,
  PrimeDebtEnvelope,
  PrimeDebtSnapshot,
  TokensResponse,
} from '../types/allocation';
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
  /** A daily upstream feed seeded by a one-shot backfill; see the lookback below. */
  referenceSeries: { staleTime: 6 * HOUR, gcTime: 24 * HOUR },
} as const;

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
