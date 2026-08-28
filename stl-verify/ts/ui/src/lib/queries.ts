import {
  createApiClient,
  createQueryApi,
} from '@archon-research/http-client-react';

import type { paths } from '../generated/openapi-types';
import type { TokensResponse } from '../types/allocation';

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
} as const;

// Selects are module-level so their identity is stable: react-query re-runs a
// select whose reference changed, even when the data behind it did not.

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
