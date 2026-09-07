import { isHttpRequestError } from '@archon-research/http-client-react';
import {
  type NetworkMode,
  QueryCache,
  QueryClient,
} from '@tanstack/react-query';

import { logging } from './logging';

/**
 * How a query wants its failures reported, so that one central handler can log
 * with the severity the call site means: `error` for a series the view cannot
 * do without, `warn` for one that degrades to a fallback on its own.
 *
 * Registered rather than exported, so `meta` is typed at every query that sets
 * it without any of them importing this. It has to stay a `type` alias — as an
 * `interface` it has no implicit index signature, `QueryMeta` silently falls
 * back to `Record<string, unknown>`, and nothing fails.
 */
type QueryLogMeta = {
  logLevel?: 'warn' | 'error';
  logMessage?: string;
};

declare module '@tanstack/react-query' {
  interface Register {
    queryMeta: QueryLogMeta;
  }
}

function logQueryFailure(
  error: unknown,
  queryKey: readonly unknown[],
  meta: QueryLogMeta | undefined,
): void {
  const level = meta?.logLevel ?? 'error';
  const message = meta?.logMessage ?? 'API request failed';
  const http = isHttpRequestError(error) ? error : undefined;

  logging[level](message, {
    error,
    queryKey,
    status: http?.status,
    statusText: http?.statusText,
  });
}

/**
 * Whether a query is allowed to fetch while the browser calls itself offline.
 *
 * react-query's `'online'` default parks a query with no cached data in
 * `pending`/`paused` and leaves it there — `isPending` true, `error`
 * undefined, no timeout — so a drawer tab opened on an uncached row shows its
 * skeleton for good. `'always'` lets the fetch run and reject, which is a state
 * the tab already renders, with a retry the reader can reach.
 *
 * The cost is `onlineManager` gating: a query that would have waited out a blip
 * and resumed on reconnect instead spends its retries on a dead network and
 * settles as an error, after the full backoff, for the reader to retry.
 */
const NETWORK_MODE: NetworkMode = 'always';

/**
 * The 4xx statuses that say "not now" rather than "no": the request itself was
 * acceptable and the identical one may well succeed.
 *
 * 429 is the one that bites here — this screen opens a dozen requests at once,
 * so it is the first paint that trips a rate limiter, and `staleTime: Infinity`
 * on the registries plus `refetchOnWindowFocus: false` mean a query stranded
 * there stays stranded until the tab is reloaded.
 */
const RETRYABLE_CLIENT_ERRORS: ReadonlySet<number> = new Set([
  408, // Request Timeout
  425, // Too Early
  429, // Too Many Requests
]);

// A 4xx is otherwise an answer, not an outage: retrying one only delays the
// error the caller is already equipped to render. Anything else gets three
// attempts rather than react-query's default four — this screen issues a dozen
// requests, and a fourth round of backoff on all of them outlasts anyone's
// patience.
function retryUnlessClientError(failureCount: number, error: Error): boolean {
  if (
    isHttpRequestError(error) &&
    error.status < 500 &&
    !RETRYABLE_CLIENT_ERRORS.has(error.status)
  ) {
    return false;
  }

  return failureCount < 2;
}

/**
 * The app's cache.
 *
 * Constructed here rather than taken from `createQueryClient()` so the defaults
 * below are stated: the package's factory takes no options, and `HttpProvider`
 * accepts a client.
 */
function createAppQueryClient(): QueryClient {
  return new QueryClient({
    queryCache: new QueryCache({
      onError: (error, query) =>
        logQueryFailure(error, query.queryKey, query.meta),
    }),
    defaultOptions: {
      queries: {
        // Every query below states its own; these only cover an endpoint that
        // forgets to.
        staleTime: 30_000,
        gcTime: 5 * 60_000,
        networkMode: NETWORK_MODE,
        // This screen issues a dozen requests on first paint. Refiring them
        // because someone alt-tabbed back is cost without an answer anyone
        // asked for.
        refetchOnWindowFocus: false,
        retry: retryUnlessClientError,
      },
    },
  });
}

/**
 * Exported as the instance rather than a factory because the route loaders need
 * the same cache the components read, and they run outside React — a second
 * client would have them filling a cache nothing observes.
 */
export const queryClient: QueryClient = createAppQueryClient();
