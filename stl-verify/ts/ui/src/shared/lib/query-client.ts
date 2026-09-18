import {
  createQueryClient,
  isHttpRequestError,
} from '@archon-research/http-client-react';
import {
  type NetworkMode,
  QueryCache,
  type QueryClient,
} from '@tanstack/react-query';

import { logging } from './logging';
import {
  browserSessionReloadDeps,
  isSessionExpired,
  reloadForLogin,
} from './session-expired';

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
 * The app's cache: `createQueryClient`'s `refetchOnWindowFocus: false` and
 * status-aware retry, plus the app-specific defaults it deliberately leaves
 * unset — `staleTime`, `gcTime` and `networkMode` are a product decision, not
 * a package one — and this app's failure logging.
 */
function createAppQueryClient(): QueryClient {
  return createQueryClient({
    queryCache: new QueryCache({
      onError: (error, query) => {
        const browser = browserSessionReloadDeps();
        if (
          isSessionExpired(error) &&
          browser !== null &&
          reloadForLogin(browser)
        ) {
          return;
        }
        logQueryFailure(error, query.queryKey, query.meta);
      },
    }),
    defaultOptions: {
      queries: {
        // Every query below states its own; these only cover an endpoint that
        // forgets to.
        staleTime: 30_000,
        gcTime: 5 * 60_000,
        networkMode: NETWORK_MODE,
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
