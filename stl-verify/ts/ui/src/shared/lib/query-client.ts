import { isHttpRequestError } from '@archon-research/http-client-react';
import { QueryCache, QueryClient } from '@tanstack/react-query';

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

// A 4xx is an answer, not an outage: retrying one only delays the error the
// caller is already equipped to render. Anything else gets three attempts
// rather than react-query's default four — this screen issues a dozen requests,
// and a fourth round of backoff on all of them outlasts anyone's patience.
function retryUnlessClientError(failureCount: number, error: Error): boolean {
  if (isHttpRequestError(error) && error.status < 500) {
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
