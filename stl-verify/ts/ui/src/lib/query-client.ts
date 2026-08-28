import { isHttpRequestError } from '@archon-research/http-client-react';
import { QueryCache, QueryClient } from '@tanstack/react-query';

import { logging } from './logging';

/**
 * How a query wants its failures reported.
 *
 * Every fetch used to carry its own `.catch` that logged with the context that
 * mattered; centralising the handler would have flattened that to one anonymous
 * message. This carries the distinction instead: `error` is a series the view
 * cannot do without, `warn` is one that degrades to a fallback on its own.
 *
 * Registered rather than exported, so `meta` is typed at every query that sets
 * it without any of them importing this.
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
  const status = isHttpRequestError(error) ? error.status : undefined;

  logging[level](message, { error, queryKey, status });
}

// A 4xx is an answer, not an outage: retrying one only delays the error the
// caller is already equipped to render.
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
export function createAppQueryClient(): QueryClient {
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
