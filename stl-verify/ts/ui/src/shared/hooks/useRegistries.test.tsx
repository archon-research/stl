import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { chainsQuery } from '../lib/queries';
import {
  hasCompleteRows,
  type RegistryStatus,
  useLocalChains,
} from './useRegistries';

/**
 * What `useLocalChains` reports out of a given cache.
 *
 * Rendered through a real `QueryClientProvider` rather than asserted on the
 * hook's shape: the status is read off `useQuery`, so a stub would pin the
 * mapping without pinning that the mapping is of anything.
 */
function readChains(client: QueryClient): RegistryStatus {
  const seen: RegistryStatus[] = [];

  function Probe() {
    const { isPending, isError } = useLocalChains();
    seen.push({ isPending, isError });
    return null;
  }

  renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <Probe />
    </QueryClientProvider>,
  );

  const status = seen.at(0);
  if (!status) {
    throw new Error('the probe never rendered');
  }
  return status;
}

// `retryOnMount` off because the probe renders without mounting: react-query
// otherwise reports a settled error optimistically as the retry it would run.
const newClient = () =>
  new QueryClient({ defaultOptions: { queries: { retryOnMount: false } } });

async function chainsThatFailed(): Promise<QueryClient> {
  const client = newClient();
  await client.prefetchQuery({
    queryKey: chainsQuery().queryKey,
    queryFn: () => Promise.reject(new Error('the chain registry is down')),
    retry: false,
  });
  return client;
}

function chainsThatAnsweredEmpty(): QueryClient {
  const client = newClient();
  client.setQueryData(chainsQuery().queryKey, []);
  return client;
}

describe('a registry read', () => {
  it('reports a failure as failed rather than as still pending', async () => {
    expect(readChains(await chainsThatFailed())).toEqual({
      isPending: false,
      isError: true,
    });
  });

  it('reports an unanswered registry as pending', () => {
    expect(readChains(newClient())).toEqual({
      isPending: true,
      isError: false,
    });
  });

  it('reports an empty 200 as neither pending nor failed', () => {
    expect(readChains(chainsThatAnsweredEmpty())).toEqual({
      isPending: false,
      isError: false,
    });
  });
});

describe('hasCompleteRows', () => {
  // The predicate that gates pruning, which deletes a filter from the URL. Both
  // false cases are ones where the option list on hand is not the real one.
  it('refuses a registry that has not answered', () => {
    expect(hasCompleteRows({ isPending: true, isError: false })).toBe(false);
  });

  it('refuses a registry that failed', () => {
    expect(hasCompleteRows({ isPending: false, isError: true })).toBe(false);
  });

  it('accepts a settled success, empty rows and all', () => {
    expect(hasCompleteRows({ isPending: false, isError: false })).toBe(true);
  });

  it('rules on the statuses a real read produces', async () => {
    expect(hasCompleteRows(readChains(await chainsThatFailed()))).toBe(false);
    expect(hasCompleteRows(readChains(chainsThatAnsweredEmpty()))).toBe(true);
  });
});
