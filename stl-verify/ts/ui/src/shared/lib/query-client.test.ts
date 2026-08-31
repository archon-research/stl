import { HttpRequestError } from '@archon-research/http-client-react';
import { describe, expect, it } from 'vitest';

import { queryClient } from './query-client';

/**
 * The retry predicate off the client's defaults, which is the surface
 * react-query calls it through — it is deliberately module-private.
 */
function defaultRetry(): (failureCount: number, error: Error) => boolean {
  const { retry } = queryClient.getDefaultOptions().queries ?? {};
  if (typeof retry !== 'function') {
    throw new Error('the query client carries no retry predicate');
  }
  return retry;
}

const retry = defaultRetry();

const httpError = (status: number) =>
  new HttpRequestError({
    method: 'get',
    path: '/v1/primes',
    body: undefined,
    response: new Response(null, { status }),
  });

describe('the default retry policy', () => {
  it.each([400, 401, 403, 404, 422])('gives up immediately on %i', (status) => {
    expect(retry(0, httpError(status))).toBe(false);
  });

  it.each([408, 425, 429])(
    'retries %i, which is a timing accident',
    (status) => {
      expect(retry(0, httpError(status))).toBe(true);
    },
  );

  it.each([500, 502, 503])('retries %i', (status) => {
    expect(retry(0, httpError(status))).toBe(true);
  });

  it('retries an error that carries no status at all', () => {
    expect(retry(0, new Error('network down'))).toBe(true);
  });

  it('stops a retryable failure at three attempts', () => {
    expect(retry(1, httpError(429))).toBe(true);
    expect(retry(2, httpError(429))).toBe(false);
  });
});
