import { HttpRequestError } from '@archon-research/http-client-react';
import { describe, expect, it, vi } from 'vitest';

import { queryClient } from './query-client';

describe("the app's query client defaults", () => {
  it('states its own staleTime rather than inheriting react-query default', () => {
    expect(queryClient.getDefaultOptions().queries?.staleTime).toBe(30_000);
  });

  it('keeps cached data for 5 minutes', () => {
    expect(queryClient.getDefaultOptions().queries?.gcTime).toBe(5 * 60_000);
  });

  it("stays 'always' so an uncached query fetches while offline instead of parking in pending forever", () => {
    expect(queryClient.getDefaultOptions().queries?.networkMode).toBe('always');
  });
});

describe('the policy inherited from createQueryClient', () => {
  /**
   * A future uikit change that drops or weakens `shouldRetryRequest` should
   * fail here, not silently ship — this app never restates the policy itself.
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

  it('gives up immediately on a non-retryable 4xx', () => {
    expect(retry(0, httpError(404))).toBe(false);
  });

  it('retries a 429', () => {
    expect(retry(0, httpError(429))).toBe(true);
  });

  it('stays disabled by refetchOnWindowFocus', () => {
    expect(queryClient.getDefaultOptions().queries?.refetchOnWindowFocus).toBe(
      false,
    );
  });
});

describe('the queryCache logging hook', () => {
  it('logs a failed query at error level by default', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      await queryClient.fetchQuery({
        queryKey: ['query-client-test', 'default-level'],
        queryFn: () => Promise.reject(new Error('boom')),
        retry: false,
      });
    } catch {
      // The rejection itself is not under test; the logging side effect is.
    }

    expect(errorSpy).toHaveBeenCalledWith(
      'API request failed',
      expect.objectContaining({
        queryKey: ['query-client-test', 'default-level'],
      }),
    );
    errorSpy.mockRestore();
  });

  it("honors a query's own logLevel and logMessage", async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      await queryClient.fetchQuery({
        queryKey: ['query-client-test', 'custom-level'],
        queryFn: () => Promise.reject(new Error('boom')),
        retry: false,
        meta: { logLevel: 'warn', logMessage: 'fell back to cache' },
      });
    } catch {
      // The rejection itself is not under test; the logging side effect is.
    }

    expect(warnSpy).toHaveBeenCalledWith(
      'fell back to cache',
      expect.objectContaining({
        queryKey: ['query-client-test', 'custom-level'],
      }),
    );
    warnSpy.mockRestore();
  });
});
