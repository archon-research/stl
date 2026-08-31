import { HttpRequestError } from '@archon-research/http-client-react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  AllocationActivityEnvelope,
  ExposureEnvelope,
  PrimeDebtEnvelope,
  TokensResponse,
} from '../types/allocation';
import { toQueryErrorMessage } from './errors';
import {
  activitySeriesQuery,
  debtSeriesQuery,
  exposureSeriesQuery,
  latestReferenceDebtQuery,
  type SeriesWindow,
  tokenSymbolsQuery,
} from './queries';

/**
 * The `select` off a query's options, which is where the transforms under test
 * live — they are deliberately module-private, and this is the surface the
 * cache actually calls them through.
 */
function selectOf<TData, TSelected>(options: {
  select?: (data: TData) => TSelected;
}): (data: TData) => TSelected {
  const { select } = options;
  if (!select) {
    throw new Error('query options carry no select');
  }
  return select;
}

/** The sanitized init a query key carries, which is what the cache compares. */
function keyInitOf(options: {
  queryKey: readonly [string, string, { query?: Record<string, unknown> }];
}) {
  const [, , init] = options.queryKey;
  return init;
}

const WINDOW: SeriesWindow = {
  fromTimestamp: '2026-08-27T00:00:00.000Z',
  toTimestamp: '2026-08-28T00:00:00.000Z',
  resolution: 'PT15M',
};

const PRIME = '0x1601843c5e9bc251a3272907010afa41fa18347e';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('latestReferenceDebtQuery', () => {
  // The bound is part of the cache key. Were it read straight off the clock the
  // key would differ on every render and the query would refetch forever, so
  // these pin the quantisation rather than the arithmetic.
  it('quantises its lower bound to UTC midnight', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-08-28T13:47:11.412Z'));

    expect(keyInitOf(latestReferenceDebtQuery(PRIME)).query).toMatchObject({
      from_timestamp: '2026-05-30T00:00:00.000Z',
    });
  });

  it('returns the same bound twice within one UTC day', () => {
    vi.useFakeTimers();

    vi.setSystemTime(new Date('2026-08-28T00:00:00.000Z'));
    const atMidnight = keyInitOf(latestReferenceDebtQuery(PRIME)).query;

    vi.setSystemTime(new Date('2026-08-28T23:59:59.999Z'));
    const beforeNextMidnight = keyInitOf(latestReferenceDebtQuery(PRIME)).query;

    expect(beforeNextMidnight).toStrictEqual(atMidnight);
  });

  it('moves the bound across a month boundary', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-03-01T09:00:00.000Z'));

    // 90 days back from 1 March 2026 lands in the previous December.
    expect(keyInitOf(latestReferenceDebtQuery(PRIME)).query).toMatchObject({
      from_timestamp: '2025-12-01T00:00:00.000Z',
    });
  });
});

describe('the token-symbol projection', () => {
  const select = selectOf<TokensResponse, string[]>(tokenSymbolsQuery());

  const token = (symbol: string | null) =>
    ({ symbol }) as TokensResponse[number];

  it('upper-cases, trims and de-duplicates', () => {
    expect(
      select([token(' usdc '), token('USDC'), token('dai')]),
    ).toStrictEqual(['DAI', 'USDC']);
  });

  it('drops tokens with no usable symbol', () => {
    expect(select([token(null), token('   '), token('WETH')])).toStrictEqual([
      'WETH',
    ]);
  });

  it('sorts the result', () => {
    expect(select([token('WETH'), token('AAVE'), token('DAI')])).toStrictEqual([
      'AAVE',
      'DAI',
      'WETH',
    ]);
  });
});

describe('envelope payload policy', () => {
  // `data` is required and non-nullable on every envelope, so a missing one is
  // a contract violation — and a `select` that throws logs nowhere by itself.
  it('rejects an envelope whose data is not an array', () => {
    const error = vi
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    const select = selectOf<PrimeDebtEnvelope, unknown>(
      debtSeriesQuery(PRIME, WINDOW),
    );

    expect(() =>
      select({
        mode: 'aggregated',
        data: null,
      } as unknown as PrimeDebtEnvelope),
    ).toThrow(/returned a non-array `data` for an aggregated request/);
    expect(error).toHaveBeenCalledOnce();
  });

  it('rejects it on a single-mode series too', () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const select = selectOf<ExposureEnvelope, unknown>(
      exposureSeriesQuery(PRIME, WINDOW),
    );

    expect(() =>
      select({ mode: 'aggregated', data: null } as unknown as ExposureEnvelope),
    ).toThrow(/GET \/v1\/primes\/\{prime_id\}\/exposure returned a non-array/);
  });
});

describe('envelope mode policy', () => {
  // The two series ask for `aggregate=true` alike; only what they do with a
  // disagreeing answer differs, and that asymmetry is deliberate.
  it('rejects a raw envelope on the primary debt series', () => {
    const select = selectOf<PrimeDebtEnvelope, unknown>(
      debtSeriesQuery(PRIME, WINDOW),
    );

    expect(() =>
      select({ mode: 'raw', data: [] } as unknown as PrimeDebtEnvelope),
    ).toThrow(/returned "raw" for an aggregated request/);
  });

  it('coerces a raw envelope to no data on the supplementary activity series', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const select = selectOf<AllocationActivityEnvelope, unknown[]>(
      activitySeriesQuery(PRIME, WINDOW),
    );

    expect(
      select({
        mode: 'raw',
        data: [],
      } as unknown as AllocationActivityEnvelope),
    ).toStrictEqual([]);
    // Coerced, but never silently: this is still a contract violation.
    expect(warn).toHaveBeenCalledOnce();
  });

  it('sorts aggregated activity buckets oldest first', () => {
    const select = selectOf<
      AllocationActivityEnvelope,
      { bucket_start: string }[]
    >(activitySeriesQuery(PRIME, WINDOW));

    const sorted = select({
      mode: 'aggregated',
      data: [
        { bucket_start: '2026-08-28T00:00:00Z' },
        { bucket_start: '2026-08-27T00:00:00Z' },
      ],
    } as unknown as AllocationActivityEnvelope);

    expect(sorted.map((bucket) => bucket.bucket_start)).toStrictEqual([
      '2026-08-27T00:00:00Z',
      '2026-08-28T00:00:00Z',
    ]);
  });
});

describe('toQueryErrorMessage', () => {
  const httpError = (status: number, body: unknown) =>
    new HttpRequestError({
      method: 'get',
      path: '/v1/primes/{prime_id}/allocations',
      body,
      response: new Response(null, { status, statusText: 'Not Found' }),
    });

  it('is null for a query that has not failed', () => {
    expect(toQueryErrorMessage(null)).toBeNull();
    expect(toQueryErrorMessage(undefined)).toBeNull();
  });

  it('keeps the status and the parsed body', () => {
    expect(
      toQueryErrorMessage(httpError(404, { detail: 'unknown prime' })),
    ).toBe(
      'GET /v1/primes/{prime_id}/allocations failed (404): {"detail":"unknown prime"}',
    );
  });

  it('reports a bodyless failure as such', () => {
    expect(toQueryErrorMessage(httpError(502, undefined))).toBe(
      'GET /v1/primes/{prime_id}/allocations failed (502): No response body.',
    );
  });

  it('passes a string body through', () => {
    expect(toQueryErrorMessage(httpError(500, 'upstream exploded'))).toBe(
      'GET /v1/primes/{prime_id}/allocations failed (500): upstream exploded',
    );
  });

  it('survives a body JSON cannot serialise', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;

    expect(toQueryErrorMessage(httpError(500, cyclic))).toBe(
      'GET /v1/primes/{prime_id}/allocations failed (500): Unserializable error body.',
    );
  });

  it('falls back to the message of a non-HTTP error', () => {
    expect(toQueryErrorMessage(new Error('network down'))).toBe('network down');
  });

  it('names a thrown non-Error rather than rendering nothing', () => {
    expect(toQueryErrorMessage('a bare string')).toBe(
      'Unknown request failure.',
    );
  });
});
