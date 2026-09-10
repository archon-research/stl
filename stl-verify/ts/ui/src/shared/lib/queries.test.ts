import { HttpRequestError } from '@archon-research/http-client-react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  AllocationActivityBucket,
  AllocationActivityEnvelope,
  ExposureEnvelope,
  PrimeDebtBucket,
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

/**
 * The same select, seen as the untyped payload the cache actually hands it. The
 * contract-violation cases below feed it what no envelope type can express.
 */
function wireSelectOf<TData, TSelected>(options: {
  select?: (data: TData) => TSelected;
}): (data: unknown) => TSelected {
  const select = selectOf(options);
  // The guard under test defends against arbitrary input, and no envelope type
  // can express a `data` that is not an array — so reaching it needs the cast.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  return select as (data: unknown) => TSelected;
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
  frequency: 'PT15M',
};

const PRIME = '0x1601843c5e9bc251a3272907010afa41fa18347e';

const ENVELOPE_WINDOW: PrimeDebtEnvelope['window'] = {
  from_timestamp: '2026-08-27T00:00:00.000Z',
  to_timestamp: '2026-08-28T00:00:00.000Z',
  frequency: 'PT15M',
  frequency_ms: 900_000,
};

/** The arm of `TEnvelope` that answers to `TMode`, as the fixtures name them. */
type Arm<TEnvelope extends { mode: string }, TMode> = Extract<
  TEnvelope,
  { mode: TMode }
>;

const rawDebtEnvelope = (
  data: Arm<PrimeDebtEnvelope, 'raw'>['data'],
): PrimeDebtEnvelope => ({
  mode: 'raw',
  data,
  source: 'indexed',
  window: ENVELOPE_WINDOW,
});

const aggregatedDebtEnvelope = (
  data: Arm<PrimeDebtEnvelope, 'aggregated'>['data'],
): PrimeDebtEnvelope => ({
  mode: 'aggregated',
  data,
  source: 'indexed',
  window: ENVELOPE_WINDOW,
});

const rawActivityEnvelope = (
  data: Arm<AllocationActivityEnvelope, 'raw'>['data'],
): AllocationActivityEnvelope => ({
  mode: 'raw',
  data,
  window: ENVELOPE_WINDOW,
});

const aggregatedActivityEnvelope = (
  data: Arm<AllocationActivityEnvelope, 'aggregated'>['data'],
): AllocationActivityEnvelope => ({
  mode: 'aggregated',
  data,
  window: ENVELOPE_WINDOW,
});

const debtBucket = (bucketStart: string, debtWad: string): PrimeDebtBucket => ({
  bucket_start: bucketStart,
  debt_wad: debtWad,
});

const activityBucket = (bucketStart: string): AllocationActivityBucket => ({
  bucket_start: bucketStart,
  event_count: 0,
  net_flow_usd: '0',
  total_tx_amount: '0',
});

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

  const token = (symbol: string | null): TokensResponse[number] => ({
    address: '0xdc035d45d973e3ec169d2276ddab16f1e407384f',
    chain_id: 1,
    id: 1,
    symbol,
    updated_at: '2026-08-27T00:00:00.000Z',
  });

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

describe('activitySeriesQuery params', () => {
  // usePrimeChartSeries reads balance_usd, not net_flow_usd -- the request has
  // to actually ask for it. `series` carries no other signal (`prime_id` and
  // the window are shared with the raw feed), so nothing else would catch it
  // if `series: 'balance'` were ever dropped from the query (VEC-760).
  it('asks the endpoint for the balance series, not the default flow one', () => {
    expect(keyInitOf(activitySeriesQuery(PRIME, WINDOW)).query).toMatchObject({
      series: 'balance',
    });
  });
});

describe('envelope payload policy', () => {
  // `data` is required and non-nullable on every envelope, so a missing one is
  // a contract violation — and a `select` that throws logs nowhere by itself.
  it('rejects an envelope whose data is not an array', () => {
    const error = vi
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    const select = wireSelectOf<PrimeDebtEnvelope, unknown>(
      debtSeriesQuery(PRIME, WINDOW),
    );

    expect(() => select({ mode: 'aggregated', data: null })).toThrow(
      /returned a non-array `data` for an aggregated request/,
    );
    expect(error).toHaveBeenCalledOnce();
  });

  // The aggregated activity branch narrows on `mode` before unwrapping, which
  // is exactly where it is tempting to trust the type and drop the guard.
  it('rejects it on the aggregated activity series too', () => {
    const error = vi
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    const select = wireSelectOf<AllocationActivityEnvelope, unknown>(
      activitySeriesQuery(PRIME, WINDOW),
    );

    expect(() => select({ mode: 'aggregated', data: null })).toThrow(
      /GET \/v1\/allocations\/activity returned a non-array `data`/,
    );
    expect(error).toHaveBeenCalledOnce();
  });

  it('rejects it on a single-mode series too', () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const select = wireSelectOf<ExposureEnvelope, unknown>(
      exposureSeriesQuery(PRIME, WINDOW),
    );

    expect(() => select({ mode: 'aggregated', data: null })).toThrow(
      /GET \/v1\/primes\/\{prime_id\}\/exposure returned a non-array/,
    );
  });
});

describe('envelope mode policy', () => {
  // The two series ask for buckets alike; only what they do with a disagreeing
  // answer differs, and that asymmetry is deliberate.
  it('rejects a raw envelope on the primary debt series', () => {
    const select = selectOf<PrimeDebtEnvelope, unknown>(
      debtSeriesQuery(PRIME, WINDOW),
    );

    expect(() => select(rawDebtEnvelope([]))).toThrow(
      /returned "raw" for an aggregated request/,
    );
  });

  it('coerces a raw envelope to no data on the supplementary activity series', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const select = selectOf<AllocationActivityEnvelope, unknown[]>(
      activitySeriesQuery(PRIME, WINDOW),
    );

    expect(select(rawActivityEnvelope([]))).toStrictEqual([]);
    // Coerced, but never silently: this is still a contract violation.
    expect(warn).toHaveBeenCalledOnce();
  });

  it('sorts aggregated debt buckets oldest first', () => {
    const select = selectOf<PrimeDebtEnvelope, { bucket_start: string }[]>(
      debtSeriesQuery(PRIME, WINDOW),
    );

    const sorted = select(
      aggregatedDebtEnvelope([
        debtBucket('2026-08-28T00:00:00Z', '2'),
        debtBucket('2026-08-27T00:00:00Z', '1'),
      ]),
    );

    expect(sorted.map((bucket) => bucket.bucket_start)).toStrictEqual([
      '2026-08-27T00:00:00Z',
      '2026-08-28T00:00:00Z',
    ]);
  });

  it('sorts aggregated activity buckets oldest first', () => {
    const select = selectOf<
      AllocationActivityEnvelope,
      { bucket_start: string }[]
    >(activitySeriesQuery(PRIME, WINDOW));

    const sorted = select(
      aggregatedActivityEnvelope([
        activityBucket('2026-08-28T00:00:00Z'),
        activityBucket('2026-08-27T00:00:00Z'),
      ]),
    );

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
