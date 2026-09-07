/**
 * The three per-prime time series: exposure, total capital, debt.
 *
 * Exposure and total capital are aggregate-only by contract (`mode` is the
 * constant `'aggregated'`). Debt has both modes, and a reference provenance is
 * aggregate-only there because upstream publishes one figure per prime per day
 * with no ilk or block identity, so the API answers `400` rather than inventing
 * them.
 */
import { mockDelay } from '@archon-research/http-client-msw';
import type { MockHandler } from '@archon-research/http-client-msw';

import { DAY_MS, MINUTE_MS, iso, mockNow } from '../clock.ts';
import { PRIMES } from '../fixtures/registry.ts';
import type { PrimeName, SeededPrime } from '../fixtures/registry.ts';
import {
  PRIME_COLLATERAL_USD,
  PRIME_MONITOR_ENCUMBRANCE,
} from '../fixtures/risk.ts';
import {
  EXPOSURE_USD,
  PRIME_DEBT_USDS,
  TOTAL_CAPITAL_USD,
  seedDebtSnapshots,
  seriesPoints,
  toWad,
  usdString,
} from '../fixtures/series.ts';
import { SERIES_DELAY_MS, mock } from '../mock-api.ts';
import type { Parsed, Problem } from '../problem.ts';
import { badRequest, notFound, problemResponse } from '../problem.ts';
import {
  bucketStarts,
  readFlag,
  readLimit,
  readProvenance,
  resolveWindow,
  sameHex,
} from '../query.ts';
import type {
  ExposureBucket,
  PrimeDebtBucket,
  Provenance,
  TimeSeriesWindow,
  TotalCapitalBucket,
} from '../schema.ts';

const BUCKET_LIMIT_DEFAULT = 100;
const BUCKET_LIMIT_MAX = 500;

/**
 * Total by construction, so adding a prime is a compile error rather than a
 * fabricated Maker ilk name — which is an on-chain identifier the fixture has no
 * authority to invent.
 */
const ILK_BY_PRIME: Readonly<Record<PrimeName, string>> = {
  spark: 'ALLOCATOR-SPARK-A',
  grove: 'ALLOCATOR-BLOOM-A',
};

/**
 * Exposure and total-capital take a proxy address; only debt resolves a vault
 * address too. Accepting a vault everywhere would answer a lookup the real API
 * 404s.
 */
function findProxy(primeId: string): SeededPrime | undefined {
  return PRIMES.find((prime) => sameHex(prime.address, primeId));
}

function findPrimeOrProxy(primeId: string): SeededPrime | undefined {
  return (
    findProxy(primeId) ??
    PRIMES.find((prime) => sameHex(prime.prime_vault_address, primeId))
  );
}

type SeriesRequest = {
  window: TimeSeriesWindow;
  starts: number[];
  limit: number;
  source: Provenance;
};

type SeriesQuery = {
  fromTimestamp: string | null;
  toTimestamp: string | null;
  resolution: string | null;
  limit: string | null;
  source: string | null;
  reference: string | null;
};

/**
 * Takes the raw strings rather than the resolver's `query` object: the three
 * endpoints declare the same five params but as three separate generated types,
 * and a helper typed against one would not accept the others.
 */
function readSeriesRequest(
  raw: SeriesQuery,
  nowMs: number,
): Parsed<SeriesRequest> {
  const resolved = resolveWindow(raw, nowMs);
  if (!resolved.ok) return resolved;
  const limit = readLimit(raw.limit, BUCKET_LIMIT_DEFAULT, BUCKET_LIMIT_MAX);
  if (!limit.ok) return limit;
  const source = readProvenance(raw.source, raw.reference);
  if (!source.ok) return source;

  const { window, fromMs, toMs } = resolved.value;
  return {
    ok: true,
    value: {
      window,
      limit: limit.value,
      starts: bucketStarts(fromMs, toMs, window.interval_ms, limit.value),
      source: source.value,
    },
  };
}

/**
 * The three endpoints declare the same five params, so one structural reader
 * serves all three — and it stays typed, which a cast to `string` would not: a
 * param the document drops stops satisfying this type.
 */
type SeriesQueryReader = {
  get: (
    name:
      | 'from_timestamp'
      | 'to_timestamp'
      | 'resolution'
      | 'limit'
      | 'source'
      | 'reference',
  ) => string | null;
};

function seriesQuery(query: SeriesQueryReader): SeriesQuery {
  return {
    fromTimestamp: query.get('from_timestamp'),
    toTimestamp: query.get('to_timestamp'),
    resolution: query.get('resolution'),
    limit: query.get('limit'),
    source: query.get('source'),
    reference: query.get('reference'),
  };
}

function unknownPrime(primeId: string): Problem {
  return notFound(`Prime not found: ${primeId}`);
}

export function seriesHandlers(): MockHandler[] {
  return [
    mock.get(
      '/v1/primes/{prime_id}/exposure',
      async ({ params, query, response }) => {
        await mockDelay(SERIES_DELAY_MS);
        const nowMs = mockNow();
        if (findProxy(params.prime_id) === undefined) {
          return response.untyped(
            problemResponse(unknownPrime(params.prime_id)),
          );
        }
        const request = readSeriesRequest(seriesQuery(query), nowMs);
        if (!request.ok) {
          return response.untyped(problemResponse(request.problem));
        }

        return response(200).json({
          mode: 'aggregated',
          source: request.value.source,
          window: request.value.window,
          // The return annotation is load-bearing, not decoration: a `.map()`
          // result is not a fresh object literal, so without it a bucket field
          // the document drops stays assignable and the mock keeps serving a
          // key the API no longer has. Annotated, that drop is a compile error.
          data: seriesPoints(EXPOSURE_USD, request.value.starts, nowMs).map(
            (point): ExposureBucket => ({
              bucket_start: iso(point.startMs),
              exposure_usd: usdString(point.value),
            }),
          ),
        });
      },
    ),

    mock.get(
      '/v1/primes/{prime_id}/total-capital',
      async ({ params, query, response }) => {
        await mockDelay(SERIES_DELAY_MS);
        const nowMs = mockNow();
        if (findProxy(params.prime_id) === undefined) {
          return response.untyped(
            problemResponse(unknownPrime(params.prime_id)),
          );
        }
        const request = readSeriesRequest(seriesQuery(query), nowMs);
        if (!request.ok) {
          return response.untyped(problemResponse(request.problem));
        }

        const proxy = findProxy(params.prime_id);
        // Wherever the response holds Sky's figures the buckets also carry the
        // upstream collateral and the monitor's ratio; self mode reports null.
        const monitorFields =
          request.value.source === 'indexed' || proxy === undefined
            ? {}
            : {
                assets_usd: PRIME_COLLATERAL_USD[proxy.name],
                assets_observed_at: iso(nowMs - DAY_MS),
                encumbrance_ratio: PRIME_MONITOR_ENCUMBRANCE[proxy.name],
                capital_observed_at: iso(nowMs - 11 * MINUTE_MS),
              };
        return response(200).json({
          mode: 'aggregated',
          source: request.value.source,
          window: request.value.window,
          data: seriesPoints(
            TOTAL_CAPITAL_USD,
            request.value.starts,
            nowMs,
          ).map((point): TotalCapitalBucket => ({
            bucket_start: iso(point.startMs),
            total_capital_usd: usdString(point.value),
            ...monitorFields,
          })),
        });
      },
    ),

    mock.get(
      '/v1/primes/{prime_id}/debt',
      async ({ params, query, response }) => {
        await mockDelay(SERIES_DELAY_MS);
        const nowMs = mockNow();
        const prime = findPrimeOrProxy(params.prime_id);
        if (prime === undefined) {
          return response.untyped(
            problemResponse(unknownPrime(params.prime_id)),
          );
        }
        const request = readSeriesRequest(seriesQuery(query), nowMs);
        if (!request.ok) {
          return response.untyped(problemResponse(request.problem));
        }

        const { window, starts, limit, source } = request.value;
        const parsedAggregate = readFlag('aggregate', query.get('aggregate'));
        if (!parsedAggregate.ok) {
          return response.untyped(problemResponse(parsedAggregate.problem));
        }
        const aggregate = parsedAggregate.value;

        if (source !== 'indexed' && !aggregate) {
          return response.untyped(
            problemResponse(
              badRequest('reference debt requires aggregate=true'),
            ),
          );
        }

        if (aggregate) {
          return response(200).json({
            mode: 'aggregated',
            source,
            window,
            data: seriesPoints(PRIME_DEBT_USDS, starts, nowMs).map(
              (point): PrimeDebtBucket => ({
                bucket_start: iso(point.startMs),
                debt_wad: toWad(point.value),
              }),
            ),
          });
        }

        return response(200).json({
          mode: 'raw',
          source: 'indexed',
          window,
          data: seedDebtSnapshots(
            nowMs,
            prime.prime_vault_address,
            prime.name,
            ILK_BY_PRIME[prime.name],
          ).slice(0, limit),
        });
      },
    ),
  ];
}
