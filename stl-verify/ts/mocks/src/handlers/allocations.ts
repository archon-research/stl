/**
 * The allocation table and the activity feed behind it.
 *
 * `/v1/allocations/activity` is the one endpoint the UI drives with a real filter
 * set, so every filter it sends is honoured — in both modes. `protocol_name` and
 * `token_symbol` are case-insensitive substrings and `action_type` is
 * case-insensitive equality, matching the `LIKE`/`LOWER()` the repository uses.
 *
 * The `aggregate=true` envelope is a different row shape, not a variant of the
 * same one, and `ui/src/lib/api.ts` throws when it gets the wrong `mode` — so
 * the mock has to get the mode right or the app fails loudly, which is the point.
 */
import { mockDelay } from '@archon-research/http-client-msw';
import type { MockHandler } from '@archon-research/http-client-msw';

import { iso, mockNow } from '../clock.ts';
import {
  receiptTokenUsdPerUnit,
  seedActivity,
  seedAllocations,
  seedCompositeAllocations,
  seedReferenceAllocations,
} from '../fixtures/allocations.ts';
import { PRIMES } from '../fixtures/registry.ts';
import { decimalString, usdString } from '../fixtures/series.ts';
import { LIST_DELAY_MS, SERIES_DELAY_MS, mock } from '../mock-api.ts';
import { notFound, problemResponse } from '../problem.ts';
import {
  bucketStarts,
  equalsInsensitive,
  includesInsensitive,
  readChainId,
  readFlag,
  readProvenance,
  readLimit,
  resolveWindow,
  sameHex,
} from '../query.ts';
import type {
  AllocationActivity,
  AllocationActivityBucket,
} from '../schema.ts';

const ACTIVITY_LIMIT_DEFAULT = 100;
const ACTIVITY_LIMIT_MAX = 1000;

type ActivityFilters = {
  primeId: string | null;
  chainId: number | null;
  protocolName: string | null;
  actionType: string | null;
  tokenSymbol: string | null;
  txHash: string | null;
};

function matchesFilters(
  row: AllocationActivity,
  filters: ActivityFilters,
): boolean {
  return (
    (filters.primeId === null || sameHex(row.prime_address, filters.primeId)) &&
    (filters.chainId === null || row.chain_id === filters.chainId) &&
    (filters.protocolName === null ||
      includesInsensitive(row.protocol_name, filters.protocolName)) &&
    (filters.actionType === null ||
      equalsInsensitive(row.action_type, filters.actionType)) &&
    (filters.tokenSymbol === null ||
      includesInsensitive(row.token_symbol, filters.tokenSymbol)) &&
    (filters.txHash === null || sameHex(row.tx_hash, filters.txHash))
  );
}

function withinWindow(
  row: AllocationActivity,
  fromMs: number,
  toMs: number,
): boolean {
  const createdMs = Date.parse(row.created_at);
  return createdMs >= fromMs && createdMs <= toMs;
}

/**
 * Counted from the same rows the raw feed serves, not generated.
 *
 * Generating them was the alternative, and it produced an aggregate whose event
 * count and tx-amount sums were ~80x the raw feed's — a screen that toggles
 * aggregation would show a chart and a table that cannot both be true. The
 * fixture is sparser than staging as a result, which is the right trade: one
 * source means the filters, the window, and the totals agree in both modes by
 * construction.
 */
function activityBuckets(
  rows: readonly AllocationActivity[],
  bucketStartsMs: readonly number[],
  intervalMs: number,
  usdPerUnit: ReadonlyMap<number, number>,
): AllocationActivityBucket[] {
  // The callback's return annotation is what makes the literal fresh; the
  // function's own `AllocationActivityBucket[]` is not enough, because a
  // `.map()` result is checked for assignability rather than for excess keys.
  return bucketStartsMs.map((startMs): AllocationActivityBucket => {
    const inBucket = rows.filter((row) => {
      const createdMs = Date.parse(row.created_at);
      return createdMs >= startMs && createdMs < startMs + intervalMs;
    });

    return {
      bucket_start: iso(startMs),
      event_count: inBucket.length,
      total_tx_amount: decimalString(sumBy(inBucket, grossAmount)),
      net_flow_usd: usdString(
        sumBy(inBucket, (row) => signedFlowUsd(row, usdPerUnit)),
      ),
    };
  });
}

function sumBy(
  rows: readonly AllocationActivity[],
  amount: (row: AllocationActivity) => number,
): number {
  return rows.reduce((total, row) => total + amount(row), 0);
}

/**
 * Token units, unvalued and unsigned, summed across whatever denominations the
 * bucket happens to hold — which is what the API means by the field:
 * `SUM(ap.tx_amount)` over the same rows, clamped at zero. Not a USD figure, and
 * not comparable across buckets; `net_flow_usd` is the one that is.
 */
function grossAmount(row: AllocationActivity): number {
  return Number(row.tx_amount);
}

/**
 * The flow in USD: the row's amount at its receipt token's USD-per-unit, signed
 * by direction. A token with no priced position contributes nothing, which is
 * both the endpoint's rule for an unpriced flow and its rule for a directly-held
 * underlying. A sweep is a rebalance, so it moves no capital in or out.
 */
function signedFlowUsd(
  row: AllocationActivity,
  usdPerUnit: ReadonlyMap<number, number>,
): number {
  const priceUsd = usdPerUnit.get(row.token_id);
  if (priceUsd === undefined) return 0;
  if (row.action_type === 'in') return grossAmount(row) * priceUsd;
  if (row.action_type === 'out') return -grossAmount(row) * priceUsd;
  return 0;
}

export function allocationHandlers(): MockHandler[] {
  return [
    mock.get(
      '/v1/primes/{prime_id}/allocations',
      async ({ params, query, response }) => {
        await mockDelay(LIST_DELAY_MS);
        const nowMs = mockNow();
        const source = readProvenance(
          query.get('source'),
          query.get('reference'),
        );
        if (!source.ok) {
          return response.untyped(problemResponse(source.problem));
        }
        const proxy = PRIMES.find((prime) =>
          sameHex(prime.address, params.prime_id),
        );

        // Existence first, so "this proxy holds nothing" (a real fixture: spark
        // on base) stays distinguishable from "this address is not a prime".
        if (proxy === undefined) {
          return response.untyped(
            problemResponse(notFound(`Prime not found: ${params.prime_id}`)),
          );
        }

        if (source.value === 'reference') {
          const referenceRows = seedReferenceAllocations(nowMs, proxy.name);
          return referenceRows === undefined
            ? response.untyped(
                problemResponse(
                  notFound(`Star monitor does not track prime ${proxy.name}`),
                ),
              )
            : response(200).json(referenceRows);
        }

        if (source.value === 'both') {
          return response(200).json(
            seedCompositeAllocations(nowMs, proxy.name, proxy.address),
          );
        }

        const rows = seedAllocations(nowMs)[proxy.address] ?? [];
        return response(200).json([...rows]);
      },
    ),

    mock.get('/v1/allocations/activity', async ({ query, response }) => {
      await mockDelay(SERIES_DELAY_MS);
      const nowMs = mockNow();
      const resolved = resolveWindow(
        {
          fromTimestamp: query.get('from_timestamp'),
          toTimestamp: query.get('to_timestamp'),
          resolution: query.get('resolution'),
        },
        nowMs,
      );
      if (!resolved.ok) {
        return response.untyped(problemResponse(resolved.problem));
      }
      const limit = readLimit(
        query.get('limit'),
        ACTIVITY_LIMIT_DEFAULT,
        ACTIVITY_LIMIT_MAX,
      );
      if (!limit.ok) {
        return response.untyped(problemResponse(limit.problem));
      }
      const chainId = readChainId(query.get('chain_id'));
      if (!chainId.ok) {
        return response.untyped(problemResponse(chainId.problem));
      }
      const aggregate = readFlag('aggregate', query.get('aggregate'));
      if (!aggregate.ok) {
        return response.untyped(problemResponse(aggregate.problem));
      }

      const { window, fromMs, toMs } = resolved.value;
      const filters: ActivityFilters = {
        primeId: query.get('prime_id'),
        chainId: chainId.value,
        protocolName: query.get('protocol_name'),
        actionType: query.get('action_type'),
        tokenSymbol: query.get('token_symbol'),
        txHash: query.get('tx_hash'),
      };
      const matched = seedActivity(nowMs)
        .filter((row) => matchesFilters(row, filters))
        .filter((row) => withinWindow(row, fromMs, toMs));

      if (aggregate.value) {
        return response(200).json({
          mode: 'aggregated',
          window,
          data: activityBuckets(
            matched,
            bucketStarts(fromMs, toMs, window.interval_ms, limit.value),
            window.interval_ms,
            receiptTokenUsdPerUnit(nowMs),
          ),
        });
      }

      return response(200).json({
        mode: 'raw',
        window,
        data: matched.slice(0, limit.value),
      });
    }),
  ];
}
