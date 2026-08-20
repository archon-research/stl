/**
 * Decoded protocol events: the filtered feed and the per-transaction lookup.
 *
 * `protocol_name` is exact here, unlike the activity feed's substring match —
 * the two repositories really do differ (`p.name = :protocol_name` against a
 * `LIKE '%…%'`), and honouring a filter differently is as misleading as ignoring
 * it.
 */
import { mockDelay } from '@archon-research/http-client-msw';
import type { MockHandler } from '@archon-research/http-client-msw';

import { iso, mockNow } from '../clock.ts';
import { seedProtocolEvents } from '../fixtures/events.ts';
import { LIST_DELAY_MS, mock } from '../mock-api.ts';
import { problemResponse } from '../problem.ts';
import {
  bucketStarts,
  readFlag,
  readLimit,
  resolveWindow,
  sameHex,
} from '../query.ts';
import type { ProtocolEvent } from '../schema.ts';

const EVENT_LIMIT_DEFAULT = 100;
const EVENT_LIMIT_MAX = 500;

function countInBucket(
  events: readonly ProtocolEvent[],
  startMs: number,
  intervalMs: number,
): number {
  return events.filter((event) => {
    const createdMs = Date.parse(event.created_at);
    return createdMs >= startMs && createdMs < startMs + intervalMs;
  }).length;
}

export function eventHandlers(): MockHandler[] {
  return [
    mock.get('/v1/protocol-events', async ({ query, response }) => {
      await mockDelay(LIST_DELAY_MS);
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
        EVENT_LIMIT_DEFAULT,
        EVENT_LIMIT_MAX,
      );
      if (!limit.ok) {
        return response.untyped(problemResponse(limit.problem));
      }

      const { window, fromMs, toMs } = resolved.value;
      const txHash = query.get('tx_hash');
      const protocolName = query.get('protocol_name');
      const matched = seedProtocolEvents(nowMs)
        .filter((event) => txHash === null || sameHex(event.tx_hash, txHash))
        .filter(
          (event) =>
            protocolName === null || event.protocol_name === protocolName,
        )
        .filter((event) => {
          const createdMs = Date.parse(event.created_at);
          return createdMs >= fromMs && createdMs <= toMs;
        });

      if (readFlag(query.get('aggregate'))) {
        return response(200).json({
          mode: 'aggregated',
          window,
          data: bucketStarts(fromMs, toMs, window.interval_ms, limit.value).map(
            (startMs) => ({
              bucket_start: iso(startMs),
              event_count: countInBucket(matched, startMs, window.interval_ms),
            }),
          ),
        });
      }

      return response(200).json({
        mode: 'raw',
        window,
        data: matched.slice(0, limit.value),
      });
    }),

    // An unknown hash is an empty list, not a 404: the endpoint documents that
    // a transaction which emitted no tracked event is indistinguishable from one
    // that does not exist, so it reports neither.
    mock.get('/v1/tx/{tx_hash}/events', ({ params, response }) =>
      response(200).json(
        seedProtocolEvents(mockNow()).filter((event) =>
          sameHex(event.tx_hash, params.tx_hash),
        ),
      ),
    ),
  ];
}
