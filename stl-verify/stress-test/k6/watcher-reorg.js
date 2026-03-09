/**
 * reorg.js — shared reorg injection module.
 *
 * Usage:
 *   import { createReorgRunner } from './watcher-reorg.js';
 *   const reorg = createReorgRunner(adminURL);
 *
 *   export default function () {
 *     reorg.tick(); // triggers reorg when interval has elapsed
 *   }
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

export const reorgsTriggered = new Counter('reorgs_triggered');
export const reorgErrors = new Counter('reorg_errors');

/**
 * createReorgRunner returns a runner that periodically triggers a reorg.
 * Call tick() once per iteration; it fires when the interval has elapsed.
 *
 * Controlled by env vars (read at construction time):
 *   REORG           - enable reorg injection (any non-empty value)
 *   REORG_DEPTH     - reorg depth (default: 5)
 *   REORG_INTERVAL_S - seconds between reorgs (default: 30)
 */
export function createReorgRunner(adminURL) {
  const enabled = !!__ENV.REORG;
  const depth = parseInt(__ENV.REORG_DEPTH || '5', 10);
  if (isNaN(depth) || depth <= 0) {
    throw new Error(`REORG_DEPTH must be a positive integer, got: ${__ENV.REORG_DEPTH}`);
  }
  const intervalSeconds = parseInt(__ENV.REORG_INTERVAL_S || '30', 10);
  if (isNaN(intervalSeconds) || intervalSeconds <= 0) {
    throw new Error(`REORG_INTERVAL_S must be a positive integer, got: ${__ENV.REORG_INTERVAL_S}`);
  }
  let lastReorgAt = Date.now() / 1000; // start clock from test start, not epoch
  let count = 0;

  return {
    enabled,
    get count() { return count; },
    tick() {
      if (!enabled) return;
      const now = Date.now() / 1000;
      if (now - lastReorgAt < intervalSeconds) return;
      lastReorgAt = now;

      const res = http.post(`${adminURL}/reorg?depth=${depth}`);
      const ok = check(res, { 'reorg triggered': (r) => r.status === 200 });
      if (ok) {
        reorgsTriggered.add(1);
        count++;
        console.log(`Reorg triggered (depth=${depth})`);
      } else {
        reorgErrors.add(1);
        console.error(`Reorg failed: ${res.status} ${res.body}`);
      }
    },
  };
}
