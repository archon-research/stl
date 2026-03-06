/**
 * watcher-burst.js
 *
 * Burst test: emits blocks at high frequency to stress the watcher's ingestion pipeline.
 *
 * Environment variables:
 *   ADMIN_URL        - Admin API base URL (default: http://mock-blockchain-server:8547)
 *   INTERVAL_MS      - Block emission interval ms (default: 50)
 *   DURATION         - Test duration (default: 30s)
 *   REORG            - Enable reorg injection (set to any value to enable)
 *   REORG_DEPTH      - Reorg depth (default: 5)
 *   REORG_INTERVAL_S - Seconds between reorgs (default: 30)
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import { createReorgRunner, reorgErrors } from './watcher-reorg.js';

const adminURL = __ENV.ADMIN_URL || 'http://mock-blockchain-server:8547';
const intervalMs = parseInt(__ENV.INTERVAL_MS || '50');
const reorg = createReorgRunner(adminURL);

const adminErrors = new Counter('admin_errors');

export const options = {
  duration: __ENV.DURATION || '30s',
  vus: 1,
  thresholds: {
    admin_errors: ['count<3'],
    ...(reorg.enabled && { reorg_errors: ['count<3'] }),
  },
};

export function setup() {
  let res = http.post(`${adminURL}/speed`, JSON.stringify({ interval_ms: intervalMs }), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { [`speed set to ${intervalMs}ms`]: (r) => r.status === 200 });

  res = http.post(`${adminURL}/start`);
  check(res, { 'replayer started': (r) => r.status === 200 });
}

export default function () {
  reorg.tick();

  const res = http.get(`${adminURL}/status`);
  const ok = check(res, { 'status 200': (r) => r.status === 200 });
  if (!ok) adminErrors.add(1);
  sleep(1);
}

export function teardown() {
  const res = http.post(`${adminURL}/stop`);
  const body = JSON.parse(res.body || '{}');
  console.log(`Burst complete — blocks emitted: ${body.last_block || 'unknown'}`);
}
