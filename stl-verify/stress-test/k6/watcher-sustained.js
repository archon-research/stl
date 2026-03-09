/**
 * watcher-sustained.js
 *
 * Sustained load test: runs the mock server at a configurable block interval,
 * measuring throughput and error rate.
 *
 * Environment variables:
 *   ADMIN_URL        - Admin API base URL (default: http://mock-blockchain-server:8547)
 *   INTERVAL_MS      - Block emission interval ms (default: 12000 ~Ethereum)
 *                      Examples: 250 for Base, 2000 for BNB Chain
 *   DURATION         - Test duration (default: 2m)
 *   REORG            - Enable reorg injection (set to any value to enable)
 *   REORG_DEPTH      - Reorg depth (default: 5)
 *   REORG_INTERVAL_S - Seconds between reorgs (default: 30)
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { createReorgRunner, reorgErrors } from './watcher-reorg.js';

const adminURL = __ENV.ADMIN_URL || 'http://mock-blockchain-server:8547';
const intervalMs = parseInt(__ENV.INTERVAL_MS || '12000', 10);
if (isNaN(intervalMs) || intervalMs <= 0) {
  throw new Error(`INTERVAL_MS must be a positive integer, got: ${__ENV.INTERVAL_MS}`);
}
const reorg = createReorgRunner(adminURL);

const adminErrors = new Counter('admin_errors');
const blockEmissionRate = new Rate('block_emission_ok');

export const options = {
  duration: __ENV.DURATION || '2m',
  vus: 1,
  thresholds: {
    admin_errors: ['count<5'],
    ...(reorg.enabled && { reorg_errors: ['count<3'] }),
  },
};

export function setup() {
  let res = http.post(`${adminURL}/speed`, JSON.stringify({ interval_ms: intervalMs }), {
    headers: { 'Content-Type': 'application/json' },
  });
  if (res.status !== 200) {
    throw new Error(`setup: /speed failed (${res.status}): ${res.body}`);
  }

  res = http.post(`${adminURL}/start`);
  if (res.status !== 200) {
    throw new Error(`setup: /start failed (${res.status}): ${res.body}`);
  }
}

export default function () {
  reorg.tick();

  const res = http.get(`${adminURL}/status`);
  const ok = check(res, {
    'status 200': (r) => r.status === 200,
    'running': (r) => {
      try {
        return JSON.parse(r.body).running === true;
      } catch (_) {
        return false;
      }
    },
  });
  if (!ok) adminErrors.add(1);
  blockEmissionRate.add(ok ? 1 : 0);
  sleep(1);
}

export function teardown() {
  const res = http.post(`${adminURL}/stop`);
  const body = JSON.parse(res.body || '{}');
  console.log(`Blocks emitted: ${body.last_block || 'unknown'}`);
  if (reorg.enabled) {
    console.log(`Reorgs triggered: ${reorg.count}`);
  }
}
