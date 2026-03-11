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
const intervalMs = parseInt(__ENV.INTERVAL_MS || '50', 10);
if (isNaN(intervalMs) || intervalMs <= 0) {
  throw new Error(`INTERVAL_MS must be a positive integer, got: ${__ENV.INTERVAL_MS}`);
}
const reorg = createReorgRunner(adminURL);

const adminErrors = new Counter('admin_errors');
const stalledPolls = new Counter('stalled_polls');

// Minimum blocks expected per 1 s poll at 50% of the configured rate.
// Only enforced for sub-second block intervals where each poll should see advancement.
const minDeltaPerPoll = intervalMs < 1000 ? Math.max(1, Math.floor(1000 / intervalMs * 0.5)) : 0;
// Maximum consecutive polls with zero advancement before flagging a stall.
// For fast intervals this is 2 s; for slow intervals it covers one full block period + 1 s.
const maxNoAdvancePolls = Math.max(2, Math.ceil(intervalMs / 1000) + 1);

export const options = {
  duration: __ENV.DURATION || '30s',
  vus: 1,
  thresholds: {
    admin_errors: ['count<3'],
    stalled_polls: ['count<3'],
    ...(reorg.enabled && { reorg_errors: ['count<3'] }),
  },
};

// VU-local state. Safe as module-level vars because vus: 1.
let prevEmitted = -1;
let pollCount = 0;
let consecutiveNoAdvance = 0;

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
  if (!check(res, { 'admin /status 200': (r) => r.status === 200 })) {
    adminErrors.add(1);
    sleep(1);
    return;
  }

  let body;
  try {
    body = JSON.parse(res.body);
  } catch (_) {
    adminErrors.add(1);
    sleep(1);
    return;
  }

  if (!check(body, { 'replayer running': (b) => b.running === true })) {
    adminErrors.add(1);
  }

  const emitted = body.blocks_emitted || 0;
  pollCount++;

  if (prevEmitted >= 0) {
    const delta = emitted - prevEmitted;

    if (delta > 0) {
      consecutiveNoAdvance = 0;
    } else {
      consecutiveNoAdvance++;
    }

    // Skip the first 2 polls: let the replayer warm up before asserting rates.
    if (pollCount > 2) {
      if (minDeltaPerPoll > 0) {
        // Sub-second interval: every poll should advance by at least minDeltaPerPoll.
        if (!check(null, { 'blocks advancing at target rate': () => delta >= minDeltaPerPoll })) {
          stalledPolls.add(1);
        }
      } else if (consecutiveNoAdvance > maxNoAdvancePolls) {
        // Slower interval: flag a stall only after the expected window is exceeded.
        stalledPolls.add(1);
        consecutiveNoAdvance = 0;
      }
    }
  }

  prevEmitted = emitted;
  sleep(1);
}

export function teardown() {
  const res = http.post(`${adminURL}/stop`);
  const body = JSON.parse(res.body || '{}');
  const totalEmitted = body.last_block || 0;
  const durationSecs = parseDurationSecs(__ENV.DURATION || '30s');
  const expectedMin = Math.floor(durationSecs / (intervalMs / 1000) * 0.8);
  check(null, { 'total throughput ≥ 80% of target': () => totalEmitted >= expectedMin });
  console.log(`Burst complete — emitted: ${totalEmitted}, expected ≥ ${expectedMin}`);
}

function parseDurationSecs(s) {
  const m = s.match(/^(\d+)([smh])$/);
  if (!m) return 30;
  const n = parseInt(m[1], 10);
  return m[2] === 'h' ? n * 3600 : m[2] === 'm' ? n * 60 : n;
}
