/**
 * Fixture timestamps are **offsets**, never instants.
 *
 * Frozen as literal timestamps they would fall out of every endpoint's default
 * 24h window the day after capture, and the dashboard would render an empty
 * chart against a fixture that looks full. Each row stores how long ago it
 * happened and a handler re-bases that onto the clock it reads per request.
 *
 * The clock is read inside handlers, never at module scope, so importing the
 * mocks has no time-dependent side effect and a suite that freezes the clock
 * gets byte-identical bodies.
 */

/** When the fixture bodies were captured from staging. */
export const FIXTURE_ANCHOR_ISO = '2026-08-18T09:00:00.000Z';

export const SECOND_MS = 1000;
export const MINUTE_MS = 60 * SECOND_MS;
export const HOUR_MS = 60 * MINUTE_MS;
export const DAY_MS = 24 * HOUR_MS;

/**
 * How stale Sky's figures are, inside the reference indexer's 15m cadence.
 *
 * The API serves STL's record of Sky rather than a live read, so a reference
 * figure carries the cycle it was observed at. Non-zero on purpose: a fixture
 * stamped `now` would never exercise the staleness the stamp exists to show.
 */
export const REFERENCE_SYNCED_AGO_MS = 11 * MINUTE_MS;

/** The instant a handler renders its fixtures against. */
export function mockNow(): number {
  return Date.now();
}

export function iso(ms: number): string {
  return new Date(ms).toISOString();
}

export function isoAgo(nowMs: number, agoMs: number): string {
  return iso(nowMs - agoMs);
}

/**
 * The API stamps most rows `+00:00` rather than `Z`. Kept because the UI parses
 * these strings and a fixture that only ever emits `Z` would not exercise the
 * offset form the real API sends.
 */
export function offsetIsoAgo(nowMs: number, agoMs: number): string {
  return isoAgo(nowMs, agoMs).replace(/Z$/u, '+00:00');
}

/** Buckets land on the resolution grid, as the real time-bucketed reads do. */
export function floorToInterval(ms: number, intervalMs: number): number {
  return Math.floor(ms / intervalMs) * intervalMs;
}

/** How coarsely the generated series track the clock; see `seriesAnchorMs`. */
const SERIES_ANCHOR_INTERVAL_MS = 15 * MINUTE_MS;

/**
 * The instant the generated value series are measured from.
 *
 * It must not come from the response: anchoring on the newest bucket made the
 * same `bucket_start` answer differently once the window or the resolution
 * changed, and made a raw debt snapshot disagree with the bucket covering it. A
 * frozen epoch would fix that by drifting the whole series away from the current
 * figures the summary tiles read, so the anchor is the clock quantised to a
 * coarse grid: independent of what was asked for, and identical across the
 * requests one screen makes.
 */
export function seriesAnchorMs(nowMs: number): number {
  return floorToInterval(nowMs, SERIES_ANCHOR_INTERVAL_MS);
}
