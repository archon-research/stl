/**
 * The bucketed value series: exposure, total capital, prime debt.
 *
 * These are the fixtures **not** stored as literal rows. Staging's captures were
 * 96–97 buckets each, and a literal copy would answer every window and
 * `resolution` with the same 96 buckets — so `resolution=PT1H` would silently
 * return 5-minute data and the UI's range switcher would look like it worked
 * while returning nonsense. Instead each series keeps the two real endpoints of
 * its 24h capture and interpolates the requested grid between them.
 *
 * A bucket's value is a function of **its own instant**, never of its position in
 * the response. Keying off the index would make the same `bucket_start` return a
 * different number at a different `limit`, so paging a chart would redraw it.
 */
import { createSeededRng } from '@archon-research/http-client-msw';

import { DAY_MS, MINUTE_MS, isoAgo } from '../clock.ts';
import type { PrimeDebtSnapshot } from '../schema.ts';

/** The 24h endpoints of a staging capture. */
type SeriesEnvelope = {
  /** The value at the newest bucket. */
  newest: number;
  /** The value 24h older, which the interpolation walks back to. */
  oldest: number;
  /** Fraction of the interpolated value the jitter may move it by. */
  jitter: number;
  seed: number;
};

export const EXPOSURE_USD: SeriesEnvelope = {
  newest: 1656538061.841276,
  oldest: 1633229570.352857,
  jitter: 0.004,
  seed: 20260818,
};

export const TOTAL_CAPITAL_USD: SeriesEnvelope = {
  newest: 48142491.085806,
  oldest: 45236509.249709,
  jitter: 0.006,
  seed: 20260819,
};

/** In USDS, not wad — the wad conversion happens at serialization. */
export const PRIME_DEBT_USDS: SeriesEnvelope = {
  newest: 2625347459.496046,
  oldest: 2644576602.993248,
  jitter: 0.002,
  seed: 20260820,
};

/**
 * A bucket start paired with its value, so no caller indexes one array by the
 * other's position. A length mismatch would otherwise surface as
 * `exposure_usd: "0"`, which the contract reserves for a real zero — it encodes
 * an unobserved bucket as `null`.
 */
export type SeriesPoint = { startMs: number; value: number };

/**
 * Values for the given bucket starts, which must be newest-first on a fixed
 * grid. The newest bucket is exactly `envelope.newest` — it is the figure the
 * summary tiles read, and a jittered "current" number would disagree with the
 * risk-capital fixture it came from.
 */
export function seriesPoints(
  envelope: SeriesEnvelope,
  bucketStartsMs: readonly number[],
): SeriesPoint[] {
  const newestStartMs = bucketStartsMs[0];
  if (newestStartMs === undefined) {
    return [];
  }

  return bucketStartsMs.map((startMs) => ({
    startMs,
    value: valueAt(envelope, newestStartMs, startMs),
  }));
}

function valueAt(
  envelope: SeriesEnvelope,
  newestStartMs: number,
  startMs: number,
): number {
  const progress = Math.min((newestStartMs - startMs) / DAY_MS, 1);
  if (progress <= 0) {
    return envelope.newest;
  }

  const trend =
    envelope.newest + (envelope.oldest - envelope.newest) * progress;
  // Seeded from the bucket's own instant, so the value is stable across
  // requests that ask for different windows or page sizes.
  const rng = createSeededRng(envelope.seed ^ Math.floor(startMs / MINUTE_MS));
  return trend * (1 + (rng.next() - 0.5) * 2 * envelope.jitter);
}

/**
 * USD decimals, at the six places a double can carry without inventing digits.
 * The API sends 18; padding rather than printing float noise is the honest
 * translation.
 */
export function usdString(value: number): string {
  return `${value.toFixed(6)}000000000000`;
}

/** USDS → `wad`, as an integer string. `debt_wad` is never fractional. */
export function toWad(usds: number): string {
  return (BigInt(Math.round(usds * 1e6)) * 10n ** 12n).toString();
}

const DEBT_SYNC_AGO = 5 * MINUTE_MS;
const DEBT_SYNC_INTERVAL = 15 * MINUTE_MS;
const DEBT_SNAPSHOT_COUNT = 8;

/**
 * The raw debt snapshots behind the same series. Eight of them, one per
 * ~15-minute sync, because `limit=1` — how the UI reads "current debt" — is only
 * a meaningful assertion when there is more than one row to not return.
 */
export function seedDebtSnapshots(
  nowMs: number,
  primeAddress: string,
  primeName: string,
  ilkName: string,
): PrimeDebtSnapshot[] {
  return Array.from({ length: DEBT_SNAPSHOT_COUNT }, (_, index) => {
    const syncedAgo = DEBT_SYNC_AGO + index * DEBT_SYNC_INTERVAL;
    const syncedMs = nowMs - syncedAgo;

    return {
      prime_address: primeAddress,
      prime_name: primeName,
      ilk_name: ilkName,
      debt_wad: toWad(valueAt(PRIME_DEBT_USDS, nowMs, syncedMs)),
      block_number: 25780913 - index * 74,
      block_version: 0,
      synced_at: isoAgo(nowMs, syncedAgo),
    };
  });
}
