/**
 * Query-param readers shared by the handlers.
 *
 * `openapi-msw` types `query.get()` off the generated document, and the document
 * stringifies every query value — so a handler always reads `string | null` and
 * does its own coercion. These are that coercion, in one place, so `limit` means
 * the same thing on all six endpoints that take it.
 *
 * They reject what the API rejects rather than substituting a default. A mock
 * that quietly accepts `limit=abc` or an inverted window teaches the app that
 * those requests work, and the 422 only arrives in staging. The rules are ported
 * from `python/app/domain/time_series.py`; when that file changes, this one is
 * wrong until it is changed too.
 */
import { DAY_MS, HOUR_MS, MINUTE_MS, floorToInterval, iso } from './clock.ts';
import type { Parsed } from './problem.ts';
import { invalidQueryParam, unprocessable } from './problem.ts';
import type { TimeSeriesResolution, TimeSeriesWindow } from './schema.ts';

const RESOLUTION_INTERVAL_MS: Record<TimeSeriesResolution, number> = {
  PT1M: MINUTE_MS,
  PT5M: 5 * MINUTE_MS,
  PT15M: 15 * MINUTE_MS,
  PT1H: HOUR_MS,
  PT6H: 6 * HOUR_MS,
  P1D: DAY_MS,
};

const RESOLUTIONS = Object.keys(
  RESOLUTION_INTERVAL_MS,
) as TimeSeriesResolution[];

const DEFAULT_WINDOW_MS = DAY_MS;
const MAX_WINDOW_MS = 366 * DAY_MS;

/**
 * The spellings pydantic parses a `bool` query param from, in either case.
 * Sets, not a record, so an inherited key cannot answer as a spelling.
 */
const TRUE_WORDS = new Set(['1', 't', 'true', 'y', 'yes', 'on']);
const FALSE_WORDS = new Set(['0', 'f', 'false', 'n', 'no', 'off']);

/**
 * Absence is the param's default, which is `false` on every flag the app sends.
 * Anything outside the two vocabularies is a `422`, not a quiet `false`: the
 * mock that reads `aggregate=maybe` as off serves the raw envelope to a screen
 * asking for buckets, which is a shape mismatch the app would then blame on
 * itself.
 */
export function readFlag(name: string, raw: string | null): Parsed<boolean> {
  if (raw === null) {
    return { ok: true, value: false };
  }
  const word = raw.toLowerCase();
  if (TRUE_WORDS.has(word)) {
    return { ok: true, value: true };
  }
  if (FALSE_WORDS.has(word)) {
    return { ok: true, value: false };
  }
  return {
    ok: false,
    problem: invalidQueryParam(
      name,
      'Input should be a valid boolean, unable to interpret input',
    ),
  };
}

const DECIMAL_INTEGER = /^\d+$/u;

/**
 * Absent means the default; present-but-not-an-integer-in-range is the client's
 * mistake. Notably `limit` is clamped nowhere: the API answers `422` rather than
 * silently handing back a smaller page than was asked for.
 */
export function readLimit(
  raw: string | null,
  fallback: number,
  max: number,
): Parsed<number> {
  if (raw === null) {
    return { ok: true, value: fallback };
  }
  if (!DECIMAL_INTEGER.test(raw)) {
    return {
      ok: false,
      problem: invalidQueryParam('limit', 'value is not a valid integer'),
    };
  }
  const value = Number(raw);
  if (value < 1 || value > max) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'limit',
        `ensure this value is between 1 and ${max}`,
      ),
    };
  }
  return { ok: true, value };
}

/**
 * `null` is "no filter". Chain 0 is a real value in this fixture set — the
 * Anchorage custody leg lives there — so an empty string cannot be coerced to it.
 */
export function readChainId(raw: string | null): Parsed<number | null> {
  if (raw === null) {
    return { ok: true, value: null };
  }
  if (!DECIMAL_INTEGER.test(raw)) {
    return {
      ok: false,
      problem: invalidQueryParam('chain_id', 'value is not a valid integer'),
    };
  }
  return { ok: true, value: Number(raw) };
}

function readResolution(
  raw: string | null,
): Parsed<TimeSeriesResolution | null> {
  if (raw === null) {
    return { ok: true, value: null };
  }
  const match = RESOLUTIONS.find((candidate) => candidate === raw);
  if (match === undefined) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'resolution',
        `value is not a valid enumeration member; permitted: ${RESOLUTIONS.join(', ')}`,
      ),
    };
  }
  return { ok: true, value: match };
}

function readTimestamp(
  name: 'from_timestamp' | 'to_timestamp',
  raw: string | null,
): Parsed<number | null> {
  if (raw === null) {
    return { ok: true, value: null };
  }
  const parsed = Date.parse(raw);
  if (Number.isNaN(parsed)) {
    return {
      ok: false,
      problem: invalidQueryParam(name, 'invalid datetime format'),
    };
  }
  return { ok: true, value: parsed };
}

/** The finest resolution the API permits for a window of the given size. */
export function minimumResolution(windowMs: number): TimeSeriesResolution {
  if (windowMs <= 6 * HOUR_MS) return 'PT1M';
  if (windowMs <= DAY_MS) return 'PT5M';
  if (windowMs <= 7 * DAY_MS) return 'PT15M';
  if (windowMs <= 30 * DAY_MS) return 'PT1H';
  return 'PT6H';
}

export type WindowQuery = {
  fromTimestamp: string | null;
  toTimestamp: string | null;
  resolution: string | null;
};

export type ResolvedWindow = {
  window: TimeSeriesWindow;
  fromMs: number;
  toMs: number;
};

/**
 * The `{from, to, resolution, interval_ms}` block every bucketed endpoint echoes
 * back, plus the grid the buckets are cut on.
 *
 * The three rejections are the ones an empty `200` would otherwise disguise as
 * "no data in this range" — which is precisely what the echoed window exists to
 * disambiguate, and it cannot do that job if the window it echoes is nonsense.
 */
export function resolveWindow(
  raw: WindowQuery,
  nowMs: number,
): Parsed<ResolvedWindow> {
  const to = readTimestamp('to_timestamp', raw.toTimestamp);
  if (!to.ok) return to;
  const from = readTimestamp('from_timestamp', raw.fromTimestamp);
  if (!from.ok) return from;
  const requested = readResolution(raw.resolution);
  if (!requested.ok) return requested;

  const toMs = to.value ?? nowMs;
  const fromMs = from.value ?? toMs - DEFAULT_WINDOW_MS;

  if (fromMs > toMs) {
    return {
      ok: false,
      problem: unprocessable(
        'from_timestamp must be less than or equal to to_timestamp',
      ),
    };
  }

  const windowMs = toMs - fromMs;
  if (windowMs > MAX_WINDOW_MS) {
    return {
      ok: false,
      problem: unprocessable(
        `requested window of ${windowMs}ms exceeds the maximum allowed of ${MAX_WINDOW_MS}ms`,
      ),
    };
  }

  const floor = minimumResolution(windowMs);
  const resolution = requested.value ?? floor;
  if (RESOLUTION_INTERVAL_MS[resolution] < RESOLUTION_INTERVAL_MS[floor]) {
    return {
      ok: false,
      problem: unprocessable(
        `resolution is too fine for the selected window; minimum allowed resolution is ${floor}`,
      ),
    };
  }

  return {
    ok: true,
    value: {
      fromMs,
      toMs,
      window: {
        from_timestamp: iso(fromMs),
        to_timestamp: iso(toMs),
        resolution,
        interval_ms: RESOLUTION_INTERVAL_MS[resolution],
      },
    },
  };
}

/**
 * Bucket starts on the resolution grid, newest first — the order every bucketed
 * endpoint returns and the order the charts assume.
 */
export function bucketStarts(
  fromMs: number,
  toMs: number,
  intervalMs: number,
  limit: number,
): number[] {
  const newest = floorToInterval(toMs, intervalMs);
  const oldest = floorToInterval(fromMs, intervalMs);
  const count = Math.min(
    Math.max(Math.floor((newest - oldest) / intervalMs) + 1, 0),
    limit,
  );

  return Array.from(
    { length: count },
    (_, index) => newest - index * intervalMs,
  );
}

/**
 * Case-insensitive substring, matching the `LIKE '%…%'` the activity feed uses.
 * A null haystack matches an empty needle, as its `COALESCE(…, '')` does.
 */
export function includesInsensitive(
  haystack: string | null | undefined,
  needle: string,
): boolean {
  return (haystack ?? '').toLowerCase().includes(needle.toLowerCase());
}

export function equalsInsensitive(
  left: string | null | undefined,
  right: string,
): boolean {
  return (left ?? '').toLowerCase() === right.toLowerCase();
}

/** Hex identity — addresses and transaction hashes — is the lower-cased form. */
export function sameHex(
  left: string | null | undefined,
  right: string | null | undefined,
): boolean {
  return (
    left !== null &&
    left !== undefined &&
    right !== null &&
    right !== undefined &&
    left.toLowerCase() === right.toLowerCase()
  );
}
