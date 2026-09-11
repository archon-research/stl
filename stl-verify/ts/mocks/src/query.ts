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
import type {
  AggregationMethod,
  Provenance,
  ResampledTimeSeriesWindow,
  TimeSeriesFrequency,
  TimeSeriesWindow,
} from './schema.ts';

const MS_BY_FREQUENCY: Record<TimeSeriesFrequency, number> = {
  PT1M: MINUTE_MS,
  PT5M: 5 * MINUTE_MS,
  PT15M: 15 * MINUTE_MS,
  PT1H: HOUR_MS,
  PT6H: 6 * HOUR_MS,
  P1D: DAY_MS,
};

// `Object.keys` is `string[]` however the record is typed, and the record is
// the single source of truth for which frequencies exist -- so filter, not cast.
const FREQUENCIES: TimeSeriesFrequency[] = Object.keys(MS_BY_FREQUENCY).filter(
  (key): key is TimeSeriesFrequency => Object.hasOwn(MS_BY_FREQUENCY, key),
);

// A Record over the generated union, for the same reason as the map above: a
// method added to the API becomes a build error here, not a mystery 422.
const METHOD_KEYS = { 'end-period': 0 } satisfies Record<
  AggregationMethod,
  number
>;
const AGGREGATION_METHODS: AggregationMethod[] = Object.keys(
  METHOD_KEYS,
).filter((key): key is AggregationMethod => Object.hasOwn(METHOD_KEYS, key));

const DEFAULT_WINDOW_MS = DAY_MS;
const MAX_WINDOW_MS = 366 * DAY_MS;

/**
 * The spellings pydantic parses a `bool` query param from, in either case.
 * Sets, not a record, so an inherited key cannot answer as a spelling.
 */
const TRUE_WORDS = new Set(['1', 't', 'true', 'y', 'yes', 'on']);
const FALSE_WORDS = new Set(['0', 'f', 'false', 'n', 'no', 'off']);

/**
 * Absence is the param's default, which is `false`. Anything outside the two
 * vocabularies is a `422`, not a quiet `false`: the mock that reads
 * `reference=maybe` as off serves STL's own figures to a screen asking for
 * Sky's, a mismatch the app would then blame on itself.
 */
function readFlag(name: string, raw: string | null): Parsed<boolean> {
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

function readFrequency(raw: string | null): Parsed<TimeSeriesFrequency | null> {
  if (raw === null) {
    return { ok: true, value: null };
  }
  const match = FREQUENCIES.find((candidate) => candidate === raw);
  if (match === undefined) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'frequency',
        `value is not a valid enumeration member; permitted: ${FREQUENCIES.join(', ')}`,
      ),
    };
  }
  return { ok: true, value: match };
}

/** Absent is the switch, not a default: no method means the stored frequency. */
function readAggregationMethod(
  raw: string | null,
): Parsed<AggregationMethod | null> {
  if (raw === null) {
    return { ok: true, value: null };
  }
  const match = AGGREGATION_METHODS.find((candidate) => candidate === raw);
  if (match === undefined) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'aggregation_method',
        `value is not a valid enumeration member; permitted: ${AGGREGATION_METHODS.join(', ')}`,
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

/** The finest frequency the API permits for a window of the given size. */
function minimumFrequency(windowMs: number): TimeSeriesFrequency {
  if (windowMs <= 6 * HOUR_MS) return 'PT1M';
  if (windowMs <= DAY_MS) return 'PT5M';
  if (windowMs <= 7 * DAY_MS) return 'PT15M';
  if (windowMs <= 30 * DAY_MS) return 'PT1H';
  return 'PT6H';
}

export type WindowQuery = {
  fromTimestamp: string | null;
  toTimestamp: string | null;
  frequency: string | null;
  aggregationMethod: string | null;
  /** The method an always-resampled route applies when the caller names none. */
  defaultAggregationMethod?: AggregationMethod;
};

export type ResolvedWindow = {
  fromMs: number;
  toMs: number;
  frequency: TimeSeriesFrequency;
  frequencyMs: number;
  bucketed: boolean;
};

/**
 * The resolved window and the grid its buckets are cut on.
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
  const requested = readFrequency(raw.frequency);
  if (!requested.ok) return requested;
  const named = readAggregationMethod(raw.aggregationMethod);
  if (!named.ok) return named;
  const method = named.value ?? raw.defaultAggregationMethod ?? null;

  // A frequency names the grid a method cuts on, so without one the API would
  // validate it and then drop it — a silent no-op the echo cannot report.
  if (requested.value !== null && method === null) {
    return {
      ok: false,
      problem: unprocessable(
        'frequency names the grid an aggregation_method cuts on; ' +
          'supply aggregation_method=end-period or omit frequency',
      ),
    };
  }

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

  const floor = minimumFrequency(windowMs);
  const frequency = requested.value ?? floor;
  if (MS_BY_FREQUENCY[frequency] < MS_BY_FREQUENCY[floor]) {
    return {
      ok: false,
      problem: unprocessable(
        `frequency is too fine for the selected window; minimum allowed frequency is ${floor}`,
      ),
    };
  }

  return {
    ok: true,
    value: {
      fromMs,
      toMs,
      frequency,
      frequencyMs: MS_BY_FREQUENCY[frequency],
      bucketed: method !== null,
    },
  };
}

/** The echo the unresampled arm of a route serves: the window, no grid. */
export function rawWindowEcho(resolved: ResolvedWindow): TimeSeriesWindow {
  return {
    from_timestamp: iso(resolved.fromMs),
    to_timestamp: iso(resolved.toMs),
  };
}

/** The echo a route that only ever answers with buckets serves. */
export function resampledWindowEcho(
  resolved: ResolvedWindow,
): ResampledTimeSeriesWindow {
  return {
    from_timestamp: iso(resolved.fromMs),
    to_timestamp: iso(resolved.toMs),
    frequency: resolved.frequency,
    frequency_ms: resolved.frequencyMs,
  };
}

/**
 * Bucket starts on the requested grid, newest first — the order every bucketed
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

const PROVENANCES: readonly Provenance[] = ['indexed', 'reference', 'both'];

/**
 * Resolve the provenance the way the API does.
 *
 * `source` wins where both are given and they agree; a disagreement is a 422
 * there, so it is a problem here too. `reference=false` asked for STL's own
 * figures by name, so it is `indexed` rather than the default.
 */
export function readProvenance(
  source: string | null,
  reference: string | null,
): Parsed<Provenance> {
  const named = PROVENANCES.find((candidate) => candidate === source);
  if (source !== null && named === undefined) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'source',
        `Input should be ${PROVENANCES.join(', ')}`,
      ),
    };
  }

  if (reference === null) {
    return { ok: true, value: named ?? 'indexed' };
  }

  const flag = readFlag('reference', reference);
  if (!flag.ok) return flag;
  const legacy: Provenance = flag.value ? 'reference' : 'indexed';

  if (named !== undefined && named !== legacy) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'source',
        `conflicts with the deprecated reference param, which asked for ${legacy}`,
      ),
    };
  }
  return { ok: true, value: named ?? legacy };
}

const ACTIVITY_SERIES = ['flow', 'balance'] as const;
export type AllocationActivitySeries = (typeof ACTIVITY_SERIES)[number];

/**
 * The aggregated arm's ``series`` selector. Defaults to `flow`, matching the
 * endpoint, and rejects anything else by name rather than coercing a typo
 * (`series=blance`) to the default -- the real endpoint would 422 it via its
 * `Literal["flow", "balance"]`, and a mock that quietly accepted it would let
 * that typo pass every mock-backed test.
 */
export function readSeries(
  raw: string | null,
): Parsed<AllocationActivitySeries> {
  if (raw === null) {
    return { ok: true, value: 'flow' };
  }
  const match = ACTIVITY_SERIES.find((candidate) => candidate === raw);
  if (match === undefined) {
    return {
      ok: false,
      problem: invalidQueryParam(
        'series',
        `value is not a valid enumeration member; permitted: ${ACTIVITY_SERIES.join(', ')}`,
      ),
    };
  }
  return { ok: true, value: match };
}
