import {
  isRangePreset,
  type RangePreset,
} from '@archon-research/design-system';
import { z } from 'zod';

import type { AllocationCategory } from '../types/allocation';

export const ALLOCATION_CATEGORIES = [
  'allocation',
  'pol',
  'psm3',
  'asset',
  'custody',
] as const satisfies readonly AllocationCategory[];

export const DRAWER_TABS = ['risk', 'rrc', 'activity'] as const;

export const ACTIVITY_ACTIONS = ['in', 'out', 'sweep'] as const;

export type DrawerTab = (typeof DRAWER_TABS)[number];
export type ActivityAction = (typeof ACTIVITY_ACTIONS)[number];

// The router JSON-parses search values, so `?network=1` arrives as the number 1,
// and a hand-edited URL must degrade to "absent" rather than fail the route.
function toSearchText(value: unknown): string | undefined {
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (typeof value === 'string' && value !== '') {
    return value;
  }
  return undefined;
}

/**
 * Narrows a URL value (or a raw control value) to a closed option set. Shared by
 * the schemas below and by the change handlers that write back into them.
 */
export function toSearchOption<T extends string>(
  value: unknown,
  allowed: readonly T[],
): T | undefined {
  const text = toSearchText(value);
  return allowed.includes(text as T) ? (text as T) : undefined;
}

function textParam() {
  return z.optional(z.unknown().transform(toSearchText));
}

function oneOfParam<T extends string>(allowed: readonly T[]) {
  return z.optional(
    z.unknown().transform((value) => toSearchOption(value, allowed)),
  );
}

// `custom` is not a preset in the URL: a usable from/to pair is what marks a
// custom range, so the selection can never outlive the bounds it needs.
function rangePresetParam() {
  return z.optional(
    z.unknown().transform((value) => {
      const text = toSearchText(value);
      return isRangePreset(text) && text !== 'custom' ? text : undefined;
    }),
  );
}

type RangeSelection = {
  range?: Exclude<RangePreset, 'custom'> | undefined;
  from?: string | undefined;
  to?: string | undefined;
};

function hasUsableCustomBounds(
  from: string | undefined,
  to: string | undefined,
): boolean {
  if (!from || !to) {
    return false;
  }

  const fromMs = new Date(from).getTime();
  const toMs = new Date(to).getTime();

  return Number.isFinite(fromMs) && Number.isFinite(toMs) && toMs > fromMs;
}

// A hand-edited URL can carry unparsable or reversed custom timestamps; drop
// both bounds so no consumer sends a bad range downstream.
function normalizeRangeSelection<T extends RangeSelection>(selection: T): T {
  if (hasUsableCustomBounds(selection.from, selection.to)) {
    return selection;
  }

  return { ...selection, from: undefined, to: undefined };
}

/**
 * Params every view shares, carried by the root route so both leaves inherit one
 * definition. Two consequences worth knowing: `from`/`to` survive validation
 * only as a usable custom range, so their presence *is* the custom case; and
 * `prime` stays here because the activities view still selects a prime through
 * the query string while the allocation view carries it in the path.
 */
export const sharedSearchSchema = z
  .object({
    prime: textParam(),
    network: textParam(),
    protocol: textParam(),
    range: rangePresetParam(),
    from: textParam(),
    to: textParam(),
  })
  .transform(normalizeRangeSelection);

/**
 * Allocation-view params. Scoping them to the `/allocation` branch is what stops
 * `sort`/`q` from being one namespace shared with any other view's table.
 */
export const allocationSearchSchema = z.object({
  category: oneOfParam(ALLOCATION_CATEGORIES),
  tab: oneOfParam(DRAWER_TABS),
  aa: oneOfParam(ACTIVITY_ACTIONS),
  sort: textParam(),
  q: textParam(),
  drawer: oneOfParam(['1']),
  row: textParam(),
});

export const activitiesSearchSchema = z.object({
  token: textParam(),
  aa: oneOfParam(ACTIVITY_ACTIONS),
  allp: textParam(),
});

/**
 * Every param the shell may write, regardless of which view is mounted. The
 * router types each individual navigation against its destination route; this is
 * only the patch shape the shell's own helpers accept.
 */
export type AppSearchPatch = Partial<
  z.infer<typeof sharedSearchSchema> &
    z.infer<typeof allocationSearchSchema> &
    z.infer<typeof activitiesSearchSchema>
>;
