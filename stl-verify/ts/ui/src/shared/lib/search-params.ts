import {
  isRangePreset,
  type RangePreset,
} from '@archon-research/design-system';
import {
  oneOfParam,
  textParam,
  toSearchText,
} from '@archon-research/router-kit';
import { z } from 'zod';

import type { AllocationCategory, Provenance } from '../types/allocation';

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

// Off values are explicit; anything else present — including the bare `?reference`
// switch, which arrives as an empty string — means on. Absence is the only other
// state, so this is `true | undefined` and never `false`: the entry-time cleanup
// drops a param whose validated value is undefined, which is what stops
// `?reference=false` from sitting in a URL that reads as "on".
//
// Exported because `shared/lib/provenance` applies the same rule to the entry URL
// before the router has validated anything; two spellings of "is it on" would be
// one drift away from a page mixing both provenances.
const PROVENANCES: readonly Provenance[] = ['indexed', 'reference', 'both'];

// `includes` on a `readonly Provenance[]` demands an argument that is already
// one, which is the only reason this needed an assertion; `some` narrows.
function isProvenance(value: string): value is Provenance {
  return PROVENANCES.some((allowed) => allowed === value);
}

/**
 * A search param arrives as `unknown` and only a primitive can name a value.
 * Anything else stringifies to '[object Object]', which would then be compared
 * against the vocabularies below as though someone had typed it.
 */
function paramText(value: unknown): string | undefined {
  return typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
    ? String(value).toLowerCase()
    : undefined;
}

export function toProvenance(value: unknown): Provenance | undefined {
  const candidate = paramText(value);

  if (candidate === undefined) {
    return undefined;
  }

  return isProvenance(candidate) ? candidate : undefined;
}

function provenanceParam() {
  return z.optional(z.unknown().transform(toProvenance));
}

const REFERENCE_OFF_VALUES = new Set(['false', '0', 'no', 'off']);

function legacyReferenceParam() {
  return z.optional(
    z.unknown().transform((value) =>
      value === undefined || value === null
        ? undefined
        : // A non-primitive is still *present*, so it reads as on -- only the
          // listed spellings turn it off.
          !REFERENCE_OFF_VALUES.has(paramText(value) ?? ''),
    ),
  );
}

/**
 * Fold the superseded boolean into `source` and drop it.
 *
 * `reference=false` asked for STL's own figures by name, so it becomes
 * `indexed` rather than falling through to the default. An explicit `source`
 * wins: it is the current spelling.
 */
function adoptLegacyReferenceFlag<
  T extends { source?: Provenance; reference?: boolean },
>({ reference, ...rest }: T): Omit<T, 'reference'> {
  if (rest.source !== undefined || reference === undefined) {
    return rest;
  }

  return { ...rest, source: reference ? 'reference' : 'indexed' };
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
    // Selects the provenance every endpoint answers from. Declared here rather
    // than read loose from the URL so the router carries it across navigations
    // instead of stripping it as unvalidated.
    source: provenanceParam(),
    // Declared only so the transform below can translate it. Undeclared, it
    // would be stripped on entry and a shared link would land on the default
    // while `shared/lib/provenance` had already read it -- a URL disagreeing with the
    // page it produced.
    reference: legacyReferenceParam(),
  })
  .transform(adoptLegacyReferenceFlag)
  .transform(normalizeRangeSelection);

/**
 * Allocation-view params. Scoping them to the `/allocation` branch is what stops
 * `sort`/`q` from being one namespace shared with any other view's table.
 */
export const allocationSearchSchema = z.object({
  category: oneOfParam(ALLOCATION_CATEGORIES),
  tab: oneOfParam(DRAWER_TABS),
  // `daa` (drawer action), not `aa`: sharing one key with the activities view
  // leaked whichever filter was set last across every switch between them.
  daa: oneOfParam(ACTIVITY_ACTIONS),
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
