// Selects the provenance every endpoint that supports it answers from:
//
//   /allocation            → source: "self"      (STL's own model)
//   /allocation?reference  → source: "reference" (Sky's published figures)
//
// The settings menu writes it; opening the same view twice also works, which is
// how the two are compared side by side.
//
// `reference` is declared on the root search schema rather than read loose from
// the URL. The router redirects an entry URL to its validated search, and every
// navigation rebuilds search from the validated previous value, so an undeclared
// param would be stripped on arrival and again on the first prime switch — the
// flag would appear to work until you clicked something.
//
// Read once here, from the entry URL, rather than through `useSearch`. Two
// reasons: the consumer is `lib/api`, which is not a component; and the value
// must not change mid-session, because the request that populated a cached
// series and the request refreshing it would then disagree about provenance —
// the one thing the `source` field exists to make impossible.
import { toReferenceFlag } from '../router/search-params';

const entryValue = new URLSearchParams(globalThis.location?.search ?? '').get(
  'reference',
);

export const REFERENCE_MODE = toReferenceFlag(entryValue ?? undefined) === true;

/** `{ reference: true }` when the flag is on, else nothing to spread. */
export const referenceQuery = REFERENCE_MODE
  ? ({ reference: true } as const)
  : {};
