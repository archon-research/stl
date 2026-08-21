import { toProvenance } from '../router/search-params';
// Selects the provenance every endpoint that supports it answers from:
//
//   /allocation                    → source: "indexed"   (STL's own model)
//   /allocation?source=reference   → source: "reference" (Sky's published figures)
//
// The settings menu writes it; opening the same view twice also works, which is
// how the two are compared side by side.
//
// `source` is declared on the root search schema rather than read loose from the
// URL. The router redirects an entry URL to its validated search, and every
// navigation rebuilds search from the validated previous value, so an undeclared
// param would be stripped on arrival and again on the first prime switch — the
// selection would appear to work until you clicked something.
//
// Read once here, from the entry URL, rather than through `useSearch`. Two
// reasons: the consumer is `lib/api`, which is not a component; and the value
// must not change mid-session, because the request that populated a cached
// series and the request refreshing it would then disagree about provenance —
// the one thing the `source` field exists to make impossible.
import type { Provenance } from '../types/allocation';

const entryParams = new URLSearchParams(globalThis.location?.search ?? '');

export const PROVENANCE: Provenance =
  toProvenance(entryParams.get('source') ?? undefined) ??
  // The superseded spelling, still live in shared links. `reference=false` asked
  // for STL's own figures by name, so it is not the same as an absent param.
  (entryParams.has('reference')
    ? (toProvenance(
        ['0', 'false', 'no', 'off'].includes(
          (entryParams.get('reference') ?? '').toLowerCase(),
        )
          ? 'indexed'
          : 'reference',
      ) ?? 'indexed')
    : 'indexed');

/**
 * The provenance to request, always stated rather than defaulted.
 *
 * The API's own default is not `indexed`, so omitting it would silently change
 * what the page shows the moment that default moves.
 */
export const sourceQuery = { source: PROVENANCE } as const;

/** Whether Sky's published figures are on screen, alone or merged. */
export const showsReference =
  PROVENANCE === 'reference' || PROVENANCE === 'both';

/** Whether STL's own figures are on screen, alone or merged. */
export const showsIndexed = PROVENANCE === 'indexed' || PROVENANCE === 'both';
