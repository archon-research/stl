import { useSearch } from '@tanstack/react-router';

import { toProvenance } from '../router/search-params';
// Selects the provenance every endpoint that supports it answers from:
//
//   /allocation                    → source: "both"      (merged, the default)
//   /allocation?source=indexed     → source: "indexed"   (STL's own model)
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
import type {
  Allocation,
  PrimeRiskCapital,
  Provenance,
} from '../types/allocation';

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
    : 'both');

/**
 * The provenance to request, always stated rather than defaulted.
 *
 * The API's own default is not `indexed`, so omitting it would silently change
 * what the page shows the moment that default moves.
 */
export const sourceQuery = { source: PROVENANCE } as const;

/** Whether the request asked for Sky's figures, alone or merged. */
export const showsReference =
  PROVENANCE === 'reference' || PROVENANCE === 'both';

/** Whether the request asked for STL's figures, alone or merged. */
export const showsIndexed = PROVENANCE === 'indexed' || PROVENANCE === 'both';

/**
 * Whether the provenance on screen can change without fetching anything.
 *
 * A composite response carries both provenances in one payload, so narrowing it
 * to either is a projection of data already held. A response fetched as one
 * provenance alone holds nothing to narrow, so switching from it needs a real
 * request — and that means a fresh entry, because `PROVENANCE` above is fixed
 * for the session by design.
 */
export const canRestateProvenance = PROVENANCE === 'both';

/**
 * The provenance being *shown*, which is not always the one fetched.
 *
 * Reads the URL, so a client-side navigation changes it; `PROVENANCE` stays at
 * whatever the session fetched. The two differ only while narrowing a composite
 * response, which is exactly when no request is needed.
 */
export function useProvenanceView(): {
  provenance: Provenance;
  showsReference: boolean;
  showsIndexed: boolean;
} {
  // Not strict: the drawer and the band render on the activities route too,
  // where the allocation search schema does not apply.
  const search = useSearch({ strict: false }) as { source?: unknown };
  const shown = canRestateProvenance
    ? (toProvenance(
        typeof search.source === 'string' ? search.source : undefined,
      ) ?? 'both')
    : PROVENANCE;

  return {
    provenance: shown,
    showsReference: shown === 'reference' || shown === 'both',
    showsIndexed: shown === 'indexed' || shown === 'both',
  };
}

/**
 * Sky's figure for a merged row, or STL's where Sky reports none.
 *
 * Sky's is the preferred model in composite mode: it prices positions STL's
 * models do not yet cover, so reading STL's first would show a zero where a
 * requirement exists. Under `indexed` there is no Sky figure to prefer and this
 * reduces to STL's own.
 *
 * Reads the row rather than `PROVENANCE`: `reference_*` is populated only under
 * `source=both`, so the row itself says whether there is anything to prefer, and
 * a row Sky alone reports already carries its figures in the bare fields.
 */
export function preferReference(
  skyValue: string | null | undefined,
  stlValue: string | null | undefined,
): string | null {
  return skyValue ?? stlValue ?? null;
}

/**
 * STL's figure for a merged row, or Sky's where STL has none.
 *
 * The mirror of `preferReference`, and the rule for anything measured rather
 * than modelled: STL computes a position's value from the chain it indexes, so
 * where it has a figure that figure is the more direct one. Sky's is the
 * fallback rather than the lead — but not a rare one, since STL prices only the
 * chains it indexes and a position on an unserved chain has a real balance and
 * no value at all.
 *
 * Absence is what triggers the fallback, so a published zero is kept: both
 * sides report real zeros, and coalescing one to the other's figure would
 * invent a holding.
 */
export function preferIndexed(
  stlValue: string | null | undefined,
  skyValue: string | null | undefined,
): string | null {
  return stlValue ?? skyValue ?? null;
}

/**
 * A row's own RRC/CRR figure for a merged position, or Sky's reference figure
 * where the model reports none.
 *
 * Inverts `preferReference` for these two fields specifically. Every other
 * merged figure prefers Sky's because Sky prices positions STL does not yet
 * model — but under `source=both`,
 * `PrimeRiskCapitalService._model_preference` dispatches to `core_model`
 * first and never falls back past it, so a merged row's bare
 * `crr_pct`/`required_risk_capital_usd` already *is* core_model's figure
 * whenever core priced the position; `reference_*` exists only as the
 * fallback for a position core_model can't price. Preferring Sky's
 * unconditionally (as `preferReference` does) would show its published
 * figure — including a reported 0/0 — over a real core-priced one, which is
 * the VEC-272 bug. Same coalescing as `preferIndexed`, kept under its own
 * name so a call site reads as a deliberate RRC/CRR decision rather than the
 * general STL-over-Sky rule.
 */
export function preferModelRiskFigure(
  modelValue: string | null | undefined,
  referenceValue: string | null | undefined,
): string | null {
  return preferIndexed(modelValue, referenceValue);
}

/**
 * A composite response as the chosen provenance alone would have answered it.
 *
 * Every display reads a figure as "Sky's, else STL's" (`preferReference`), so
 * narrowing is a matter of clearing the half that is not being shown rather
 * than teaching each of those reads which provenance it is in: under `indexed`
 * the `reference_*` fields go, leaving STL's; under `reference` the bare fields
 * go, leaving Sky's with no fallback — which is what asking for Sky alone means.
 *
 * Applied once where the response is held, so no view can disagree with another
 * about which provenance it is showing.
 *
 * Only a composite response has a half to clear. A single-provenance fetch
 * already answers in the bare fields — under `source=reference` Sky's figures
 * arrive there with `reference_*` null — so narrowing it again would clear the
 * very fields carrying the chosen provenance and render every figure as n/a.
 */
export function narrowRiskCapital(
  view: Provenance,
  response: PrimeRiskCapital | null,
): PrimeRiskCapital | null {
  if (response === null || view === 'both' || PROVENANCE !== 'both') {
    return response;
  }

  const sky = view === 'reference';
  const drop = <T>(value: T, isSkys: boolean): T | null =>
    isSkys === sky ? value : null;

  return {
    ...response,
    prime_exposure_usd: drop(response.prime_exposure_usd, false) ?? '0',
    reference_prime_exposure_usd: drop(
      response.reference_prime_exposure_usd,
      true,
    ),
    prime_required_risk_capital_usd:
      drop(response.prime_required_risk_capital_usd, false) ?? '0',
    reference_prime_required_risk_capital_usd: drop(
      response.reference_prime_required_risk_capital_usd,
      true,
    ),
    total_risk_capital_usd: drop(response.total_risk_capital_usd, false),
    reference_total_risk_capital_usd: drop(
      response.reference_total_risk_capital_usd,
      true,
    ),
    prime_encumbrance_ratio: drop(response.prime_encumbrance_ratio, false),
    reference_prime_encumbrance_ratio: drop(
      response.reference_prime_encumbrance_ratio,
      true,
    ),
    per_allocation: response.per_allocation.map((row) => ({
      ...row,
      required_risk_capital_usd: drop(row.required_risk_capital_usd, false),
      crr_pct: drop(row.crr_pct, false),
      reference_required_risk_capital_usd: drop(
        row.reference_required_risk_capital_usd,
        true,
      ),
      reference_crr_pct: drop(row.reference_crr_pct, true),
    })),
  };
}

/**
 * The rows the chosen provenance would have returned.
 *
 * A row states which provenances describe it, so narrowing drops the ones the
 * other provenance alone reported. A `both` row belongs to either view.
 */
export function narrowAllocations(
  view: Provenance,
  allocations: readonly Allocation[],
): Allocation[] {
  if (view === 'both') return [...allocations];

  const excluded = view === 'indexed' ? 'reference' : 'indexed';
  return allocations.filter((allocation) => allocation.source !== excluded);
}
