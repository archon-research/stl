import { parseNumericValue } from '../../shared/lib/dashboard';
import {
  preferIndexed,
  preferModelRiskFigure,
} from '../../shared/lib/provenance';
import type {
  Allocation,
  AllocationRiskCapital,
  Prime,
} from '../../shared/types/allocation';

// Joined on the keys both endpoints publish rather than on `receipt_token_id`,
// which only STL's own rows carry: Sky prices off-chain custody and the Arkis
// vault — its two largest requirements — and STL resolves no receipt token for
// either, so an id join left them blank. The keys encode the rules that make
// those pair up (custody by protocol, everything else by chain and address), so
// the grid does not restate them.
function lookupAllocationRiskCapital(
  riskByPositionKey: Map<string, AllocationRiskCapital>,
  allocation: Allocation,
): AllocationRiskCapital | undefined {
  for (const key of allocation.position_keys ?? []) {
    const entry = riskByPositionKey.get(key);
    if (entry !== undefined) return entry;
  }

  return undefined;
}

// Required risk capital in USD, derived rather than carried: `crr x exposure`,
// against the two figures shown beside it in the same row.
//
// Neither provenance's published RRC is used. Under `both` the ratio is Sky's
// and the exposure is STL's, so upstream's own requirement was computed against
// a different exposure than the one on screen and the three columns did not
// reconcile. Deriving costs provenance purity — the product of two sources is a
// figure neither published — and buys a row a reader can check by eye, which is
// the trade this table is for.
function derivedRiskCapitalUsd(
  crrPct: number | null,
  exposureUsd: number | null,
): number | null {
  if (crrPct === null || exposureUsd === null) {
    return null;
  }

  // `crrPct` is a 0-100 percentage at every boundary in this codebase; upstream
  // reports a 0-1 fraction and the adapter rescales once.
  return (exposureUsd * crrPct) / 100;
}

// Comparable capital-risk ratio (0-100), the row's own model preferred over
// Sky's — see `preferModelRiskFigure` for why this is the opposite of the
// exposure column's rule.
function preferredCrrPct(
  entry: AllocationRiskCapital | undefined,
): number | null {
  if (entry === undefined) {
    return null;
  }

  return parseNumericValue(
    preferModelRiskFigure(entry.crr_pct, entry.reference_crr_pct),
  );
}

// Each row's share of the requirement the table itself accounts for, 0-1.
//
// The denominator is Σ of the RRC column over the rows on screen, not the
// prime's published total: the RRC column is derived per row, so dividing by a
// total computed some other way would not sum to 1 and a reader adding the
// column up would find it short. `encumbrance_contribution` on the row divides
// by available capital instead and sums to the encumbrance ratio, which is a
// different question.
//
// Filtering therefore rebases the column — it answers "of what is shown" — and
// rows with no RRC are absent from both the numerator and the sum.
//
// Summing the visible rows was tried before and abandoned, when the RRC column
// was carried from the risk-capital response: Sky priced positions STL resolved
// no receipt token for — off-chain custody and the Arkis vault, its two largest
// — so they never reached a grid row and the denominator collapsed to $2.8M
// against a real $19.1M. Deriving each row's RRC from the CRR and exposure it
// already shows is what makes this safe now, because both of those positions do
// carry a CRR: measured against spark, they are the two largest contributors to
// a $22.9M sum. Check that before carrying the column back to a published
// total — the failure was a join gap, not the arithmetic.
export function withRrcShare(rows: AllocationGridRow[]): AllocationGridRow[] {
  // A chain-mismatched row is withheld from the RRC and share cells, so it is
  // withheld from the sum too. It cannot reach here with a figure today — the
  // risk fetch is scoped to the prime's own chain, so such a row has no entry
  // and no CRR to derive from — but the column's promise is that what is shown
  // adds to 100%, and that should not rest on a scoping decision made in
  // another file.
  const contributes = (row: AllocationGridRow) =>
    !row.risk.chainMismatch && row.risk.riskCapitalUsd !== null;

  const total = rows.reduce(
    (sum, row) => sum + (contributes(row) ? (row.risk.riskCapitalUsd ?? 0) : 0),
    0,
  );
  if (total === 0) {
    return rows;
  }

  return rows.map((row) =>
    contributes(row)
      ? {
          ...row,
          risk: {
            ...row.risk,
            sharePct: (row.risk.riskCapitalUsd ?? 0) / total,
          },
        }
      : row,
  );
}

// riskByPositionKey is built from a risk-capital call scoped to
// selectedPrime's own chain, so an allocation on a different chain has no
// entry there for the same reason a genuine non-applicable allocation
// doesn't: the map simply has nothing for its receipt_token_id. Distinguish
// the two so a real risk capital figure that is merely uncomputed for this
// chain doesn't read as the same "n/a" as an allocation no risk model
// applies to.
//
// The receipt_token_id check gates this to rows that could ever carry a
// figure. A null receipt_token_id (the Anchorage custody row, and every
// direct/bare holding) can never key into riskByPositionKey regardless of
// chain, so without this check a mainnet-primary prime would flag its own
// off-chain custody row (chain_id 0) as a cross-chain mismatch and claim a
// figure exists but merely wasn't fetched, when "n/a" is the correct read.
function isRiskCapitalChainMismatch(
  selectedPrime: Prime | null,
  allocation: Allocation,
): boolean {
  return (
    allocation.receipt_token_id != null &&
    selectedPrime !== null &&
    allocation.chain_id !== selectedPrime.chain_id
  );
}

/**
 * A grid row carries its risk figures as data rather than having the column
 * accessors look them up: TanStack caches `row.getValue` per row for the
 * lifetime of a `data` identity, so a lookup through a column closure freezes
 * at whatever the map held when a value was first read — risk capital arrives
 * after the allocations, and sorting by CRR ordered the stale nulls while the
 * cells (re-rendered with the fresh closure) showed real figures. Deriving the
 * figures into the rows makes risk arrival a `data` change, which is the one
 * signal TanStack rebuilds its caches on.
 */
/**
 * `state` keeps the risk columns honest while the risk-capital call is not
 * settled: `n/a` claims no model applies, which is false both mid-flight and
 * after a failed fetch — a staging outage once rendered a full column of
 * `n/a` with the only error signal far away in the metrics band.
 */
export type RiskFetchState = 'loading' | 'error' | 'ready';

export type AllocationGridRow = Allocation & {
  risk: {
    state: RiskFetchState;
    entry: AllocationRiskCapital | undefined;
    chainMismatch: boolean;
    /** True when the figures shown lean on Legacy's published values. */
    fromReference: boolean;
    exposureUsd: number | null;
    riskCapitalUsd: number | null;
    crrPct: number | null;
    sharePct: number | null;
  };
};

// The value the Exposure column shows, as a number.
//
// The headline total is the sum of this over every row, so the two are the same
// rule read twice rather than two rules that happen to agree. Sky-only rows are
// included: after the merge each position appears once whichever side reported
// it, so summing them all counts the money once — and excluding them made the
// headline unable to match the table it sits above by construction.
export function rowExposureUsd(allocation: Allocation): number | null {
  return parseNumericValue(
    preferIndexed(allocation.amount_usd, allocation.reference_amount_usd),
  );
}

export function toAllocationGridRow(
  allocation: Allocation,
  riskByPositionKey: Map<string, AllocationRiskCapital>,
  riskFetchState: RiskFetchState,
  selectedPrime: Prime | null,
): AllocationGridRow {
  const entry = lookupAllocationRiskCapital(riskByPositionKey, allocation);
  // The row's own value, which is the same measurement in both provenances —
  // not the risk-capital breakdown's `exposure`, which covers only the priced
  // subset and runs about a third smaller.
  const exposureUsd = rowExposureUsd(allocation);
  const crrPct = preferredCrrPct(entry);
  return {
    ...allocation,
    risk: {
      state: riskFetchState,
      entry,
      chainMismatch: isRiskCapitalChainMismatch(selectedPrime, allocation),
      // Whether the figures shown lean on Sky. Both the exposure and the ratio
      // now prefer STL's own figure (see `preferModelRiskFigure`), so a row is
      // Sky-flavoured only when one of them actually fell back: the exposure
      // has none of STL's, the row is wholly Sky-reported (`source ===
      // 'reference'`, e.g. a position STL never joined to), or the model
      // reported no ratio and Sky's filled in for it.
      fromReference:
        allocation.amount_usd == null ||
        (entry !== undefined &&
          (entry.source === 'reference' ||
            (entry.crr_pct == null && entry.reference_crr_pct != null))),
      exposureUsd,
      riskCapitalUsd: derivedRiskCapitalUsd(crrPct, exposureUsd),
      crrPct,
      // Filled by `withRrcShare` once every row is known: the denominator is
      // the column's own sum, which no single row can see.
      sharePct: null,
    },
  };
}
