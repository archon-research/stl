import { useMemo } from 'react';

import {
  encumbranceSeverity,
  parseNumericValue,
  type EncumbranceSeverity,
} from '../../shared/lib/dashboard';
import { preferReference } from '../../shared/lib/provenance';
import type {
  Allocation,
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
} from '../../shared/types/allocation';
import { rowExposureUsd } from './allocationGridRows';
import type { UseAllocationGridArgs } from './useAllocationGrid';

export type UseAllocationSummariesArgs = Pick<
  UseAllocationGridArgs,
  | 'referenceDebt'
  | 'primeDebtSnapshot'
  | 'topMetricsAllocations'
  | 'areAllocationsSettled'
  | 'allocations'
  | 'riskCapital'
  | 'isRiskCapitalLoading'
  | 'selectedPrime'
>;

export type UseAllocationSummariesResult = {
  summary: {
    allocationCount: number;
    latestActivityAt: string | null;
    totalUsd: number;
  } | null;
  overallSummary: { allocationCount: number; totalUsd: number } | null;
  debtWad: string | null | undefined;
  debtObservedAt: string | undefined;
  debtTimestampLabel: string;
  debtIlkLabel: string | null;
  encumbranceRatio: number | null;
  encumbranceBreach: EncumbranceSeverity;
  encumbranceCaption: string | null;
  showTopMetricsSkeleton: boolean;
  hasTopMetrics: boolean;
};

function computeAllocationSummary(
  topMetricsAllocations: Allocation[],
  areAllocationsSettled: boolean,
): UseAllocationSummariesResult['summary'] {
  if (topMetricsAllocations.length === 0) {
    // An empty list means either the request hasn't settled yet or it
    // settled with zero rows; `PrimeMetricsBand` reads `summary === null`
    // as "still loading" (MetricCardCell), so a resolved-empty fetch must
    // report zeros here instead of `null` or the Total allocation card
    // would stay on its skeleton forever.
    return areAllocationsSettled
      ? { allocationCount: 0, latestActivityAt: null, totalUsd: 0 }
      : null;
  }

  const totalUsd = topMetricsAllocations.reduce(
    (sum, allocation) => sum + (rowExposureUsd(allocation) ?? 0),
    0,
  );

  const latestActivityAt = topMetricsAllocations.reduce<string | null>(
    (latest, allocation) => {
      if (!allocation.latest_activity_at) {
        return latest;
      }

      if (!latest) {
        return allocation.latest_activity_at;
      }

      return new Date(allocation.latest_activity_at) > new Date(latest)
        ? allocation.latest_activity_at
        : latest;
    },
    null,
  );

  return {
    allocationCount: topMetricsAllocations.length,
    latestActivityAt,
    totalUsd,
  };
}

function computeOverallSummary(
  allocations: Allocation[],
): UseAllocationSummariesResult['overallSummary'] {
  if (allocations.length === 0) {
    return null;
  }

  return {
    allocationCount: allocations.length,
    totalUsd: allocations.reduce(
      (sum, allocation) => sum + (rowExposureUsd(allocation) ?? 0),
      0,
    ),
  };
}

function computeDebtFields(
  showsReferenceNow: boolean,
  referenceDebt: PrimeDebtBucket | null,
  primeDebtSnapshot: PrimeDebtSnapshot | null,
): Pick<
  UseAllocationSummariesResult,
  'debtWad' | 'debtObservedAt' | 'debtTimestampLabel' | 'debtIlkLabel'
> {
  const debtWad = showsReferenceNow
    ? referenceDebt?.debt_wad
    : primeDebtSnapshot?.debt_wad;
  // Reference mode has no observation time: upstream publishes one figure per
  // prime per day, so the closest thing is the bucket the figure falls in. The
  // label says "as of" rather than "sync" so a boundary is not read as a
  // moment we observed the value.
  const debtObservedAt = showsReferenceNow
    ? referenceDebt?.bucket_start
    : primeDebtSnapshot?.synced_at;
  // "as of" either way: reference mode has only a daily bucket boundary, and
  // even the on-chain snapshot is a sync time rather than the block's own.
  const debtTimestampLabel = 'Debt as of';
  // Only reference mode lacks an ilk, but the label keys off its absence rather
  // than off the mode — an unknown ilk in either mode reads the same.
  const debtIlkLabel = primeDebtSnapshot?.ilk_name
    ? `Ilk ${primeDebtSnapshot.ilk_name}`
    : null;

  return { debtWad, debtObservedAt, debtTimestampLabel, debtIlkLabel };
}

// Absence has a cause worth naming: the ratio is required-over-total risk
// capital, so it cannot be computed without a total. And where chains go
// unserved the numerator is bounded, making the ratio a floor rather than a
// measurement — on a risk surface that difference matters.
// The band itself renders as a chip beside the value; this line carries only
// what the chip cannot say — why a figure is absent, or that a bounded
// numerator makes the ratio a floor rather than a measurement.
function encumbranceCaptionFor(
  encumbranceRatio: number | null,
  unservedChains: string[],
): string | null {
  if (encumbranceRatio === null) {
    return 'Needs total risk capital, which is not yet observed';
  }
  if (unservedChains.length > 0) {
    return `A floor: ${unservedChains.length} chain${unservedChains.length === 1 ? '' : 's'} unserved`;
  }
  return null;
}

function computeEncumbranceFields(
  riskCapital: PrimeRiskCapital | null,
): Pick<
  UseAllocationSummariesResult,
  'encumbranceRatio' | 'encumbranceBreach' | 'encumbranceCaption'
> {
  // One call decides the ratio for the card, its severity, its caption and the
  // chart's fallback value, so they cannot end up describing different
  // provenances — a Sky figure over a breach threshold beside STL's "within the
  // 100% breach level" would read as a bug in the threshold.
  const skyEncumbranceRatio = riskCapital?.reference_prime_encumbrance_ratio;
  const encumbranceRatio = parseNumericValue(
    preferReference(skyEncumbranceRatio, riskCapital?.prime_encumbrance_ratio),
  );
  const encumbranceBreach = encumbranceSeverity(encumbranceRatio);
  // Only STL's ratio is bounded by the chains STL does not serve. Sky's covers
  // whatever it covers, so the "at least this" caption does not apply to it.
  const unservedChains =
    skyEncumbranceRatio == null
      ? (riskCapital?.prime_unserved_chains ?? [])
      : [];

  return {
    encumbranceRatio,
    encumbranceBreach,
    encumbranceCaption: encumbranceCaptionFor(encumbranceRatio, unservedChains),
  };
}

/**
 * The top-of-grid summary figures: the visible/overall allocation totals, the
 * prime's debt (from whichever provenance is on screen), the encumbrance
 * ratio derived from risk capital, and whether the top metrics band has
 * anything to show yet. None of this reads or writes the grid rows
 * themselves — it is what the header and `MetricsBand` show above the table.
 */
export function useAllocationSummaries(
  {
    referenceDebt,
    primeDebtSnapshot,
    topMetricsAllocations,
    areAllocationsSettled,
    allocations,
    riskCapital,
    isRiskCapitalLoading,
    selectedPrime,
  }: UseAllocationSummariesArgs,
  showsReferenceNow: boolean,
): UseAllocationSummariesResult {
  const summary = useMemo(
    () =>
      computeAllocationSummary(topMetricsAllocations, areAllocationsSettled),
    [topMetricsAllocations, areAllocationsSettled],
  );

  const overallSummary = useMemo(
    () => computeOverallSummary(allocations),
    [allocations],
  );

  // Includes the window before a prime is resolved: the page always picks one,
  // so an empty band there is the same "still arriving" state as a prime whose
  // figures are in flight, not a page waiting on the reader.
  const showTopMetricsSkeleton = !areAllocationsSettled || isRiskCapitalLoading;

  const hasTopMetrics =
    riskCapital !== null ||
    summary !== null ||
    selectedPrime !== null ||
    !areAllocationsSettled;

  return {
    summary,
    overallSummary,
    ...computeDebtFields(showsReferenceNow, referenceDebt, primeDebtSnapshot),
    ...computeEncumbranceFields(riskCapital),
    showTopMetricsSkeleton,
    hasTopMetrics,
  };
}
