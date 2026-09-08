import { useQuery } from '@tanstack/react-query';

import { toQueryErrorMessage } from '../../shared/lib/errors';
import {
  activitySeriesQuery,
  debtSeriesQuery,
  exposureSeriesQuery,
  type SeriesWindow,
  totalCapitalSeriesQuery,
} from '../../shared/lib/queries';
import type {
  AllocationActivityBucket,
  ExposureBucket,
  PrimeDebtBucket,
  TotalCapitalBucket,
} from '../../shared/types/allocation';

export interface PrimeChartData {
  debtBuckets: PrimeDebtBucket[];
  activityBuckets: AllocationActivityBucket[];
  totalCapitalBuckets: TotalCapitalBucket[];
  exposureBuckets: ExposureBucket[];
  isLoading: boolean;
  // Set only for the prime-debt chart, which is the primary series; the
  // total-capital and exposure series are supplementary and degrade to their
  // current-value fallbacks on failure rather than surfacing an error.
  errorMessage: string | null;
  // Its own channel because the activity card has no current-value fallback to
  // degrade to: an empty series there is an empty state, so a failed read that
  // travelled as one told the reader the prime had been quiet.
  activityErrorMessage: string | null;
}

const NO_DEBT: PrimeDebtBucket[] = [];
const NO_ACTIVITY: AllocationActivityBucket[] = [];
const NO_CAPITAL: TotalCapitalBucket[] = [];
const NO_EXPOSURE: ExposureBucket[] = [];

/**
 * Loads the four per-prime time series backing the metric trend charts (prime
 * debt, allocation activity, total capital, exposure) for a given range and
 * resolution. Each is its own query, so a supplementary failure does not blank
 * the whole view and a range already looked at comes back from the cache.
 */
export function usePrimeChartData(
  primeId: string | null,
  range: SeriesWindow,
): PrimeChartData {
  // `enabled` is the whole gate; the empty id only ever reaches the key of a
  // query that will not run.
  const enabled = Boolean(primeId);
  const forPrime = primeId ?? '';

  const debt = useQuery({ ...debtSeriesQuery(forPrime, range), enabled });
  const activity = useQuery({
    ...activitySeriesQuery(forPrime, range),
    enabled,
  });
  const totalCapital = useQuery({
    ...totalCapitalSeriesQuery(forPrime, range),
    enabled,
  });
  const exposure = useQuery({
    ...exposureSeriesQuery(forPrime, range),
    enabled,
  });

  return {
    debtBuckets: debt.data ?? NO_DEBT,
    activityBuckets: activity.data ?? NO_ACTIVITY,
    totalCapitalBuckets: totalCapital.data ?? NO_CAPITAL,
    exposureBuckets: exposure.data ?? NO_EXPOSURE,
    // The other three settle together so no card flashes its fallback while a
    // sibling is still in flight. Exposure is deliberately outside that: it
    // feeds one card that already degrades on its own.
    isLoading:
      enabled &&
      (debt.isPending || activity.isPending || totalCapital.isPending),
    errorMessage: toQueryErrorMessage(debt.error),
    activityErrorMessage: toQueryErrorMessage(activity.error),
  };
}
