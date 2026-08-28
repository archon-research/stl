import { useQuery } from '@tanstack/react-query';

import { toQueryErrorMessage } from '../lib/errors';
import {
  activitySeriesQuery,
  debtSeriesQuery,
  exposureSeriesQuery,
  totalCapitalSeriesQuery,
} from '../lib/queries';
import type {
  AllocationActivityBucket,
  ExposureBucket,
  PrimeDebtBucket,
  TimeSeriesResolution,
  TotalCapitalBucket,
} from '../types/allocation';

export interface PrimeChartData {
  debtBuckets: PrimeDebtBucket[];
  activityBuckets: AllocationActivityBucket[];
  totalCapitalBuckets: TotalCapitalBucket[];
  exposureBuckets: ExposureBucket[];
  isLoading: boolean;
  // Set only for the prime-debt chart, which is the primary series; the
  // activity, total-capital and exposure series are supplementary and degrade
  // to their current-value fallbacks on failure rather than surfacing an error.
  errorMessage: string | null;
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
  fromTimestamp: string | undefined,
  toTimestamp: string | undefined,
  resolution: TimeSeriesResolution,
): PrimeChartData {
  // `enabled` is the whole gate; the empty id only ever reaches the key of a
  // query that will not run.
  const enabled = primeId !== null;
  const forPrime = primeId ?? '';
  const window = { fromTimestamp, toTimestamp, resolution };

  const debt = useQuery({ ...debtSeriesQuery(forPrime, window), enabled });
  const activity = useQuery({
    ...activitySeriesQuery(forPrime, window),
    enabled,
  });
  const totalCapital = useQuery({
    ...totalCapitalSeriesQuery(forPrime, window),
    enabled,
  });
  const exposure = useQuery({
    ...exposureSeriesQuery(forPrime, window),
    enabled,
  });

  return {
    debtBuckets: debt.data ?? NO_DEBT,
    activityBuckets: activity.data ?? NO_ACTIVITY,
    totalCapitalBuckets: totalCapital.data ?? NO_CAPITAL,
    exposureBuckets: exposure.data ?? NO_EXPOSURE,
    // Exposure is deliberately not one of these: it feeds a single card that
    // already falls back on its own, and waiting on it would hold the other
    // three in a skeleton behind the slowest request on the screen.
    isLoading:
      enabled &&
      (debt.isPending || activity.isPending || totalCapital.isPending),
    errorMessage: toQueryErrorMessage(debt.error),
  };
}
