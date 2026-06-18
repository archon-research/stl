import { useEffect, useState } from 'react';

import {
  getAllocationActivityEnvelope,
  getCapitalMetricsEnvelope,
  getPrimeDebtEnvelope,
} from '../lib/api';
import { sortByBucketStart } from '../lib/dashboard';
import { isAbortError, toErrorMessage } from '../lib/errors';
import { logging } from '../lib/logging';
import type {
  AllocationActivityBucket,
  CapitalMetricsBucket,
  PrimeDebtBucket,
  TimeSeriesResolution,
} from '../types/allocation';

export interface PrimeChartData {
  debtBuckets: PrimeDebtBucket[];
  activityBuckets: AllocationActivityBucket[];
  capitalMetricsBuckets: CapitalMetricsBucket[];
  isLoading: boolean;
  // Set only for the prime-debt chart, which is the primary series; the
  // activity and capital-metrics series are supplementary and degrade to their
  // current-value fallbacks on failure rather than surfacing an error.
  errorMessage: string | null;
}

const EMPTY: PrimeChartData = {
  debtBuckets: [],
  activityBuckets: [],
  capitalMetricsBuckets: [],
  isLoading: false,
  errorMessage: null,
};

/**
 * Loads the three per-prime time series backing the metric trend charts
 * (prime debt, allocation activity, capital metrics) for a given range and
 * resolution. Each series is fetched independently so a supplementary failure
 * does not blank the whole view.
 */
export function usePrimeChartData(
  primeId: string | null,
  fromTimestamp: string | undefined,
  toTimestamp: string | undefined,
  resolution: TimeSeriesResolution,
): PrimeChartData {
  const [debtBuckets, setDebtBuckets] = useState<PrimeDebtBucket[]>([]);
  const [activityBuckets, setActivityBuckets] = useState<
    AllocationActivityBucket[]
  >([]);
  const [capitalMetricsBuckets, setCapitalMetricsBuckets] = useState<
    CapitalMetricsBucket[]
  >([]);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!primeId) {
      setDebtBuckets([]);
      setActivityBuckets([]);
      setCapitalMetricsBuckets([]);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();
    setIsLoading(true);
    setErrorMessage(null);

    // limit 500 (the per-prime max) so the longest ranges (e.g. 365d at P1D)
    // return every bucket rather than being truncated to the default page.
    const bucketFilters = {
      from_timestamp: fromTimestamp,
      to_timestamp: toTimestamp,
      resolution,
      aggregate: true,
      limit: 500,
    };

    void getPrimeDebtEnvelope(primeId, bucketFilters, controller.signal)
      .then((debtEnvelope) => {
        // We request aggregate=true, so a non-aggregated envelope is a backend
        // contract violation, not "no data" — surface it instead of silently
        // rendering an empty chart.
        if (debtEnvelope.mode !== 'aggregated') {
          throw new Error(
            `Prime debt envelope returned unexpected mode "${debtEnvelope.mode}" (expected "aggregated")`,
          );
        }

        setDebtBuckets(
          sortByBucketStart(debtEnvelope.data as PrimeDebtBucket[]),
        );
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load chart buckets', {
          error,
          primeId,
          resolution,
        });
        setDebtBuckets([]);
        setErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    // Allocation-activity is supplementary: it drives the reconstructed
    // total-allocation balance series, and on failure that card degrades to the
    // current-value fallback rather than failing the whole view.
    void getAllocationActivityEnvelope(
      { prime_id: primeId, ...bucketFilters },
      controller.signal,
    )
      .then((activityEnvelope) => {
        const nextActivityBuckets =
          activityEnvelope.mode === 'aggregated'
            ? (activityEnvelope.data as AllocationActivityBucket[])
            : [];
        setActivityBuckets(sortByBucketStart(nextActivityBuckets));
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.warn(
          'Allocation activity history unavailable; using current value',
          {
            error,
            primeId,
          },
        );
        setActivityBuckets([]);
      });

    // Capital-metrics history is supplementary: a failure (e.g. the endpoint not
    // yet deployed, or no snapshots ingested) degrades the risk/total-capital
    // charts to their current-value fallback rather than failing the whole view.
    void getCapitalMetricsEnvelope(primeId, bucketFilters, controller.signal)
      .then((capitalEnvelope) => {
        const nextCapitalBuckets =
          capitalEnvelope.mode === 'aggregated'
            ? (capitalEnvelope.data as CapitalMetricsBucket[])
            : [];
        setCapitalMetricsBuckets(sortByBucketStart(nextCapitalBuckets));
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.warn(
          'Capital metrics history unavailable; using current value',
          {
            error,
            primeId,
          },
        );
        setCapitalMetricsBuckets([]);
      });

    return () => controller.abort();
  }, [primeId, fromTimestamp, toTimestamp, resolution]);

  if (!primeId) {
    return EMPTY;
  }

  return {
    debtBuckets,
    activityBuckets,
    capitalMetricsBuckets,
    isLoading,
    errorMessage,
  };
}
