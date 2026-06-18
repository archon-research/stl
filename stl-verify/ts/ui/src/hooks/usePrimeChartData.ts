import { useEffect, useState } from 'react';

import {
  getAllocationActivityEnvelope,
  getPrimeDebtEnvelope,
  getTotalCapitalEnvelope,
} from '../lib/api';
import { sortByBucketStart } from '../lib/dashboard';
import { isAbortError, toErrorMessage } from '../lib/errors';
import { logging } from '../lib/logging';
import type {
  AllocationActivityBucket,
  PrimeDebtBucket,
  TimeSeriesResolution,
  TotalCapitalBucket,
} from '../types/allocation';

export interface PrimeChartData {
  debtBuckets: PrimeDebtBucket[];
  activityBuckets: AllocationActivityBucket[];
  totalCapitalBuckets: TotalCapitalBucket[];
  isLoading: boolean;
  // Set only for the prime-debt chart, which is the primary series; the
  // activity and total-capital series are supplementary and degrade to their
  // current-value fallbacks on failure rather than surfacing an error.
  errorMessage: string | null;
}

const EMPTY: PrimeChartData = {
  debtBuckets: [],
  activityBuckets: [],
  totalCapitalBuckets: [],
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
  const [totalCapitalBuckets, setTotalCapitalBuckets] = useState<
    TotalCapitalBucket[]
  >([]);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!primeId) {
      setDebtBuckets([]);
      setActivityBuckets([]);
      setTotalCapitalBuckets([]);
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

    const debtRequest = getPrimeDebtEnvelope(
      primeId,
      bucketFilters,
      controller.signal,
    )
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
      });

    // Supplementary series degrade independently: on failure the card falls
    // back to its current value (cleared buckets + a warn) rather than
    // surfacing an error and blanking the view like the primary debt series.
    const loadSupplementarySeries = <T extends { bucket_start: string }>(
      buckets: Promise<T[]>,
      setBuckets: (next: T[]) => void,
      unavailableMessage: string,
    ): Promise<void> =>
      buckets
        .then((next) => setBuckets(sortByBucketStart(next)))
        .catch((error: unknown) => {
          if (isAbortError(error)) {
            return;
          }

          logging.warn(unavailableMessage, { error, primeId });
          setBuckets([]);
        });

    // Allocation activity drives the reconstructed total-allocation balance
    // series. A non-aggregated envelope means "no data", so coerce to empty.
    const activityRequest = loadSupplementarySeries(
      getAllocationActivityEnvelope(
        { prime_id: primeId, ...bucketFilters },
        controller.signal,
      ).then((activityEnvelope) =>
        activityEnvelope.mode === 'aggregated'
          ? (activityEnvelope.data as AllocationActivityBucket[])
          : [],
      ),
      setActivityBuckets,
      'Allocation activity history unavailable; using current value',
    );

    // Total-capital history drives the total-capital chart from the on-chain
    // SubProxy treasury balance.
    const capitalRequest = loadSupplementarySeries(
      getTotalCapitalEnvelope(primeId, bucketFilters, controller.signal).then(
        (capitalEnvelope) => capitalEnvelope.data as TotalCapitalBucket[],
      ),
      setTotalCapitalBuckets,
      'Total capital history unavailable; using current value',
    );

    // Clear loading only once all three series settle, so a card never flashes
    // its empty/fallback state while a slower supplementary request is still in
    // flight.
    void Promise.allSettled([
      debtRequest,
      activityRequest,
      capitalRequest,
    ]).then(() => {
      if (!controller.signal.aborted) {
        setIsLoading(false);
      }
    });

    return () => controller.abort();
  }, [primeId, fromTimestamp, toTimestamp, resolution]);

  if (!primeId) {
    return EMPTY;
  }

  return {
    debtBuckets,
    activityBuckets,
    totalCapitalBuckets,
    isLoading,
    errorMessage,
  };
}
