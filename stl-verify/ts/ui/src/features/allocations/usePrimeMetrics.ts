import type { UseQueryResult } from '@tanstack/react-query';
import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef } from 'react';

import { toQueryErrorMessage } from '../../shared/lib/errors';
import {
  narrowRiskCapital,
  showsReference,
  useProvenanceView,
} from '../../shared/lib/provenance';
import {
  latestDebtSnapshotQuery,
  latestReferenceDebtQuery,
  riskCapitalQuery,
} from '../../shared/lib/queries';
import type {
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
} from '../../shared/types/allocation';

export type PrimeMetrics = {
  riskCapital: PrimeRiskCapital | null;
  isRiskCapitalLoading: boolean;
  riskCapitalErrorMessage: string | null;
  referenceDebt: PrimeDebtBucket | null;
  primeDebtSnapshot: PrimeDebtSnapshot | null;
  isPrimeDebtLoading: boolean;
  primeDebtErrorMessage: string | null;
};

/**
 * The prime's headline figures: risk capital, and its debt from whichever
 * provenance the session fetched.
 *
 * Both reads are prime-scoped snapshots with no range of their own, so
 * `enabled` is the whole gate, and the empty address only ever reaches the key
 * of a query that will not run. Falsy rather than nullish, as the effects these
 * replaced were: an empty address still builds a plausible request path.
 */
/**
 * The range selector, restored as a retry gesture.
 *
 * These snapshots are point-in-time and carry no range in their key, so
 * changing the range cannot refetch them the way the effects they replaced
 * did — which left a failed card needing a page reload. Retried only while a
 * read has nothing cached to show.
 *
 * The `data === undefined` guard is deliberate and not the bug it looks like:
 * widening it would let a control none of these reads depend on refire a dozen
 * queries at once, every time the range moves. A read that failed *over* a
 * cached figure is reported instead — `MetricCardCell` marks that card stale
 * rather than letting it pass its old number off as current.
 */
function useRetryEmptyOn(
  signal: string | undefined,
  reads: readonly UseQueryResult<unknown, unknown>[],
): void {
  const lastSignal = useRef(signal);

  useEffect(() => {
    if (lastSignal.current === signal) {
      return;
    }
    lastSignal.current = signal;

    for (const read of reads) {
      if (read.isError && read.data === undefined) {
        void read.refetch();
      }
    }
  }, [signal, reads]);
}

export function usePrimeMetrics(
  primaryProxyAddress: string | null,
  rangeSignal?: string,
): PrimeMetrics {
  const { provenance: shownProvenance } = useProvenanceView();
  const isPrimeSelected = Boolean(primaryProxyAddress);
  const forPrime = primaryProxyAddress ?? '';

  const riskCapitalResult = useQuery({
    ...riskCapitalQuery(forPrime),
    enabled: isPrimeSelected,
  });
  const fetchedRiskCapital = riskCapitalResult.data ?? null;

  const riskCapital = useMemo(
    () => narrowRiskCapital(shownProvenance, fetchedRiskCapital),
    [shownProvenance, fetchedRiskCapital],
  );

  // `showsReference` is fixed for the session, so exactly one of these ever
  // runs — but both hooks are called, which is what keeps the order stable.
  const referenceDebtResult = useQuery({
    ...latestReferenceDebtQuery(forPrime),
    enabled: isPrimeSelected && showsReference,
  });
  const debtSnapshotResult = useQuery({
    ...latestDebtSnapshotQuery(forPrime),
    enabled: isPrimeSelected && !showsReference,
  });

  const primeDebtResult = showsReference
    ? referenceDebtResult
    : debtSnapshotResult;

  const reads = useMemo(
    () => [riskCapitalResult, referenceDebtResult, debtSnapshotResult],
    [riskCapitalResult, referenceDebtResult, debtSnapshotResult],
  );
  useRetryEmptyOn(rangeSignal, reads);

  return {
    riskCapital,
    isRiskCapitalLoading: isPrimeSelected && riskCapitalResult.isPending,
    riskCapitalErrorMessage: toQueryErrorMessage(riskCapitalResult.error),
    referenceDebt: referenceDebtResult.data ?? null,
    primeDebtSnapshot: debtSnapshotResult.data ?? null,
    isPrimeDebtLoading: isPrimeSelected && primeDebtResult.isPending,
    primeDebtErrorMessage: toQueryErrorMessage(primeDebtResult.error),
  };
}
