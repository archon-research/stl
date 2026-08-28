import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import { toQueryErrorMessage } from '../../lib/errors';
import {
  narrowRiskCapital,
  showsReference,
  useProvenanceView,
} from '../../lib/provenance';
import {
  latestDebtSnapshotQuery,
  latestReferenceDebtQuery,
  riskCapitalQuery,
} from '../../lib/queries';
import type {
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
} from '../../types/allocation';

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
export function usePrimeMetrics(
  primaryProxyAddress: string | null,
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
