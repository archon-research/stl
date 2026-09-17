import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import type { PrimeGroup } from '../lib/dashboard';
import { toQueryErrorMessage } from '../lib/errors';
import { narrowAllocations, useProvenanceView } from '../lib/provenance';
import { allocationsQuery } from '../lib/queries';
import type { Allocation } from '../types/allocation';

export type AllocationRows = {
  allocations: Allocation[];
  errorMessage: string | null;
  isLoading: boolean;
  // The rows are this prime's and the fetch has finished. Narrower than
  // `!isLoading`, which is also false before a fetch starts.
  isLoaded: boolean;
};

// A literal `?? []` would hand the memo below a fresh array on every render,
// which is the identity it compares on.
const NO_ALLOCATIONS: Allocation[] = [];

/**
 * A prime's allocation rows, narrowed to the provenance on screen.
 *
 * One request: the endpoint answers whole-prime, so the per-proxy fan-out this
 * hook used to fold — and the double-count it had to avoid under
 * `source=reference` — are the server's business now.
 *
 * Called for the selected prime by the allocation view and the shell's filter
 * options, and for every prime by the sidebar's network count. A repeat call for
 * a prime already fetched issues no request — react-query serves it from the
 * cache — but it is another observer, so the narrowing below does run again.
 */
export function useAllocationRows(
  primeGroup: PrimeGroup | null,
): AllocationRows {
  // What is on screen, which is not always what was fetched: narrowing a
  // composite response changes this without a request.
  const { provenance: shownProvenance } = useProvenanceView();

  const fetched = useQuery({
    ...allocationsQuery(primeGroup?.primeId ?? ''),
    enabled: primeGroup !== null,
  });

  // What was fetched, narrowed to what is being shown. A composite response
  // holds both provenances, so switching between them is this projection rather
  // than a request — and doing it here, once, is what keeps the table, the
  // cards, the charts and the drawer from disagreeing about which they show.
  const allocations = useMemo(
    () => narrowAllocations(shownProvenance, fetched.data ?? NO_ALLOCATIONS),
    [shownProvenance, fetched.data],
  );

  return {
    allocations,
    errorMessage: toQueryErrorMessage(fetched.error),
    isLoading: fetched.isPending && primeGroup !== null,
    // Whether the rows on screen are this prime's, settled. An empty list from
    // a query that has not answered would otherwise read as an answer.
    isLoaded: fetched.isSuccess,
  };
}
