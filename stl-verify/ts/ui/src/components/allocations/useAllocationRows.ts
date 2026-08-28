import { useQueries, type UseQueryResult } from '@tanstack/react-query';
import { useMemo } from 'react';

import type { PrimeGroup } from '../../lib/dashboard';
import { toQueryErrorMessage } from '../../lib/errors';
import {
  narrowAllocations,
  showsReference,
  useProvenanceView,
} from '../../lib/provenance';
import { allocationsQuery } from '../../lib/queries';
import type { Allocation } from '../../types/allocation';

export type AllocationRows = {
  allocations: Allocation[];
  errorMessage: string | null;
  isLoading: boolean;
  // The rows are this prime's and the fetch has finished. Narrower than
  // `!isLoading`, which is also false before a fetch starts.
  isLoaded: boolean;
};

// Shared fallbacks for a query that has not answered yet: a literal `?? []`
// would hand the memo below a fresh array on every render, which is the
// identity it compares on.
const NO_PROXIES: string[] = [];
const NO_ALLOCATIONS: Allocation[] = [];

/**
 * Folds the per-proxy allocation queries into the one list the screen reads.
 *
 * A failure on any single proxy blanks the whole set rather than quietly
 * showing a prime that is missing a chain. Declared at module scope because
 * react-query only memoises a combined result while the `combine` reference
 * holds still.
 */
function combineAllocations(results: readonly UseQueryResult<Allocation[]>[]) {
  const failed = results.find((result) => result.error !== null);

  return {
    allocations: failed
      ? NO_ALLOCATIONS
      : results.flatMap((result) => result.data ?? NO_ALLOCATIONS),
    errorMessage: toQueryErrorMessage(failed?.error),
    isLoading: results.some((result) => result.isPending),
    // Whether the rows on screen are this prime's, settled. An empty list from
    // a query that has not answered would otherwise read as an answer.
    isLoaded: results.length > 0 && results.every((result) => result.isSuccess),
  };
}

/**
 * The selected prime's allocation rows, narrowed to the provenance on screen.
 *
 * Called from both the allocation view and the shell's filter options, which is
 * a cache read rather than a second fan-out.
 */
export function useAllocationRows(
  primeGroup: PrimeGroup | null,
): AllocationRows {
  // What is on screen, which is not always what was fetched: narrowing a
  // composite response changes this without a request.
  const { provenance: shownProvenance } = useProvenanceView();

  const allocationProxies = primeGroup?.proxyAddresses ?? NO_PROXIES;

  // One call for anything the server answers prime-wide: reference rows are
  // prime-scoped, and the merged view resolves the prime's proxies itself.
  // Fanning either out would show each position once per chain — exactly the
  // double-count the `scope` field warns about. Otherwise it is one query per
  // proxy, so a chain's rows cache on their own and returning to a prime is free.
  const queriedProxies = showsReference
    ? allocationProxies.slice(0, 1)
    : allocationProxies;

  const fetched = useQueries({
    queries: queriedProxies.map((proxyAddress) =>
      allocationsQuery(proxyAddress),
    ),
    combine: combineAllocations,
  });

  // What was fetched, narrowed to what is being shown. A composite response
  // holds both provenances, so switching between them is this projection rather
  // than a request — and doing it here, once, is what keeps the table, the
  // cards, the charts and the drawer from disagreeing about which they show.
  const allocations = useMemo(
    () => narrowAllocations(shownProvenance, fetched.allocations),
    [shownProvenance, fetched.allocations],
  );

  return {
    allocations,
    errorMessage: fetched.errorMessage,
    isLoading: fetched.isLoading,
    isLoaded: fetched.isLoaded,
  };
}
