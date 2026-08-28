import { useMemo } from 'react';

import { getAllocationKey } from '../../lib/dashboard';
import type { Allocation } from '../../types/allocation';

export type AllocationSelection = {
  selectedAllocation: Allocation | null;
  selectedAllocationKey: string | null;
  isDrawerOpen: boolean;
};

/**
 * Which row the drawer is about, and whether it is showing.
 *
 * `requestedRow` restores a drawer deep link; anything the current filters
 * exclude falls back to the first row in view, so a tab always has something to
 * render.
 */
export function useAllocationSelection(
  filteredAllocations: Allocation[],
  requestedRow: string | undefined,
  isDrawerRequested: boolean,
): AllocationSelection {
  const selectedAllocation = useMemo(() => {
    const requested = requestedRow
      ? filteredAllocations.find(
          (allocation) => getAllocationKey(allocation) === requestedRow,
        )
      : undefined;

    return requested ?? filteredAllocations[0] ?? null;
  }, [filteredAllocations, requestedRow]);

  return {
    selectedAllocation,
    selectedAllocationKey: selectedAllocation
      ? getAllocationKey(selectedAllocation)
      : null,
    // Derived, never corrected: a deep link names its row before the
    // allocations are fetched, so `drawer=1` waits for a row instead of being
    // dropped as stale.
    isDrawerOpen: isDrawerRequested && selectedAllocation !== null,
  };
}
