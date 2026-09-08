import { useMemo } from 'react';

import { parseNumericValue } from '../../shared/lib/dashboard';
import type { Allocation } from '../../shared/types/allocation';

/**
 * The anchor the reconstructed balance series is walked back from. Two bases
 * must line up with the flows driving the reconstruction:
 *
 *   - Scope: activity buckets are fetched per-prime (no network/protocol/search
 *     filter), so the anchor is the whole-prime total; anchoring on a filtered
 *     subset while subtracting whole-prime flows would be wrong. The chart is
 *     therefore intentionally unaffected by the table filters.
 *   - Valuation: net_flow_usd values both receipt-token and direct-asset flows,
 *     so the anchor sums amount_usd across all allocations (receipt positions
 *     and direct holdings alike) rather than receipt positions only.
 */
export function usePrimeTotalAllocationUsd(allocations: Allocation[]): number {
  return useMemo(
    () =>
      allocations.reduce((sum, allocation) => {
        const numericAmount = parseNumericValue(allocation.amount_usd);
        return numericAmount === null ? sum : sum + numericAmount;
      }, 0),
    [allocations],
  );
}
