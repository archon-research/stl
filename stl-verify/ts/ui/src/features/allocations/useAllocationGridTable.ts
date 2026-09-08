import {
  type ColumnDef,
  type DataTableConfig,
  type DataTableProps,
  type SkeletonColumnHint,
  type SortingState,
  useDataTable,
} from '@archon-research/design-system';
import { useMemo } from 'react';

import { getAllocationKey } from '../../shared/lib/dashboard';
import type {
  Allocation,
  AllocationRiskCapital,
  Prime,
  PrimeRiskCapital,
} from '../../shared/types/allocation';
import { createAllocationColumns } from './allocationGridColumns';
import {
  toAllocationGridRow,
  withRrcShare,
  type AllocationGridRow,
  type RiskFetchState,
} from './allocationGridRows';
import type { UseAllocationGridArgs } from './useAllocationGrid';

export type UseAllocationGridTableArgs = Pick<
  UseAllocationGridArgs,
  | 'riskCapital'
  | 'isRiskCapitalLoading'
  | 'riskCapitalErrorMessage'
  | 'selectedPrime'
  | 'chainLabels'
  | 'localProtocols'
  | 'onSortingChange'
  | 'sorting'
>;

// `table`'s type (`Table<AllocationGridRow>`) isn't exported from
// `@archon-research/design-system`'s public surface, so an inferred return
// type for this hook can't be printed into `ui`'s declaration output ("cannot
// be named without a reference to 'Table'"). Routing it through
// `DataTableProps<T>['table']` — a type the package does export — is the only
// portable spelling, and it only holds if this return type is explicit: an
// inferred one still gets normalized down to the same unnameable `Table<T>`
// before TS checks it for portability.
export type UseAllocationGridTableResult = {
  skeletonColumnHints: SkeletonColumnHint[];
  table: DataTableProps<AllocationGridRow>['table'];
};

// Under every key the row answers to, so a grid row matching on any one of
// its own finds it. First writer wins: the strongest key is listed first, so
// a weaker one cannot displace it.
function buildRiskByPositionKey(
  riskCapital: PrimeRiskCapital | null,
): Map<string, AllocationRiskCapital> {
  const map = new Map<string, AllocationRiskCapital>();
  for (const entry of riskCapital?.per_allocation ?? []) {
    for (const key of entry.position_keys ?? []) {
      if (!map.has(key)) map.set(key, entry);
    }
  }
  return map;
}

function deriveRiskFetchState(
  riskCapital: PrimeRiskCapital | null,
  isRiskCapitalLoading: boolean,
  riskCapitalErrorMessage: string | null,
): RiskFetchState {
  if (riskCapital !== null) return 'ready';
  if (isRiskCapitalLoading) return 'loading';
  if (riskCapitalErrorMessage !== null) return 'error';
  return 'ready';
}

function buildGridRows(
  visibleAllocations: Allocation[],
  riskByPositionKey: Map<string, AllocationRiskCapital>,
  riskFetchState: RiskFetchState,
  selectedPrime: Prime | null,
): AllocationGridRow[] {
  return withRrcShare(
    visibleAllocations.map((allocation) =>
      toAllocationGridRow(
        allocation,
        riskByPositionKey,
        riskFetchState,
        selectedPrime,
      ),
    ),
  );
}

// Explicit hints replace DataTable's meta-derived ones wholesale, so they are
// read off the same column defs rather than restated: only the leading Asset
// cell needs a shape `meta` cannot express (a symbol over its protocol line).
function buildSkeletonColumnHints(
  columns: ColumnDef<AllocationGridRow>[],
): SkeletonColumnHint[] {
  return columns.map((column, index) => {
    if (index === 0) return { kind: 'identity' };
    return column.meta?.align === 'right'
      ? { kind: 'numeric' }
      : { kind: 'text' };
  });
}

function buildDataTableConfig(
  onSortingChange: UseAllocationGridTableArgs['onSortingChange'],
  sorting: SortingState,
): DataTableConfig<AllocationGridRow> {
  return {
    enableSorting: true,
    onSortingChange,
    sorting,
    // The same identity the grid already selects rows by. Virtualization
    // keys its measurement cache off it, which a sort would otherwise
    // scramble.
    getRowId: getAllocationKey,
  };
}

/**
 * The grid rows themselves: risk figures joined onto each allocation, the
 * column defs, and the `useDataTable` instance the `DataTable` renders.
 */
export function useAllocationGridTable(
  {
    riskCapital,
    isRiskCapitalLoading,
    riskCapitalErrorMessage,
    selectedPrime,
    chainLabels,
    localProtocols,
    onSortingChange,
    sorting,
  }: UseAllocationGridTableArgs,
  visibleAllocations: Allocation[],
): UseAllocationGridTableResult {
  const riskByPositionKey = useMemo(
    () => buildRiskByPositionKey(riskCapital),
    [riskCapital],
  );

  // A new array when risk data lands, deliberately: see AllocationGridRow.
  const riskFetchState = deriveRiskFetchState(
    riskCapital,
    isRiskCapitalLoading,
    riskCapitalErrorMessage,
  );

  const gridRows = useMemo<AllocationGridRow[]>(
    () =>
      buildGridRows(
        visibleAllocations,
        riskByPositionKey,
        riskFetchState,
        selectedPrime,
      ),
    [visibleAllocations, riskByPositionKey, riskFetchState, selectedPrime],
  );

  const columns = useMemo<ColumnDef<AllocationGridRow>[]>(
    () => createAllocationColumns(chainLabels, localProtocols),
    [chainLabels, localProtocols],
  );

  const skeletonColumnHints = useMemo<SkeletonColumnHint[]>(
    () => buildSkeletonColumnHints(columns),
    [columns],
  );

  const table: DataTableProps<AllocationGridRow>['table'] = useDataTable(
    gridRows,
    columns,
    buildDataTableConfig(onSortingChange, sorting),
  );

  return { skeletonColumnHints, table };
}
