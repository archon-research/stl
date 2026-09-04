import {
  DataTable,
  type DataTableProps,
  EmptyState,
  ErrorState,
  type SkeletonColumnHint,
} from '@archon-research/design-system';

import { css } from '#styled-system/css';

import { getAllocationKey } from '../../shared/lib/dashboard';
import type { Allocation, Prime } from '../../shared/types/allocation';
import { tableHeaderTypographyClassName } from '../../shared/ui';
import type { AllocationGridRow } from './allocationGridRows';

type AllocationGridTableProps = {
  errorMessage: string | null;
  selectedPrime: Prime | null;
  areAllocationsSettled: boolean;
  allocations: Allocation[];
  visibleAllocations: Allocation[];
  onAllocationIntent?: () => void;
  table: DataTableProps<AllocationGridRow>['table'];
  onSelectAllocation: (allocationKey: string) => void;
  selectedAllocationKey: string | null;
  skeletonColumnHints: SkeletonColumnHint[];
};

export function AllocationGridTable({
  errorMessage,
  selectedPrime,
  areAllocationsSettled,
  allocations,
  visibleAllocations,
  onAllocationIntent,
  table,
  onSelectAllocation,
  selectedAllocationKey,
  skeletonColumnHints,
}: AllocationGridTableProps) {
  return (
    <div className={css({ mt: '6' })}>
      {selectedPrime && errorMessage ? (
        <ErrorState
          title="Unable to load allocations"
          description="An error occurred while fetching allocation data."
          errorMessage={errorMessage}
          tone="critical"
          size="inline"
        />
      ) : null}

      {areAllocationsSettled && !errorMessage && allocations.length === 0 ? (
        <EmptyState
          title="No allocations returned"
          description="The selected prime did not return any allocation rows from the API."
          stretch
        />
      ) : null}

      {areAllocationsSettled &&
      !errorMessage &&
      allocations.length > 0 &&
      visibleAllocations.length === 0 ? (
        <EmptyState
          title="No rows match the active filters"
          description="Clear the category or search filter above the grid, or one of the filters in the top bar, to restore the allocation grid."
          stretch
        />
      ) : null}

      {!errorMessage &&
      (!areAllocationsSettled || visibleAllocations.length > 0) ? (
        <div
          className={tableHeaderTypographyClassName}
          onMouseEnter={onAllocationIntent}
        >
          <DataTable
            table={table}
            isLoading={!areAllocationsSettled}
            onRowClick={(allocation) =>
              onSelectAllocation(getAllocationKey(allocation))
            }
            getRowKey={getAllocationKey}
            selectedRowKey={selectedAllocationKey}
            density="compact"
            // A prime's positions are returned in full — no server-side limit
            // — so this is the one grid whose row count nothing bounds.
            virtualized
            // Proportional, not `calc(100dvh - chrome)`: the metric band
            // above wraps with width, so the chrome measures 656px at 1920
            // and 1081px at 1280 and no fixed subtraction is right at both.
            // The design system's 640px default strands ~300px of an
            // ultra-tall display; 40rem is that default as a floor.
            maxHeight="max(40rem, 70dvh)"
            // Six nowrap columns push min-content well past this, so it binds
            // only on the loading skeleton, which has no intrinsic width.
            minWidth="48rem"
            // No `firstColumnTall`: the identity hint owns that cell's height.
            skeletonConfig={{ rows: 8, columnHints: skeletonColumnHints }}
          />
        </div>
      ) : null}
    </div>
  );
}
