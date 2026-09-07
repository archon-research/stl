import {
  AsyncStateRenderer,
  DataTable,
  type DataTableProps,
  EmptyState,
  ErrorState,
  SkeletonStack,
} from '@archon-research/design-system';

import { css } from '#styled-system/css';

import type { AllocationActivity } from '../../shared/types/allocation';
import { tableHeaderTypographyClassName } from '../../shared/ui';
import { getRealTxHash } from './activityColumns';
import { TxProtocolEventsPanel } from './ProtocolEventsPanel';

type ActivityTableProps = {
  table: DataTableProps<AllocationActivity>['table'];
  isLoading: boolean;
  visibleEventCount: number;
  rowLimit: number;
  emptyDescription: string;
};

function ActivityTable({
  table,
  isLoading,
  visibleEventCount,
  rowLimit,
  emptyDescription,
}: ActivityTableProps) {
  return (
    <div className={css({ display: 'grid', gap: '2' })}>
      {visibleEventCount === 0 ? (
        <EmptyState
          title="No Activity Found"
          description={emptyDescription}
          stretch
        />
      ) : (
        <div className={tableHeaderTypographyClassName}>
          <DataTable
            table={table}
            isLoading={isLoading}
            density="compact"
            // Safe alongside the detail panel: each row is its own <tbody>, so
            // the virtualizer measures a row and its open panel as one unit.
            virtualized
            // Matches the allocation grid: see the note in
            // AllocationGridTable.tsx for why this is proportional rather
            // than a subtraction from the viewport.
            maxHeight="max(40rem, 70dvh)"
            renderDetailPanel={(event) => {
              const txHash = getRealTxHash(event);
              return txHash === null ? null : (
                <TxProtocolEventsPanel txHash={txHash} />
              );
            }}
          />
        </div>
      )}

      <div
        className={css({
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: '3',
          px: '1',
          fontSize: 'xs',
          color: 'text.default',
        })}
      >
        <span>Showing {visibleEventCount} events</span>
        {visibleEventCount >= rowLimit ? (
          <span className={css({ color: 'text.muted' })}>
            Limited to most recent {rowLimit}
          </span>
        ) : null}
      </div>
    </div>
  );
}

type ActivityResultsProps = ActivityTableProps & {
  error: string | null;
  // Rows fetched before the search filter narrows them: the skeleton shows
  // only while nothing is on screen, so a refetch over rows already fetched
  // leaves them up. A filter change is a new query key with no rows of its
  // own, so that one does show the skeleton.
  totalEventCount: number;
  emptyDescription: string;
};

export function ActivityResults({
  table,
  isLoading,
  error,
  totalEventCount,
  visibleEventCount,
  rowLimit,
  emptyDescription,
}: ActivityResultsProps) {
  return (
    <AsyncStateRenderer
      isLoading={isLoading && totalEventCount === 0}
      error={error}
      isEmpty={false}
      loadingView={<SkeletonStack count={3} />}
      errorView={
        <ErrorState
          title="Error Loading Activity"
          description="An error occurred while loading the activity feed."
          errorMessage={error ?? undefined}
          tone="critical"
          size="inline"
        />
      }
      emptyView={
        <EmptyState
          title="No Activity Found"
          description={emptyDescription}
          stretch
        />
      }
    >
      <ActivityTable
        table={table}
        isLoading={isLoading}
        visibleEventCount={visibleEventCount}
        rowLimit={rowLimit}
        emptyDescription={emptyDescription}
      />
    </AsyncStateRenderer>
  );
}
