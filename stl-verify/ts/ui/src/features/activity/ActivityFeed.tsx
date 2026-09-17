import { EmptyState } from '@archon-research/design-system';

import { css } from '#styled-system/css';

import type { ChainLabelLookup } from '../../shared/lib/dashboard';
import { PageShell } from '../../shared/ui';
import { unindexedChainMessage } from '../../shared/ui/TabStatePanels';
import { useActivityTable } from './activityColumns';
import { ActivityFilterBar } from './ActivityFilterBar';
import { ActivityPageHeader } from './ActivityPageHeader';
import { ActivityResults } from './ActivityResultsPanel';
import {
  useAllocationActivity,
  type UseAllocationActivityProps,
} from './useAllocationActivity';

type ActivityFeedProps = UseAllocationActivityProps & {
  // Whether an empty `tokenOptions` is empty because its registry failed. The
  // two look identical in the filter bar, and only one is a fact about tokens.
  tokenOptionsFailed?: boolean;
  chainLabels?: ChainLabelLookup;
};

export function ActivityFeed(props: ActivityFeedProps) {
  const {
    actionFilter,
    onActionFilterChange,
    tokenFilter = null,
    onTokenFilterChange,
    isEnabled,
    selectedPrime,
    showAllPrimes = false,
    chainLabels,
    selectedReceiptToken = null,
    tokenOptionsFailed = false,
  } = props;
  const {
    isPageMode,
    events,
    filteredEvents,
    isLoading,
    error,
    effectivePreset,
    effectiveRange,
    updateRangePreset,
    uniqueTokenOptions,
    hasActiveFilters,
    clearFilters,
    rowLimit,
    unindexedNetwork,
  } = useAllocationActivity(props);

  const table = useActivityTable(filteredEvents, chainLabels);

  if (!isEnabled) {
    return (
      <EmptyState
        title={isPageMode ? 'Activity Unavailable' : 'Open Activity Tab'}
        description={
          isPageMode
            ? 'Activity view is currently unavailable.'
            : 'Activity loads when the drawer is open and the Activity tab is selected.'
        }
        stretch
      />
    );
  }

  if (!isPageMode && !selectedPrime) {
    return (
      <EmptyState
        title="No Prime Selected"
        description="Select a prime to view its activity feed."
        stretch
      />
    );
  }

  const latestActivityAt = events[0]?.created_at ?? null;

  // A chain STL has no id for suppresses the request entirely, so "nothing
  // matched your filters" would be the wrong reason. The drawer reads that off
  // the selected receipt token; page mode has only the network filter.
  // Wrapped, not a bare name: the chain being unindexed is known even when it
  // cannot be named, and the message says "this chain" for that case.
  const unindexedFilter =
    selectedReceiptToken?.chain_id === null
      ? { network: selectedReceiptToken.network }
      : unindexedNetwork === null
        ? null
        : { network: unindexedNetwork };
  const emptyDescription =
    unindexedFilter === null
      ? 'No allocation activity events match your filters.'
      : unindexedChainMessage(unindexedFilter.network, 'activity');

  const activityResults = (
    <ActivityResults
      table={table}
      isLoading={isLoading}
      error={error}
      totalEventCount={events.length}
      visibleEventCount={filteredEvents.length}
      rowLimit={rowLimit}
      emptyDescription={emptyDescription}
    />
  );

  if (!isPageMode) {
    return activityResults;
  }

  return (
    <PageShell>
      <div className={css({ display: 'grid', gap: '5' })}>
        <ActivityPageHeader
          isPageMode={isPageMode}
          showAllPrimes={showAllPrimes}
          latestActivityAt={latestActivityAt}
          rangePreset={effectivePreset}
          range={effectiveRange}
          onRangeChange={updateRangePreset}
        />
        <ActivityFilterBar
          actionFilter={actionFilter}
          onActionFilterChange={onActionFilterChange}
          tokenFilter={tokenFilter}
          onTokenFilterChange={onTokenFilterChange}
          tokenOptions={uniqueTokenOptions}
          tokenOptionsFailed={tokenOptionsFailed}
          hasActiveFilters={hasActiveFilters}
          onClearFilters={clearFilters}
        />
        <div
          className={css({
            display: 'flex',
            flexDirection: 'column',
            minHeight: '96',
          })}
        >
          {activityResults}
        </div>
      </div>
    </PageShell>
  );
}
