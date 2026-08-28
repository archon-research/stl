import { Outlet, useSearch } from '@tanstack/react-router';
import { useState } from 'react';

import { css } from '#styled-system/css';

import { useProvenanceAvailability } from '../shared/hooks/useProvenanceAvailability';
import { useUpdateSearch } from '../shared/hooks/useUpdateSearch';
import { CollapsibleSidebarLayout } from '../shared/ui/CollapsibleSidebarLayout';
import { PrimeSidebar } from '../shared/ui/PrimeSidebar';
import { TopBar } from '../shared/ui/TopBar';
import {
  useSelectedView,
  useViewNavigation,
  useViewPreload,
} from './navigation';
import { PrimeSelectionProvider, usePrimeSelection } from './prime-selection';
import { TimeRangeProvider, useTimeRange } from './time-range';
import { useDashboardFilters } from './useDashboardFilters';
import { useProvenanceRedirect } from './useProvenanceRedirect';

const shellClassName = css({
  position: 'relative',
  // Not a workaround: the sidebar splitter's 1px indicator line is
  // redundant next to the sidebar's own border, so hide it and let the
  // col-resize cursor carry the affordance.
  '& [data-sidebar-layout] [data-scope="resize-handle"][data-part="indicator"]':
    {
      opacity: 0,
    },
});

/**
 * The chrome both views sit in: prime list, top bar, and the slot the route
 * renders into.
 *
 * It hangs off the root route, so it survives navigation between the views —
 * which is what keeps the collapsed sidebar collapsed and the dragged sidebar
 * width where the reader put it.
 */
function DashboardChrome() {
  const selectedView = useSelectedView();
  const navigateToView = useViewNavigation();
  const preloadView = useViewPreload();
  const updateSearch = useUpdateSearch();
  const search = useSearch({ from: '__root__' });
  const activitiesSearch = useSearch({
    from: '/activities',
    shouldThrow: false,
  });
  const { rangePreset, timeRange, onRangeChange } = useTimeRange();
  const {
    primeGroups,
    selectedPrimeGroup,
    selectedPrimeId,
    selectedPrime,
    isLoading,
    errorMessage,
    selectPrime,
  } = usePrimeSelection();

  const provenanceAvailability = useProvenanceAvailability();
  useProvenanceRedirect(provenanceAvailability, selectedPrimeGroup?.name);

  const { networkOptions, protocolOptions } = useDashboardFilters(
    selectedView,
    selectedPrimeGroup,
  );

  // View-local on purpose: collapsing the prime list is a momentary "give me the
  // whole width" gesture, not a preference worth persisting across sessions.
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);

  const showAllPrimesInActivities =
    selectedView === 'activities' ? activitiesSearch?.allp !== '0' : false;

  return (
    <div className={shellClassName}>
      <div data-sidebar-layout>
        <CollapsibleSidebarLayout
          isSidebarCollapsed={isSidebarCollapsed}
          sidebar={
            <PrimeSidebar
              primeGroups={primeGroups}
              selectedPrimeId={selectedPrimeId}
              isLoading={isLoading}
              errorMessage={errorMessage}
              onSelectPrime={selectPrime}
              showAllPrimes={showAllPrimesInActivities}
              canShowAllPrimes={selectedView === 'activities'}
              onShowAllPrimesChange={(value) =>
                updateSearch({ allp: value ? '1' : '0' })
              }
            />
          }
          topBar={
            <TopBar
              isSidebarCollapsed={isSidebarCollapsed}
              onToggleSidebar={() =>
                setIsSidebarCollapsed((collapsed) => !collapsed)
              }
              availableProvenances={provenanceAvailability.forPrime(
                selectedPrimeGroup?.name,
              )}
              hasSelectedPrime={selectedPrime !== null}
              networkOptions={networkOptions}
              onNetworkChange={(value) =>
                updateSearch({ network: value ?? undefined })
              }
              onProtocolChange={(value) =>
                updateSearch({ protocol: value ?? undefined })
              }
              protocolOptions={protocolOptions}
              selectedNetwork={search.network ?? null}
              selectedProtocol={search.protocol ?? null}
              selectedView={selectedView}
              onViewChange={(view) =>
                navigateToView({ view, primeKey: selectedPrimeId })
              }
              onViewIntent={preloadView}
              rangePreset={rangePreset}
              timeRange={timeRange}
              onRangeChange={onRangeChange}
            />
          }
          main={<Outlet />}
        />
      </div>
    </div>
  );
}

/**
 * The root route's component. Only the providers whose values must resolve once
 * for the whole app live here; the chrome itself consumes them.
 */
export function RootLayout() {
  return (
    <TimeRangeProvider>
      <PrimeSelectionProvider>
        <DashboardChrome />
      </PrimeSelectionProvider>
    </TimeRangeProvider>
  );
}
