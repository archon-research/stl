import type { SortingState } from '@archon-research/design-system';
import { useSearch } from '@tanstack/react-router';
import { useMemo } from 'react';

import { AllocationDrawer } from '../features/allocations/AllocationDrawer';
import { AllocationGrid } from '../features/allocations/AllocationGrid';
import { buildMetricCharts } from '../features/allocations/metric-charts';
import { preloadAllocationDetail } from '../features/allocations/preload';
import { useAllocationRows } from '../features/allocations/useAllocationRows';
import { useAllocationSelection } from '../features/allocations/useAllocationSelection';
import { useFilteredAllocations } from '../features/allocations/useFilteredAllocations';
import { usePrimeChartSeries } from '../features/allocations/usePrimeChartSeries';
import { usePrimeMetrics } from '../features/allocations/usePrimeMetrics';
import { usePrimeTotalAllocationUsd } from '../features/allocations/usePrimeTotalAllocationUsd';
import {
  useChainLabels,
  useLocalProtocols,
} from '../shared/hooks/useRegistries';
import { useUpdateSearch } from '../shared/hooks/useUpdateSearch';
import { useUrlSyncedTableState } from '../shared/hooks/useUrlSyncedTableState';
import { useProvenanceView } from '../shared/lib/provenance';
import { usePrimeSelection } from './prime-selection';
import { useTimeRange } from './time-range';

/**
 * The allocation view: the selected prime's positions, its metric cards, and
 * the risk drawer for whichever row is addressed by the URL.
 */
export function AllocationRoute() {
  const {
    selectedPrimeGroup,
    selectedPrime,
    isLoading: isPrimesLoading,
    unknownPrimeMessage,
  } = usePrimeSelection();
  const { rangePreset, timeRange } = useTimeRange();
  const search = useSearch({ from: '/allocation' });
  const updateSearch = useUpdateSearch();
  const chainLabels = useChainLabels();
  const localProtocols = useLocalProtocols();
  const { showsReference: showsReferenceNow } = useProvenanceView();
  const { globalFilter, setGlobalFilter, setSorting, sorting } =
    useUrlSyncedTableState();

  const rows = useAllocationRows(selectedPrimeGroup);

  // The proxy every prime-wide read is addressed to; why one is enough is on
  // `riskCapitalQuery`, which is what depends on it.
  const primaryProxyAddress = selectedPrimeGroup?.primaryProxyAddress ?? null;
  // The range is passed only as a retry signal; these figures do not vary with
  // it, which is why it is not part of their key.
  const metrics = usePrimeMetrics(
    primaryProxyAddress,
    `${rangePreset}:${timeRange.from_timestamp ?? ''}:${timeRange.to_timestamp ?? ''}`,
  );

  const primeTotalAllocationUsd = usePrimeTotalAllocationUsd(rows.allocations);

  const series = usePrimeChartSeries(
    primaryProxyAddress,
    rangePreset,
    timeRange,
    primeTotalAllocationUsd,
  );

  const metricCharts = useMemo(
    () =>
      buildMetricCharts({
        series,
        riskCapital: metrics.riskCapital,
        referenceDebt: metrics.referenceDebt,
        primeDebtSnapshot: metrics.primeDebtSnapshot,
        showsReferenceNow,
        timeRange,
      }),
    [
      metrics.primeDebtSnapshot,
      metrics.referenceDebt,
      metrics.riskCapital,
      series,
      showsReferenceNow,
      timeRange,
    ],
  );

  const { searchFilteredAllocations, filteredAllocations } =
    useFilteredAllocations({
      allocations: rows.allocations,
      chainLabels,
      localProtocols,
      globalFilter,
      selectedNetwork: search.network ?? null,
      selectedProtocol: search.protocol ?? null,
    });

  const { selectedAllocation, selectedAllocationKey, isDrawerOpen } =
    useAllocationSelection(
      filteredAllocations,
      search.row,
      search.drawer === '1',
    );

  return (
    <>
      <AllocationGrid
        allocations={rows.allocations}
        riskCapital={metrics.riskCapital}
        chainLabels={chainLabels}
        errorMessage={rows.errorMessage}
        filteredAllocations={filteredAllocations}
        topMetricsAllocations={searchFilteredAllocations}
        isLoading={rows.isLoading}
        areAllocationsSettled={!isPrimesLoading && rows.isLoaded}
        isRiskCapitalLoading={metrics.isRiskCapitalLoading}
        isPrimeDebtLoading={metrics.isPrimeDebtLoading}
        localProtocols={localProtocols}
        onSelectAllocation={(allocationKey) => {
          updateSearch({ row: allocationKey, drawer: '1' });
        }}
        onAllocationIntent={preloadAllocationDetail}
        primeDebtSnapshot={metrics.primeDebtSnapshot}
        referenceDebt={metrics.referenceDebt}
        onSearchChange={setGlobalFilter}
        onSortingChange={setSorting}
        searchValue={globalFilter}
        selectedAllocationKey={selectedAllocationKey}
        selectedPrime={selectedPrime}
        sorting={sorting as SortingState}
        metricCharts={metricCharts}
        isChartsLoading={series.isLoading}
        chartsErrorMessage={series.errorMessage}
        riskCapitalErrorMessage={metrics.riskCapitalErrorMessage}
        primeDebtErrorMessage={metrics.primeDebtErrorMessage}
        noticeMessage={unknownPrimeMessage}
        primeCollateralUsd={series.primeCollateralValue}
        primeCollateralObservedAt={series.primeCollateralObservedAt}
        capitalObservedAt={series.capitalObservedAt}
      />

      <AllocationDrawer
        allocations={rows.allocations}
        chainLabels={chainLabels}
        errorMessage={rows.errorMessage}
        isLoading={rows.isLoading}
        isOpen={isDrawerOpen}
        localProtocols={localProtocols}
        onClose={() => updateSearch({ drawer: undefined })}
        riskCapital={metrics.riskCapital}
        selectedAllocation={selectedAllocation}
        selectedPrime={selectedPrime}
      />
    </>
  );
}
