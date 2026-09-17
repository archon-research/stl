import type { SortingState } from '@archon-research/design-system';

import type { ChainLabelLookup } from '../../shared/lib/dashboard';
import { useProvenanceView } from '../../shared/lib/provenance';
import type {
  Allocation,
  Prime,
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
} from '../../shared/types/allocation';
import type { LocalProtocolRow } from '../../shared/types/local-data';
import { findMetricChart, type MetricChartSpec } from './metricCards';
import {
  useAllocationGridFilters,
  type UseAllocationGridFiltersResult,
} from './useAllocationGridFilters';
import {
  useAllocationGridTable,
  type UseAllocationGridTableResult,
} from './useAllocationGridTable';
import {
  useAllocationSummaries,
  type UseAllocationSummariesResult,
} from './useAllocationSummaries';

export type UseAllocationGridArgs = {
  allocations: Allocation[];
  riskCapital: PrimeRiskCapital | null;
  chainLabels: ChainLabelLookup;
  filteredAllocations: Allocation[];
  topMetricsAllocations: Allocation[];
  areAllocationsSettled: boolean;
  isRiskCapitalLoading: boolean;
  localProtocols: LocalProtocolRow[];
  primeDebtSnapshot: PrimeDebtSnapshot | null;
  referenceDebt: PrimeDebtBucket | null;
  onSearchChange: (value: string) => void;
  onSortingChange: (
    sorting: SortingState | ((old: SortingState) => SortingState),
  ) => void;
  searchValue: string;
  selectedPrime: Prime | null;
  sorting: SortingState;
  metricCharts: MetricChartSpec[];
  riskCapitalErrorMessage: string | null;
};

// Explicit for the same portability reason as `UseAllocationGridTableResult`,
// whose doc carries the why.
type UseAllocationGridResult = UseAllocationGridFiltersResult &
  UseAllocationSummariesResult &
  UseAllocationGridTableResult &
  ReturnType<typeof resolveMetricCharts>;

function resolveMetricCharts(metricCharts: MetricChartSpec[]): {
  allocationActivityChart: MetricChartSpec | null;
  riskCapitalChart: MetricChartSpec | null;
  totalCapitalChart: MetricChartSpec | null;
  primeDebtChart: MetricChartSpec | null;
  primeCollateralChart: MetricChartSpec | null;
  encumbranceChart: MetricChartSpec | null;
} {
  return {
    allocationActivityChart: findMetricChart(
      metricCharts,
      'allocation-activity-volume',
    ),
    riskCapitalChart: findMetricChart(metricCharts, 'risk-capital'),
    totalCapitalChart: findMetricChart(metricCharts, 'total-capital'),
    primeDebtChart: findMetricChart(metricCharts, 'prime-debt-exposure'),
    primeCollateralChart: findMetricChart(metricCharts, 'prime-collateral'),
    encumbranceChart: findMetricChart(metricCharts, 'encumbrance-ratio'),
  };
}

/**
 * All the derivations and hooks the allocation grid needs, composed from
 * cohesive sub-hooks: `useAllocationGridFilters` (search box + category
 * filter), `useAllocationSummaries` (headline totals + debt/encumbrance/top-
 * metrics skeleton), and `useAllocationGridTable` (risk-joined rows + the
 * `useDataTable` instance). Each sub-hook's own args type is a subset of
 * `UseAllocationGridArgs`'s shape, so `args` is handed to each directly
 * rather than re-destructured here.
 */
export function useAllocationGrid(
  args: UseAllocationGridArgs,
): UseAllocationGridResult {
  // The provenance on screen, not the one fetched: narrowing a composite
  // response changes what is shown without issuing a request.
  const { showsReference: showsReferenceNow } = useProvenanceView();

  const filters = useAllocationGridFilters(args);
  const summaries = useAllocationSummaries(args, showsReferenceNow);
  const tableState = useAllocationGridTable(args, filters.visibleAllocations);

  return {
    ...filters,
    ...summaries,
    ...tableState,
    ...resolveMetricCharts(args.metricCharts),
  };
}
