import type { SortingState } from '@archon-research/design-system';

import { css } from '#styled-system/css';

import type { ChainLabelLookup } from '../../shared/lib/dashboard';
import type {
  Allocation,
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
  Prime,
} from '../../shared/types/allocation';
import type { LocalProtocolRow } from '../../shared/types/local-data';
import { PageShell } from '../../shared/ui';
import { TabNotePanel } from '../../shared/ui/TabStatePanels';
import { AllocationGridFilterBar } from './AllocationGridFilterBar';
import { AllocationGridHeader } from './AllocationGridHeader';
import { AllocationGridTable } from './AllocationGridTable';
import type { MetricChartSpec } from './metricCards';
import { MetricsBand } from './MetricsBand';
import { useAllocationGrid } from './useAllocationGrid';

type AllocationGridProps = {
  allocations: Allocation[];
  riskCapital: PrimeRiskCapital | null;
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  topMetricsAllocations: Allocation[];
  isLoading: boolean;
  // The rows are this prime's and the fetch has finished. Narrower than
  // `!isLoading`, which is also false before a fetch starts.
  areAllocationsSettled: boolean;
  isRiskCapitalLoading: boolean;
  isPrimeDebtLoading: boolean;
  localProtocols: LocalProtocolRow[];
  onSelectAllocation: (allocationKey: string) => void;
  // The pointer reaching the table is the earliest honest signal that a row is
  // about to be clicked, which is what the drawer's chunk is waiting for.
  onAllocationIntent?: () => void;
  primeDebtSnapshot: PrimeDebtSnapshot | null;
  referenceDebt: PrimeDebtBucket | null;
  onSearchChange: (value: string) => void;
  onSortingChange: (
    sorting: SortingState | ((old: SortingState) => SortingState),
  ) => void;
  searchValue: string;
  selectedAllocationKey: string | null;
  selectedPrime: Prime | null;
  sorting: SortingState;
  metricCharts: MetricChartSpec[];
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
  riskCapitalErrorMessage: string | null;
  primeDebtErrorMessage: string | null;
  noticeMessage: string | null;
  primeCollateralUsd: number | null;
  primeCollateralObservedAt: string | null;
  capitalObservedAt: string | null;
};

export function AllocationGrid(props: AllocationGridProps) {
  const {
    allocations,
    riskCapital,
    errorMessage,
    isLoading,
    areAllocationsSettled,
    isPrimeDebtLoading,
    onSelectAllocation,
    onAllocationIntent,
    selectedAllocationKey,
    selectedPrime,
    isChartsLoading,
    chartsErrorMessage,
    riskCapitalErrorMessage,
    primeDebtErrorMessage,
    noticeMessage,
    primeCollateralUsd,
    primeCollateralObservedAt,
    capitalObservedAt,
  } = props;

  const {
    categoryFilter,
    handleCategoryChange,
    visibleAllocations,
    localSearchValue,
    setLocalSearchValue,
    summary,
    overallSummary,
    debtWad,
    debtObservedAt,
    debtTimestampLabel,
    debtIlkLabel,
    hasSearchQuery,
    skeletonColumnHints,
    table,
    showTopMetricsSkeleton,
    hasTopMetrics,
    allocationActivityChart,
    riskCapitalChart,
    totalCapitalChart,
    primeDebtChart,
    primeCollateralChart,
    encumbranceChart,
    encumbranceRatio,
    encumbranceBreach,
    encumbranceCaption,
  } = useAllocationGrid(props);

  return (
    <PageShell>
      <div
        className={css({
          display: 'grid',
          gap: '4',
        })}
      >
        <AllocationGridHeader
          selectedPrime={selectedPrime}
          showTopMetricsSkeleton={showTopMetricsSkeleton}
          summary={summary}
          isPrimeDebtLoading={isPrimeDebtLoading}
          primeDebtErrorMessage={primeDebtErrorMessage}
          debtTimestampLabel={debtTimestampLabel}
          debtObservedAt={debtObservedAt}
        />
        {noticeMessage === null ? null : (
          <TabNotePanel message={noticeMessage} />
        )}
        <MetricsBand
          primeKey={selectedPrime?.id ?? null}
          isSkeleton={showTopMetricsSkeleton}
          hasTopMetrics={hasTopMetrics}
          summary={summary}
          overallSummary={overallSummary}
          hasSearchQuery={hasSearchQuery}
          riskCapital={riskCapital}
          capitalObservedAt={capitalObservedAt}
          riskCapitalErrorMessage={riskCapitalErrorMessage}
          summaryErrorMessage={errorMessage}
          primeDebtErrorMessage={primeDebtErrorMessage}
          hasPrime={selectedPrime !== null}
          collateral={{
            usd: primeCollateralUsd,
            observedAt: primeCollateralObservedAt,
            isLoading,
          }}
          encumbrance={{
            ratio: encumbranceRatio,
            caption: encumbranceCaption,
            severity: encumbranceBreach,
          }}
          debt={{
            wad: debtWad,
            ilkLabel: debtIlkLabel,
            isLoading: isPrimeDebtLoading,
          }}
          charts={{
            activity: allocationActivityChart,
            exposure: riskCapitalChart,
            totalCapital: totalCapitalChart,
            collateral: primeCollateralChart,
            encumbrance: encumbranceChart,
            debt: primeDebtChart,
          }}
          isChartsLoading={isChartsLoading}
          chartsErrorMessage={chartsErrorMessage}
        />
        {/* The provenance footnote lived here. Extracted whole to
            `MetricsFootnote` and deliberately not rendered — see that file for
            why, and for how to switch it back on. */}
        <AllocationGridFilterBar
          categoryFilter={categoryFilter}
          handleCategoryChange={handleCategoryChange}
          selectedPrime={selectedPrime}
          localSearchValue={localSearchValue}
          setLocalSearchValue={setLocalSearchValue}
        />
      </div>

      <AllocationGridTable
        errorMessage={errorMessage}
        selectedPrime={selectedPrime}
        areAllocationsSettled={areAllocationsSettled}
        allocations={allocations}
        visibleAllocations={visibleAllocations}
        onAllocationIntent={onAllocationIntent}
        table={table}
        onSelectAllocation={onSelectAllocation}
        selectedAllocationKey={selectedAllocationKey}
        skeletonColumnHints={skeletonColumnHints}
      />
    </PageShell>
  );
}
