import { SearchInput } from '@archon-research/design-system';
import { type ColumnDef, type SortingState } from '@tanstack/react-table';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { DataTable, useDataTable } from '../../data-table';
import {
  type ChainLabelLookup,
  formatDateTime,
  formatFreshnessLabel,
  formatRatioPercent,
  formatTokenAmount,
  formatUsdValue,
  getAllocationKey,
  getCategoryLabel,
  getChainLabel,
  getProtocolLabel,
  parseNumericValue,
} from '../../lib/dashboard';
import type {
  Allocation,
  AllocationCategory,
  CapitalMetrics,
  Prime,
} from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import {
  ChainLogo,
  EmptyState,
  ErrorState,
  ProtocolLogo,
  SummaryMetric,
  TokenAddress,
  TokenLogo,
} from '../shared';

type AllocationGridProps = {
  allocations: Allocation[];
  capitalMetrics: CapitalMetrics | null;
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  topMetricsAllocations: Allocation[];
  isLoading: boolean;
  isCapitalMetricsLoading: boolean;
  localProtocols: LocalProtocolRow[];
  onSelectAllocation: (allocationKey: string) => void;
  onSearchChange: (value: string) => void;
  onSortingChange: (
    sorting: SortingState | ((old: SortingState) => SortingState),
  ) => void;
  searchValue: string;
  selectedAllocationKey: string | null;
  selectedPrime: Prime | null;
  sorting: SortingState;
};

function getCategoryColor(category: AllocationCategory | undefined): string {
  switch (category) {
    case 'allocation':
      return 'bg.success';
    case 'pol':
      return 'bg.warning';
    case 'psm3':
      return 'bg.interactive';
    case 'asset':
      return 'bg.info';
    default:
      return 'bg.subtle';
  }
}

function getCategoryTextColor(
  category: AllocationCategory | undefined,
): string {
  switch (category) {
    case 'allocation':
      return 'text.success';
    case 'pol':
      return 'text.warning';
    case 'psm3':
      return 'text.interactive';
    case 'asset':
      return 'text.info';
    default:
      return 'text.default';
  }
}

export function AllocationGrid({
  allocations,
  capitalMetrics,
  chainLabels,
  errorMessage,
  filteredAllocations,
  topMetricsAllocations,
  isLoading,
  isCapitalMetricsLoading,
  localProtocols,
  onSelectAllocation,
  onSearchChange,
  onSortingChange,
  searchValue,
  selectedAllocationKey,
  selectedPrime,
  sorting,
}: AllocationGridProps) {
  const [localSearchValue, setLocalSearchValue] = useState(searchValue);

  useEffect(() => {
    setLocalSearchValue(searchValue);
  }, [searchValue]);

  useEffect(() => {
    if (localSearchValue === searchValue) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      onSearchChange(localSearchValue);
    }, 300);

    return () => window.clearTimeout(timeoutId);
  }, [localSearchValue, onSearchChange, searchValue]);

  const summary = useMemo(() => {
    if (topMetricsAllocations.length === 0) {
      return null;
    }

    const totalUsd = topMetricsAllocations.reduce(
      (sum, allocation) =>
        sum + (parseNumericValue(allocation.amount_usd) ?? 0),
      0,
    );

    const latestActivityAt = topMetricsAllocations.reduce<string | null>(
      (latest, allocation) => {
        if (!allocation.latest_activity_at) {
          return latest;
        }

        if (!latest) {
          return allocation.latest_activity_at;
        }

        return new Date(allocation.latest_activity_at) > new Date(latest)
          ? allocation.latest_activity_at
          : latest;
      },
      null,
    );

    return {
      allocationCount: topMetricsAllocations.length,
      latestActivityAt,
      totalUsd,
    };
  }, [topMetricsAllocations]);

  const overallSummary = useMemo(() => {
    if (allocations.length === 0) {
      return null;
    }

    const totalUsd = allocations.reduce(
      (sum, allocation) =>
        sum + (parseNumericValue(allocation.amount_usd) ?? 0),
      0,
    );

    return {
      allocationCount: allocations.length,
      totalUsd,
    };
  }, [allocations]);

  const hasSearchQuery = searchValue.trim().length > 0;

  const columns = useMemo<ColumnDef<Allocation>[]>(
    () => [
      {
        id: 'symbol',
        header: 'Asset',
        accessorFn: (allocation) => allocation.symbol,
        cell: ({ row }) => {
          const allocation = row.original;

          return (
            <div className={css({ display: 'grid', gap: '1', minWidth: 0 })}>
              <p
                className={css({
                  m: 0,
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                {allocation.symbol}
              </p>
              <div className={flex({ gap: '1.5', wrap: 'wrap' })}>
                <span
                  className={css({
                    fontSize: 'xs',
                    color: 'text.muted',
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '1.5',
                    whiteSpace: 'nowrap',
                  })}
                >
                  <ProtocolLogo
                    protocolName={getProtocolLabel(
                      allocation.protocol_name,
                      localProtocols,
                      allocation.chain_id,
                    )}
                    size="5"
                  />
                  {getProtocolLabel(
                    allocation.protocol_name,
                    localProtocols,
                    allocation.chain_id,
                  )}
                </span>
                <span
                  className={css({
                    fontSize: 'xs',
                    color: 'text.muted',
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '1.5',
                    whiteSpace: 'nowrap',
                  })}
                >
                  <ChainLogo
                    chainId={allocation.chain_id}
                    label={getChainLabel(allocation.chain_id, chainLabels)}
                    size="5"
                  />
                  {getChainLabel(allocation.chain_id, chainLabels)}
                </span>
              </div>
            </div>
          );
        },
      },
      {
        id: 'underlying_symbol',
        header: 'Underlying',
        accessorFn: (allocation) => allocation.underlying_symbol,
        cell: ({ row }) => {
          const allocation = row.original;

          return (
            <div
              className={css({
                display: 'flex',
                flexDirection: 'column',
                gap: '1',
              })}
            >
              <div className={flex({ align: 'center', gap: '2' })}>
                <TokenLogo
                  address={allocation.underlying_token_address}
                  chainId={allocation.chain_id}
                  size="6"
                  symbol={allocation.underlying_symbol}
                />
                <span
                  className={css({
                    fontSize: 'sm',
                    fontWeight: 'semibold',
                    color: 'text.strong',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    m: 0,
                  })}
                >
                  {allocation.underlying_symbol}
                </span>
              </div>
              <TokenAddress
                address={allocation.underlying_token_address}
                chainId={allocation.chain_id}
                style={{ fontSize: '0.8rem' }}
              />
            </div>
          );
        },
      },
      {
        id: 'balance',
        header: 'Balance',
        accessorFn: (allocation) => Number(allocation.balance),
        cell: ({ row }) => {
          const allocation = row.original;
          const amountUsd = allocation.amount_usd;

          return (
            <div
              className={css({
                display: 'flex',
                flexDirection: 'column',
                gap: '1',
              })}
            >
              <div className={flex({ align: 'center', gap: '2' })}>
                <TokenLogo
                  address={allocation.receipt_token_address}
                  chainId={allocation.chain_id}
                  size="6"
                  symbol={allocation.symbol}
                />
                <span
                  className={css({
                    fontSize: 'sm',
                    fontWeight: 'semibold',
                    color: 'text.strong',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    m: 0,
                  })}
                >
                  {amountUsd !== undefined && amountUsd !== null
                    ? formatUsdValue(amountUsd)
                    : `${formatTokenAmount(allocation.balance)} ${allocation.symbol}`}
                </span>
              </div>
              <TokenAddress
                address={allocation.receipt_token_address}
                chainId={allocation.chain_id}
                style={{ fontSize: '0.8rem' }}
              />
            </div>
          );
        },
      },
      {
        id: 'latest_activity_at',
        header: 'Latest activity',
        accessorFn: (allocation) => {
          const latestActivityAt = allocation.latest_activity_at;
          return latestActivityAt ? new Date(latestActivityAt).getTime() : 0;
        },
        cell: ({ row }) => {
          const allocation = row.original;

          if (!allocation.latest_activity_at) {
            return (
              <p
                className={css({
                  m: 0,
                  fontSize: 'sm',
                  color: 'text.muted',
                })}
              >
                —
              </p>
            );
          }

          return (
            <div>
              <p
                className={css({
                  m: 0,
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                {formatFreshnessLabel(allocation.latest_activity_at)}
              </p>
              <p
                className={css({
                  m: 0,
                  fontSize: 'xs',
                  color: 'text.muted',
                })}
              >
                {formatDateTime(allocation.latest_activity_at)}
              </p>
            </div>
          );
        },
      },
      {
        id: 'category',
        header: 'Category',
        accessorFn: (allocation) => allocation.category,
        cell: ({ row }) => {
          const allocation = row.original;
          const category = allocation.category;
          const categoryBg = getCategoryColor(category);
          const categoryText = getCategoryTextColor(category);

          return (
            <div
              className={css({
                display: 'inline-flex',
                alignItems: 'center',
                px: '2',
                py: '1',
                borderRadius: 'md',
                fontSize: 'xs',
                fontWeight: 'semibold',
                bg: categoryBg,
                color: categoryText,
              })}
            >
              {getCategoryLabel(category)}
            </div>
          );
        },
      },
    ],
    [chainLabels, localProtocols],
  );

  const table = useDataTable(filteredAllocations, columns, {
    enableSorting: true,
    onSortingChange,
    sorting,
  });

  const showTopMetricsSkeleton =
    selectedPrime !== null && (isLoading || isCapitalMetricsLoading);

  const hasTopMetrics = capitalMetrics !== null || summary !== null;

  return (
    <div
      className={css({
        minHeight: '100%',
        bg: 'surface.subtle',
        px: { base: '5', md: '7' },
        py: { base: '6', md: '7' },
      })}
    >
      <section
        className={css({
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
          bg: 'surface.default',
          p: { base: '5', md: '6' },
          boxShadow: '0 24px 80px rgba(15, 23, 42, 0.08)',
        })}
      >
        <div className={css({ display: 'grid', gap: '4' })}>
          <div
            className={flex({
              align: 'flex-end',
              justify: 'space-between',
              gap: '3',
              wrap: 'wrap',
            })}
          >
            <div
              className={css({ display: 'grid', gap: '1', minWidth: '18rem' })}
            >
              <div className={flex({ align: 'center', gap: '2.5' })}>
                {selectedPrime ? (
                  <ProtocolLogo protocolName={selectedPrime.name} size="9" />
                ) : null}
                <h1
                  className={css({
                    m: 0,
                    fontSize: { base: '2xl', md: '3xl' },
                    lineHeight: 'tight',
                    color: 'text.strong',
                  })}
                >
                  {selectedPrime ? selectedPrime.name : 'Select a prime'}
                </h1>
              </div>
              {selectedPrime ? (
                <TokenAddress address={selectedPrime.id} />
              ) : null}
            </div>
            {!showTopMetricsSkeleton &&
            capitalMetrics &&
            parseNumericValue(capitalMetrics.risk_to_capital_ratio) !== null ? (
              <span
                className={css({
                  fontSize: 'xs',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                Risk-to-capital{' '}
                {formatRatioPercent(capitalMetrics.risk_to_capital_ratio)}
              </span>
            ) : null}
          </div>
          {showTopMetricsSkeleton ? (
            <div
              className={css({
                display: 'grid',
                gridTemplateColumns: {
                  base: 'repeat(2, minmax(0, 1fr))',
                  md: 'repeat(4, minmax(0, 1fr))',
                },
                gap: '3',
              })}
            >
              {Array.from({ length: 4 }).map((_, index) => (
                <div
                  key={`metrics-skeleton-${index}`}
                  className={css({
                    height: '88px',
                    borderRadius: 'md',
                    borderStyle: 'solid',
                    borderWidth: '1px',
                    borderColor: 'border.subtle',
                    bg: 'surface.subtle',
                  })}
                />
              ))}
            </div>
          ) : null}
          {!showTopMetricsSkeleton && hasTopMetrics ? (
            <div
              className={css({
                display: 'grid',
                gridTemplateColumns: {
                  base: 'repeat(2, minmax(0, 1fr))',
                  md: 'repeat(4, minmax(0, 1fr))',
                },
                gap: '3',
              })}
            >
              {capitalMetrics ? (
                <>
                  <SummaryMetric
                    label="Risk capital"
                    value={formatUsdValue(capitalMetrics.risk_capital)}
                    detail="Onchain allocation exposure"
                  />
                  <SummaryMetric
                    label="Total capital"
                    value={formatUsdValue(capitalMetrics.total_capital)}
                    detail={`Buffer ${formatUsdValue(capitalMetrics.capital_buffer)} · First loss ${formatUsdValue(capitalMetrics.first_loss_capital)}`}
                  />
                </>
              ) : null}

              {summary ? (
                <>
                  <SummaryMetric
                    label="Total allocation"
                    value={
                      hasSearchQuery && overallSummary
                        ? `${formatUsdValue(summary.totalUsd)} / ${formatUsdValue(overallSummary.totalUsd)}`
                        : formatUsdValue(summary.totalUsd)
                    }
                    detail={
                      hasSearchQuery && overallSummary
                        ? `${summary.allocationCount}/${overallSummary.allocationCount} allocations`
                        : `${summary.allocationCount} allocations`
                    }
                  />
                  <SummaryMetric
                    label="Latest activity"
                    value={
                      summary.latestActivityAt
                        ? formatFreshnessLabel(summary.latestActivityAt)
                        : '—'
                    }
                    detail={
                      summary.latestActivityAt
                        ? formatDateTime(summary.latestActivityAt)
                        : 'No indexed activity'
                    }
                  />
                </>
              ) : null}
            </div>
          ) : null}
          {!showTopMetricsSkeleton && capitalMetrics?.validation_note ? (
            <p
              className={css({
                m: 0,
                fontSize: 'xs',
                color: 'text.muted',
                fontStyle: 'italic',
                textAlign: 'left',
              })}
            >
              {capitalMetrics.validation_note}
            </p>
          ) : null}
          <div
            className={flex({
              align: 'flex-end',
              justify: 'space-between',
              gap: '5',
              wrap: 'wrap',
            })}
          >
            <span
              className={css({
                display: 'inline-flex',
                width: 'fit-content',
                alignItems: 'center',
                borderRadius: 'full',
                bg: { _dark: 'gray.800', base: 'gray.100' },
                px: '3',
                py: '1',
                fontSize: 'xs',
                fontWeight: 'semibold',
                letterSpacing: '0.14em',
                textTransform: 'uppercase',
                color: 'text.muted',
              })}
            >
              Allocations
            </span>
            <div
              className={css({
                flex: '0 1 24rem',
                minWidth: { base: '100%', md: '22rem' },
                marginLeft: 'auto',
                alignSelf: 'flex-end',
              })}
            >
              <SearchInput
                aria-label="Search allocations"
                disabled={!selectedPrime}
                onValueChange={setLocalSearchValue}
                placeholder="Search assets, protocols, chains"
                value={localSearchValue}
              />
            </div>
          </div>
        </div>

        <div className={css({ mt: '6' })}>
          {!selectedPrime && !isLoading ? (
            <EmptyState
              title="Choose a prime to load positions"
              description="The main grid activates once a prime is selected from the sidebar."
            />
          ) : null}

          {selectedPrime && errorMessage ? (
            <ErrorState
              title="Unable to load allocations"
              description="An error occurred while fetching allocation data."
              errorMessage={errorMessage}
            />
          ) : null}

          {selectedPrime &&
          !errorMessage &&
          !isLoading &&
          allocations.length === 0 ? (
            <EmptyState
              title="No allocations returned"
              description="The selected prime did not return any allocation rows from the API."
            />
          ) : null}

          {selectedPrime &&
          !errorMessage &&
          !isLoading &&
          allocations.length > 0 &&
          filteredAllocations.length === 0 ? (
            <EmptyState
              title="No rows match the active filters"
              description="Clear one of the filters in the top bar to restore the allocation grid."
            />
          ) : null}

          {selectedPrime &&
          !errorMessage &&
          (isLoading || filteredAllocations.length > 0) ? (
            <DataTable
              table={table}
              isLoading={isLoading}
              onRowClick={(allocation) =>
                onSelectAllocation(getAllocationKey(allocation))
              }
              getRowKey={getAllocationKey}
              selectedRowKey={selectedAllocationKey}
            />
          ) : null}
        </div>
      </section>
    </div>
  );
}
