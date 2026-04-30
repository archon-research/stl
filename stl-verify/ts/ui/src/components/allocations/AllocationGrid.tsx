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
  formatTokenAmount,
  formatUsdValue,
  getAllocationKey,
  getChainLabel,
  getProtocolLabel,
  parseNumericValue,
} from '../../lib/dashboard';
import type { Allocation, AllocationCategory, Prime } from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import { Address, EmptyState, ErrorState, SummaryMetric } from '../shared';

type AllocationGridProps = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  isLoading: boolean;
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

function getCategoryTextColor(category: AllocationCategory | undefined): string {
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

function getCategoryLabel(category: AllocationCategory | undefined): string {
  const labels: Record<AllocationCategory, string> = {
    allocation: 'Allocation',
    pol: 'Protocol Owned Liquidity',
    psm3: 'PSM3',
    asset: 'Asset',
  };
  return category ? labels[category] : 'Unknown';
}

export function AllocationGrid({
  allocations,
  chainLabels,
  errorMessage,
  filteredAllocations,
  isLoading,
  localProtocols,
  onSelectAllocation,
  onSearchChange,
  onSortingChange,
  searchValue,
  selectedAllocationKey,
  selectedPrime,
  sorting,
}: AllocationGridProps) {
  type AllocationWithMetrics = Allocation & {
    amount_usd?: string | number | null;
    latest_activity_at?: string | null;
  };

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
    if (filteredAllocations.length === 0) {
      return null;
    }

    const allocationsWithMetrics = filteredAllocations as AllocationWithMetrics[];
    const totalUsd = allocationsWithMetrics.reduce(
      (sum, allocation) => sum + (parseNumericValue(allocation.amount_usd) ?? 0),
      0,
    );

    const largestAllocation = allocationsWithMetrics.reduce<AllocationWithMetrics | null>(
      (largest, allocation) => {
        if (!largest) {
          return allocation;
        }

        const largestUsd = parseNumericValue(largest.amount_usd) ?? 0;
        const nextUsd = parseNumericValue(allocation.amount_usd) ?? 0;
        return nextUsd > largestUsd ? allocation : largest;
      },
      null,
    );

    const latestActivityAt = allocationsWithMetrics.reduce<string | null>(
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
      allocationCount: filteredAllocations.length,
      largestAllocation,
      latestActivityAt,
      totalUsd,
    };
  }, [filteredAllocations]);

  const columns = useMemo<ColumnDef<Allocation>[]>(
    () => [
      {
        id: 'symbol',
        header: 'Asset',
        accessorFn: (allocation) => allocation.symbol,
        cell: ({ row }) => {
          const allocation = row.original;
          const isSelected =
            getAllocationKey(allocation) === selectedAllocationKey;

          return (
            <div className={flex({ align: 'center', gap: '3' })}>
              <div
                className={css({
                  width: '10',
                  height: '10',
                  borderRadius: 'full',
                  bg: isSelected ? 'interactive.accent' : 'surface.subtle',
                  color: isSelected ? 'white' : 'text.strong',
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: 'xs',
                  fontWeight: 'semibold',
                  flexShrink: 0,
                })}
              >
                {allocation.symbol.slice(0, 2).toUpperCase()}
              </div>
              <div className={css({ display: 'grid', gap: '1' })}>
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
                    })}
                  >
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
                    })}
                  >
                    {getChainLabel(allocation.chain_id, chainLabels)}
                  </span>
                </div>
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
            <div>
              <p
                className={css({
                  m: 0,
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                {allocation.underlying_symbol}
              </p>
              <Address
                value={allocation.underlying_token_address}
                truncate={false}
                className={css({
                  fontFamily: 'mono',
                  fontSize: 'xs',
                  color: 'text.muted',
                })}
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
          const allocation = row.original as AllocationWithMetrics;
          const amountUsd = allocation.amount_usd;

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
                {amountUsd !== undefined && amountUsd !== null
                  ? formatUsdValue(amountUsd)
                  : `${formatTokenAmount(allocation.balance)} ${allocation.symbol}`}
              </p>
              <Address
                value={allocation.receipt_token_address}
                truncate={false}
                className={css({
                  fontFamily: 'mono',
                  fontSize: 'xs',
                  color: 'text.muted',
                })}
              />
            </div>
          );
        },
      },
      {
        id: 'latest_activity_at',
        header: 'Latest activity',
        accessorFn: (allocation) => {
          const latestActivityAt = (allocation as AllocationWithMetrics).latest_activity_at;
          return latestActivityAt ? new Date(latestActivityAt).getTime() : 0;
        },
        cell: ({ row }) => {
          const allocation = row.original as AllocationWithMetrics;

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
    [chainLabels, localProtocols, selectedAllocationKey],
  );

  const table = useDataTable(filteredAllocations, columns, {
    enableSorting: true,
    onSortingChange,
    sorting,
  });

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
          {summary ? (
            <div
              className={css({
                display: 'grid',
                gridTemplateColumns: {
                  base: '1fr',
                  md: 'repeat(4, minmax(0, 1fr))',
                },
                gap: '3',
              })}
            >
              <SummaryMetric
                label="Total allocation"
                value={formatUsdValue(summary.totalUsd)}
                detail={`${summary.allocationCount} filtered allocations`}
              />
              <SummaryMetric
                label="Largest position"
                value={
                  summary.largestAllocation
                    ? summary.largestAllocation.symbol
                    : '—'
                }
                detail={
                  summary.largestAllocation
                    ? formatUsdValue(summary.largestAllocation.amount_usd)
                    : undefined
                }
              />
              <SummaryMetric
                label="Allocations"
                value={String(summary.allocationCount)}
                detail="Reflects current top-bar filters"
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
            </div>
          ) : null}
          <div
            className={flex({
              align: 'flex-end',
              justify: 'space-between',
              gap: '5',
              wrap: 'wrap',
            })}
          >
            <div
              className={css({ display: 'grid', gap: '1', minWidth: '18rem' })}
            >
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
              {selectedPrime ? (
                <p
                  className={css({
                    m: 0,
                    fontSize: 'sm',
                    color: 'text.muted',
                  })}
                >
                  {selectedPrime.id}
                </p>
              ) : null}
            </div>
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
