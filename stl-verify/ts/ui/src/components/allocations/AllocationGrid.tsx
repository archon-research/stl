import {
  type ColumnDef,
  DataTable,
  EmptyState,
  ErrorState,
  SearchInput,
  type SortingState,
  useDataTable,
} from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  type ChainLabelLookup,
  formatDateTime,
  formatFreshnessLabel,
  formatRawWadLabel,
  formatRatioPercent,
  formatTokenAmount,
  formatUsdValue,
  formatWadValue,
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
  PrimeDebtSnapshot,
} from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import {
  AppTooltip,
  ChainLogo,
  PageShell,
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
  isPrimeDebtLoading: boolean;
  localProtocols: LocalProtocolRow[];
  onSelectAllocation: (allocationKey: string) => void;
  primeDebtSnapshot: PrimeDebtSnapshot | null;
  onSearchChange: (value: string) => void;
  onSortingChange: (
    sorting: SortingState | ((old: SortingState) => SortingState),
  ) => void;
  searchValue: string;
  selectedAllocationKey: string | null;
  selectedPrime: Prime | null;
  sorting: SortingState;
};

const tableHeaderTypographyClassName = css({
  '& thead th': {
    fontSize: 'sm',
    fontWeight: 'semibold',
    lineHeight: 'shorter',
    letterSpacing: '0.02em',
    textTransform: 'uppercase',
    color: 'text.default',
  },
  '& thead th button': {
    fontSize: 'sm',
    fontWeight: 'semibold',
    lineHeight: 'shorter',
    letterSpacing: '0.02em',
    textTransform: 'uppercase',
    color: 'text.default',
  },
});

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

function AllocationAssetCell({
  allocation,
  localProtocols,
  chainLabels,
}: {
  allocation: Allocation;
  localProtocols: LocalProtocolRow[];
  chainLabels: ChainLabelLookup;
}) {
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
}

function AllocationUnderlyingCell({ allocation }: { allocation: Allocation }) {
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
}

function AllocationBalanceCell({ allocation }: { allocation: Allocation }) {
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
}

function AllocationActivityCell({ allocation }: { allocation: Allocation }) {
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
}

function AllocationCategoryCell({ allocation }: { allocation: Allocation }) {
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
}

function createAllocationColumns(
  chainLabels: ChainLabelLookup,
  localProtocols: LocalProtocolRow[],
): ColumnDef<Allocation>[] {
  return [
    {
      id: 'symbol',
      header: 'Asset',
      accessorFn: (allocation) => allocation.symbol,
      cell: ({ row }) => (
        <AllocationAssetCell
          allocation={row.original}
          chainLabels={chainLabels}
          localProtocols={localProtocols}
        />
      ),
    },
    {
      id: 'underlying_symbol',
      header: 'Underlying',
      accessorFn: (allocation) => allocation.underlying_symbol,
      cell: ({ row }) => <AllocationUnderlyingCell allocation={row.original} />,
    },
    {
      id: 'balance',
      header: 'Balance',
      accessorFn: (allocation) => Number(allocation.balance),
      cell: ({ row }) => <AllocationBalanceCell allocation={row.original} />,
    },
    {
      id: 'latest_activity_at',
      header: 'Latest Activity',
      accessorFn: (allocation) => {
        const latestActivityAt = allocation.latest_activity_at;
        return latestActivityAt ? new Date(latestActivityAt).getTime() : 0;
      },
      cell: ({ row }) => <AllocationActivityCell allocation={row.original} />,
    },
    {
      id: 'category',
      header: 'Category',
      accessorFn: (allocation) => allocation.category,
      cell: ({ row }) => <AllocationCategoryCell allocation={row.original} />,
    },
  ];
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
  isPrimeDebtLoading,
  localProtocols,
  onSelectAllocation,
  primeDebtSnapshot,
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
    () => createAllocationColumns(chainLabels, localProtocols),
    [chainLabels, localProtocols],
  );

  const table = useDataTable(filteredAllocations, columns, {
    enableSorting: true,
    onSortingChange,
    sorting,
  });

  const showTopMetricsSkeleton =
    selectedPrime !== null && (isLoading || isCapitalMetricsLoading);

  const hasTopMetrics =
    capitalMetrics !== null || summary !== null || selectedPrime !== null;

  const metricsCardClassName = css({
    borderRadius: 'sm',
    borderStyle: 'solid',
    borderWidth: '1px',
    borderColor: 'border.default',
    bg: 'surface.subtle',
    p: { base: '3', md: '3.5' },
    boxShadow: 'none',
  });

  return (
    <PageShell>
      <div
        className={css({
          display: 'grid',
          gap: '4',
        })}
      >
        <div
          className={flex({
            align: 'flex-start',
            justify: 'space-between',
            gap: { base: '3', md: '4' },
            wrap: 'wrap',
          })}
        >
          <div
            className={css({
              display: 'grid',
              gap: '1',
              minWidth: { base: '0', md: '18rem' },
              flex: '1 1 20rem',
            })}
          >
            <div className={flex({ align: 'center', gap: '2.5' })}>
              {selectedPrime ? (
                <ProtocolLogo protocolName={selectedPrime.name} size="8" />
              ) : null}
              <h1
                className={css({
                  m: 0,
                  fontSize: { base: '3xl', md: '4xl' },
                  lineHeight: 'tight',
                  color: 'text.strong',
                })}
              >
                {selectedPrime ? selectedPrime.name : 'Select a prime'}
              </h1>
            </div>
            {selectedPrime ? <TokenAddress address={selectedPrime.id} /> : null}
          </div>
          {!showTopMetricsSkeleton ? (
            <div
              className={css({
                display: 'flex',
                flexWrap: 'wrap',
                gap: { base: '2.5', md: '4' },
                justifyContent: { base: 'flex-start', md: 'flex-end' },
                textAlign: { base: 'left', md: 'right' },
                flex: '1 1 22rem',
              })}
            >
              {summary ? (
                <div
                  className={css({
                    display: 'flex',
                    alignItems: 'center',
                    gap: '1.5',
                    flexWrap: 'wrap',
                    justifyContent: 'flex-end',
                  })}
                >
                  <span
                    className={css({
                      fontSize: 'sm',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                    })}
                  >
                    Latest activity{' '}
                    {summary.latestActivityAt
                      ? formatFreshnessLabel(summary.latestActivityAt)
                      : '—'}
                  </span>
                  <span
                    className={css({
                      fontSize: 'xs',
                      lineHeight: 'short',
                      color: 'text.muted',
                    })}
                  >
                    {summary.latestActivityAt
                      ? formatDateTime(summary.latestActivityAt)
                      : 'No indexed activity'}
                  </span>
                </div>
              ) : null}
              {selectedPrime ? (
                <div
                  className={css({
                    display: 'flex',
                    alignItems: 'center',
                    gap: '1.5',
                    flexWrap: 'wrap',
                    justifyContent: 'flex-end',
                  })}
                >
                  <span
                    className={css({
                      fontSize: 'sm',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                    })}
                  >
                    Debt sync{' '}
                    {isPrimeDebtLoading
                      ? 'Loading...'
                      : primeDebtSnapshot?.synced_at
                        ? formatFreshnessLabel(primeDebtSnapshot.synced_at)
                        : '—'}
                  </span>
                  <span
                    className={css({
                      fontSize: 'xs',
                      lineHeight: 'short',
                      color: 'text.muted',
                    })}
                  >
                    {isPrimeDebtLoading
                      ? 'Waiting for sync timestamp'
                      : primeDebtSnapshot?.synced_at
                        ? formatDateTime(primeDebtSnapshot.synced_at)
                        : 'No debt sync timestamp'}
                  </span>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
        {showTopMetricsSkeleton ? (
          <div
            className={css({
              display: 'grid',
              gridTemplateColumns: {
                base: '1fr',
                sm: 'repeat(2, minmax(0, 1fr))',
                lg: 'repeat(4, minmax(0, 1fr))',
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
                base: '1fr',
                sm: 'repeat(2, minmax(0, 1fr))',
                lg: 'repeat(4, minmax(0, 1fr))',
              },
              gap: '3',
            })}
          >
            {summary ? (
              <SummaryMetric
                className={metricsCardClassName}
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
            ) : null}

            {capitalMetrics ? (
              <>
                <SummaryMetric
                  className={metricsCardClassName}
                  label="Risk capital"
                  value={formatUsdValue(capitalMetrics.risk_capital)}
                  detail={
                    parseNumericValue(capitalMetrics.risk_to_capital_ratio) !==
                    null
                      ? `Risk-to-capital ${formatRatioPercent(capitalMetrics.risk_to_capital_ratio)}`
                      : undefined
                  }
                />
              </>
            ) : null}

            {capitalMetrics ? (
              <SummaryMetric
                className={metricsCardClassName}
                label="Total capital"
                value={formatUsdValue(capitalMetrics.total_capital)}
                detail={`Buffer ${formatUsdValue(capitalMetrics.capital_buffer)} · First loss ${formatUsdValue(capitalMetrics.first_loss_capital)}`}
              />
            ) : null}

            {selectedPrime ? (
              <>
                <SummaryMetric
                  className={metricsCardClassName}
                  label="Prime debt exposure"
                  value={
                    isPrimeDebtLoading
                      ? 'Loading...'
                      : formatWadValue(primeDebtSnapshot?.debt_wad)
                  }
                  detail={
                    isPrimeDebtLoading ? (
                      'Fetching latest debt snapshot'
                    ) : (
                      <div
                        className={css({
                          display: 'flex',
                          flexWrap: 'wrap',
                          alignItems: 'center',
                          gap: '1',
                        })}
                      >
                        <span>
                          Ilk {primeDebtSnapshot?.ilk_name ?? 'Unknown'}
                        </span>
                        <span aria-hidden="true">·</span>
                        <AppTooltip
                          ariaLabel={
                            primeDebtSnapshot?.debt_wad
                              ? `Exact raw WAD ${primeDebtSnapshot.debt_wad}`
                              : 'Raw WAD unavailable'
                          }
                          trigger={
                            <span
                              className={css({
                                textDecoration: 'underline',
                                textDecorationStyle: 'dotted',
                                textUnderlineOffset: '2px',
                              })}
                            >
                              {formatRawWadLabel(primeDebtSnapshot?.debt_wad)}
                            </span>
                          }
                          content={
                            primeDebtSnapshot?.debt_wad
                              ? `Exact raw WAD: ${primeDebtSnapshot.debt_wad}`
                              : 'Raw WAD unavailable'
                          }
                        />
                      </div>
                    )
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
          className={css({
            display: 'grid',
            gridTemplateColumns: {
              base: '1fr',
              lg: 'auto minmax(20rem, 24rem)',
            },
            gap: { base: '3', md: '4', lg: '5' },
            alignItems: 'end',
          })}
        >
          <span
            className={css({
              display: 'inline-flex',
              width: 'fit-content',
              alignItems: 'center',
              borderRadius: 'full',
              bg: { _dark: 'gray.700', base: 'gray.200' },
              px: '3',
              py: '1',
              fontSize: 'xs',
              fontWeight: 'semibold',
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: 'text.muted',
            })}
          >
            Allocations
          </span>
          <div
            className={css({
              minWidth: '0',
              width: '100%',
              justifySelf: { lg: 'end' },
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
            stretch
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
            stretch
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
            stretch
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        (isLoading || filteredAllocations.length > 0) ? (
          <div className={tableHeaderTypographyClassName}>
            <DataTable
              table={table}
              isLoading={isLoading}
              onRowClick={(allocation) =>
                onSelectAllocation(getAllocationKey(allocation))
              }
              getRowKey={getAllocationKey}
              selectedRowKey={selectedAllocationKey}
            />
          </div>
        ) : null}
      </div>
    </PageShell>
  );
}
