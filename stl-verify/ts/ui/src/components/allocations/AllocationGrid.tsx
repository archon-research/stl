import {
  Grid,
  XYChart,
  LineSeries,
  AreaSeries,
  Tooltip,
  Axis,
  buildChartTheme,
  chartTokens,
} from '@archon-research/charting';
import {
  type ColumnDef,
  DataTable,
  EmptyState,
  ErrorState,
  SearchInput,
  type SortingState,
  useDataTable,
} from '@archon-research/design-system';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getActionColor, getActionIcon } from '../../lib/activity';
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
  AllocationRiskCapital,
  Prime,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
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
  riskCapital: PrimeRiskCapital | null;
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  topMetricsAllocations: Allocation[];
  isLoading: boolean;
  isRiskCapitalLoading: boolean;
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
  metricCharts: MetricChartSpec[];
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
  riskCapitalErrorMessage: string | null;
  primeDebtErrorMessage: string | null;
};

export type ChartDatum = {
  label: string;
  value: number;
};

export type MetricChartKey =
  | 'allocation-activity-volume'
  | 'risk-capital'
  | 'total-capital'
  | 'prime-debt-exposure';

// 'fallback' is a synthetic constant placeholder (current value repeated)
// shown when no real history is available; 'series' is a real time series.
// The card drops the area fill for fallbacks so they read as a flat baseline
// rather than a filled block.
export type MetricChartKind = 'series' | 'fallback';

export type MetricChartSpec = {
  key: MetricChartKey;
  data: ChartDatum[];
  stroke: string;
  formatValue: (value: number) => string;
  kind: MetricChartKind;
};

function findMetricChart(
  charts: MetricChartSpec[],
  key: MetricChartKey,
): MetricChartSpec | null {
  return charts.find((chart) => chart.key === key) ?? null;
}

const chartTooltipSurfaceClassName = css({
  borderColor: 'border.subtle',
  borderStyle: 'solid',
  borderWidth: '1px',
  borderRadius: 'md',
  background: 'surface.default',
  boxShadow: 'sm',
  px: '3',
  py: '2.5',
  fontSize: 'sm',
  width: 'fit-content',
  minW: '8rem',
});

const chartTooltipTitleClassName = css({
  fontWeight: 'semibold',
  color: 'text.default',
  mb: '1',
});

const chartTooltipValueClassName = css({
  fontSize: 'sm',
  fontWeight: 'medium',
});

function buildSingleSeriesTheme(stroke: string) {
  return buildChartTheme({
    backgroundColor: 'transparent',
    colors: [stroke],
    gridColor: chartTokens.grid,
    gridColorDark: chartTokens.grid,
    tickLength: 6,
    svgLabelSmall: { fill: chartTokens.label, fontSize: 11 },
    svgLabelBig: { fill: chartTokens.axis, fontSize: 12 },
    xAxisLineStyles: { stroke: chartTokens.axis },
    yAxisLineStyles: { stroke: chartTokens.axis },
    xTickLineStyles: { stroke: chartTokens.axis },
    yTickLineStyles: { stroke: chartTokens.axis },
  });
}

// Callback-ref based width measurement: a stable ref that wires up a
// ResizeObserver when the node mounts and tears it down on unmount. A bare
// useEffect+useRef would miss the mount because the measured node only renders
// after the loading/empty guards below resolve.
function useMeasuredWidth(): [
  (node: HTMLDivElement | null) => void,
  number | null,
] {
  const [width, setWidth] = useState<number | null>(null);
  const observerRef = useRef<ResizeObserver | null>(null);

  const measureRef = useCallback((node: HTMLDivElement | null) => {
    observerRef.current?.disconnect();

    if (!node) {
      return;
    }

    const measure = () => {
      const nextWidth = Math.floor(node.getBoundingClientRect().width);
      setWidth(nextWidth > 0 ? nextWidth : null);
    };

    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    observerRef.current = observer;
  }, []);

  return [measureRef, width];
}

const chartEmptyMessageClassName = css({
  m: 0,
  mt: '2',
  fontSize: 'xs',
  color: 'text.subtle',
});

const CHART_HEIGHT = 236;

function MetricCardTrend({
  chart,
  isLoading,
  errorMessage,
}: {
  chart: MetricChartSpec | null;
  isLoading: boolean;
  errorMessage: string | null;
}) {
  const [measureRef, chartWidth] = useMeasuredWidth();
  const chartTheme = useMemo(
    () => buildSingleSeriesTheme(chart?.stroke ?? chartTokens.axis),
    [chart?.stroke],
  );

  if (isLoading) {
    // Match the chart's footprint so the placeholder fills the same space and
    // there's no jump (or floating box) when the real chart loads in.
    return (
      <div
        style={{ height: CHART_HEIGHT }}
        className={css({
          mt: '2',
          borderRadius: 'sm',
          borderWidth: '1px',
          borderStyle: 'solid',
          borderColor: 'border.subtle',
          bg: 'surface.default',
        })}
      />
    );
  }

  if (errorMessage) {
    return (
      <p
        className={css({
          m: 0,
          mt: '2',
          fontSize: 'xs',
          color: 'text.warning',
        })}
      >
        Chart unavailable for this range.
      </p>
    );
  }

  if (!chart || chart.data.length === 0) {
    return (
      <p className={chartEmptyMessageClassName}>
        No trend data in this window.
      </p>
    );
  }

  const values = chart.data.map((point) => point.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const chartHeight = CHART_HEIGHT;

  // A constant series (the current-value fallback) has a degenerate [v, v]
  // domain whose area would fill the whole plot as a solid block; pad it so the
  // line sits centered, and drop the area fill so it reads as a flat baseline.
  const isFlat = minValue === maxValue;
  const flatPad = Math.max(Math.abs(minValue) * 0.5, 1);
  const yDomain: [number, number] = isFlat
    ? [minValue - flatPad, maxValue + flatPad]
    : [minValue, maxValue];

  return (
    <div
      ref={measureRef}
      className={css({ mt: '2', width: 'full', minWidth: 0 })}
    >
      {chartWidth === null ? (
        <div
          className={css({
            height: `${chartHeight}px`,
            width: 'full',
          })}
        />
      ) : null}
      {chartWidth !== null ? (
        <XYChart
          theme={chartTheme}
          width={chartWidth}
          height={chartHeight}
          margin={{ top: 8, right: 24, bottom: 76, left: 64 }}
          xScale={{ type: 'band', paddingInner: 0.2 }}
          yScale={{ type: 'linear', domain: yDomain, nice: !isFlat }}
        >
          <Grid columns={false} numTicks={3} />
          <Axis
            orientation="bottom"
            numTicks={4}
            hideTicks
            tickLabelProps={() => ({
              fontSize: 10,
              textAnchor: 'end',
              angle: -35,
              dx: '-0.25em',
              dy: '0.25em',
              fill: 'var(--colors-text-muted)',
            })}
          />
          {chart.kind === 'fallback' ? null : (
            <AreaSeries
              dataKey={`${chart.key}-area`}
              data={chart.data as ChartDatum[]}
              xAccessor={(d: ChartDatum) => d.label}
              yAccessor={(d: ChartDatum) => d.value}
              fill={chart.stroke}
              fillOpacity={0.18}
              lineProps={{ stroke: 'none' }}
            />
          )}
          <LineSeries
            dataKey={chart.key}
            data={chart.data as ChartDatum[]}
            xAccessor={(d: ChartDatum) => d.label}
            yAccessor={(d: ChartDatum) => d.value}
            stroke={chart.stroke}
          />
          <Tooltip
            snapTooltipToDatumX
            snapTooltipToDatumY
            showVerticalCrosshair
            showSeriesGlyphs
            renderTooltip={({
              tooltipData,
            }: {
              tooltipData?: { nearestDatum?: { datum: unknown } };
            }) => {
              const datum = tooltipData?.nearestDatum?.datum as
                | ChartDatum
                | undefined;
              if (!datum) return null;
              return (
                <div className={chartTooltipSurfaceClassName}>
                  <div className={chartTooltipTitleClassName}>
                    {datum.label}
                  </div>
                  <div
                    className={chartTooltipValueClassName}
                    style={{ color: chart.stroke }}
                  >
                    {chart.formatValue(datum.value)}
                  </div>
                </div>
              );
            }}
          />
        </XYChart>
      ) : null}
    </div>
  );
}

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

// Approximate the latest flow's USD value from the position's current implied
// price (amount_usd / balance) rather than a historical price: the activity
// row carries only a token-unit tx_amount, and this is a magnitude annotation,
// not an accounting figure. Falls back to the token amount when unpriced.
function formatActivityMagnitude(allocation: Allocation): string | null {
  const amount = parseNumericValue(allocation.latest_activity_amount);
  // Sweeps are internal reallocations with tx_amount 0; show the icon alone
  // rather than a misleading "$0.00".
  if (amount === null || amount === 0) {
    return null;
  }

  const action = allocation.latest_activity_action?.toLowerCase();
  const sign = action === 'in' ? '+' : action === 'out' ? '-' : '';

  const balance = parseNumericValue(allocation.balance);
  const amountUsd = parseNumericValue(allocation.amount_usd);
  if (amountUsd !== null && balance !== null && balance > 0) {
    return `${sign}${formatUsdValue(amount * (amountUsd / balance))}`;
  }

  return `${sign}${formatTokenAmount(amount)} ${allocation.symbol}`;
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

  const actionColor = getActionColor(allocation.latest_activity_action);
  const actionIcon = getActionIcon(allocation.latest_activity_action);
  const magnitude = formatActivityMagnitude(allocation);

  return (
    <div>
      <div className={flex({ align: 'center', gap: '1.5' })}>
        {actionIcon ? (
          <span
            className={css({ display: 'inline-flex', color: actionColor })}
          >
            {actionIcon}
          </span>
        ) : null}
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {formatFreshnessLabel(allocation.latest_activity_at)}
        </span>
        {magnitude ? (
          <span
            className={css({
              fontSize: 'xs',
              fontWeight: 'medium',
              color: actionColor,
              whiteSpace: 'nowrap',
            })}
          >
            {magnitude}
          </span>
        ) : null}
      </div>
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

function lookupAllocationRiskCapital(
  riskByReceiptTokenId: Map<number, AllocationRiskCapital>,
  allocation: Allocation,
): AllocationRiskCapital | undefined {
  if (
    allocation.receipt_token_id === undefined ||
    allocation.receipt_token_id === null
  ) {
    return undefined;
  }

  return riskByReceiptTokenId.get(allocation.receipt_token_id);
}

// Applied required risk capital in USD, or null when none applies. Shared by the
// column accessor and the magnitude bar so the two cannot diverge on the rule.
function appliedRiskCapitalUsd(
  riskByReceiptTokenId: Map<number, AllocationRiskCapital>,
  allocation: Allocation,
): number | null {
  const entry = lookupAllocationRiskCapital(riskByReceiptTokenId, allocation);
  return entry?.applied
    ? parseNumericValue(entry.required_risk_capital_usd)
    : null;
}

function AllocationRiskCapitalCell({
  entry,
}: {
  entry: AllocationRiskCapital | undefined;
}) {
  if (!entry?.applied) {
    return (
      <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>n/a</p>
    );
  }

  return (
    <p
      className={css({
        m: 0,
        fontSize: 'sm',
        fontWeight: 'semibold',
        color: 'text.strong',
      })}
    >
      {formatUsdValue(entry.required_risk_capital_usd)}
    </p>
  );
}

function createAllocationColumns(
  chainLabels: ChainLabelLookup,
  localProtocols: LocalProtocolRow[],
  riskByReceiptTokenId: Map<number, AllocationRiskCapital>,
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
      // Bar reflects USD value so magnitudes compare across heterogeneous
      // tokens; the cell text keeps the token holding. NaN (not null) suppresses
      // the bar for unpriced rows: a null here would fall back to the column
      // accessor (token balance), mixing token units into the USD domain.
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) => parseNumericValue(allocation.amount_usd) ?? NaN,
          getValueText: () => null,
        },
      },
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
    {
      id: 'risk_capital',
      header: 'Risk capital',
      accessorFn: (allocation) =>
        appliedRiskCapitalUsd(riskByReceiptTokenId, allocation) ?? 0,
      cell: ({ row }) => (
        <AllocationRiskCapitalCell
          entry={lookupAllocationRiskCapital(
            riskByReceiptTokenId,
            row.original,
          )}
        />
      ),
      // No bar for n/a rows: NaN suppresses it (see Balance for why not null).
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) =>
            appliedRiskCapitalUsd(riskByReceiptTokenId, allocation) ?? NaN,
          getValueText: () => null,
        },
      },
    },
  ];
}

export function AllocationGrid({
  allocations,
  riskCapital,
  chainLabels,
  errorMessage,
  filteredAllocations,
  topMetricsAllocations,
  isLoading,
  isRiskCapitalLoading,
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
  metricCharts,
  isChartsLoading,
  chartsErrorMessage,
  riskCapitalErrorMessage,
  primeDebtErrorMessage,
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

  const riskByReceiptTokenId = useMemo(() => {
    const map = new Map<number, AllocationRiskCapital>();
    for (const entry of riskCapital?.per_allocation ?? []) {
      map.set(entry.receipt_token_id, entry);
    }
    return map;
  }, [riskCapital]);

  const columns = useMemo<ColumnDef<Allocation>[]>(
    () =>
      createAllocationColumns(
        chainLabels,
        localProtocols,
        riskByReceiptTokenId,
      ),
    [chainLabels, localProtocols, riskByReceiptTokenId],
  );

  const table = useDataTable(filteredAllocations, columns, {
    enableSorting: true,
    onSortingChange,
    sorting,
  });

  const showTopMetricsSkeleton =
    selectedPrime !== null && (isLoading || isRiskCapitalLoading);

  const hasTopMetrics =
    riskCapital !== null || summary !== null || selectedPrime !== null;

  const metricsCardClassName = css({
    borderRadius: 'sm',
    borderStyle: 'solid',
    borderWidth: '1px',
    borderColor: 'border.default',
    bg: 'surface.subtle',
    p: { base: '3', md: '3.5' },
    boxShadow: 'none',
    display: 'flex',
    flexDirection: 'column',
    // Uniform gap between label, value, and detail. Avoids `space-between`,
    // which stretched the slack between the value and the subtitle unevenly
    // across cards. The detail's fixed min-height keeps the chart row aligned.
    gap: '2',
  });

  const metricDetailClassName = css({
    display: 'grid',
    gridTemplateRows: 'auto 1fr',
    gap: '2',
    minHeight: '17rem',
  });

  const allocationActivityChart = findMetricChart(
    metricCharts,
    'allocation-activity-volume',
  );
  const riskCapitalChart = findMetricChart(metricCharts, 'risk-capital');
  const totalCapitalChart = findMetricChart(metricCharts, 'total-capital');
  const primeDebtChart = findMetricChart(metricCharts, 'prime-debt-exposure');

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
                      : primeDebtErrorMessage
                        ? 'Error'
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
                      : primeDebtErrorMessage
                        ? primeDebtErrorMessage
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
                lg: 'repeat(2, minmax(0, 1fr))',
                '2xl': 'repeat(4, minmax(0, 1fr))',
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
                lg: 'repeat(2, minmax(0, 1fr))',
                '2xl': 'repeat(4, minmax(0, 1fr))',
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
                  <div className={metricDetailClassName}>
                    <div
                      className={css({ fontSize: 'sm', color: 'text.muted' })}
                    >
                      {hasSearchQuery && overallSummary
                        ? `${summary.allocationCount}/${overallSummary.allocationCount} allocations`
                        : `${summary.allocationCount} allocations`}
                    </div>
                    <MetricCardTrend
                      chart={allocationActivityChart}
                      isLoading={isChartsLoading}
                      // chartsErrorMessage tracks the primary (prime-debt) series
                      // only; supplementary cards degrade to their own fallback.
                      errorMessage={null}
                    />
                  </div>
                }
              />
            ) : null}

            {riskCapital ? (
              <>
                <SummaryMetric
                  className={metricsCardClassName}
                  label="Exposure"
                  value={formatUsdValue(riskCapital.exposure_usd)}
                  detail={
                    <div className={metricDetailClassName}>
                      <MetricCardTrend
                        chart={riskCapitalChart}
                        isLoading={isChartsLoading}
                        errorMessage={null}
                      />
                    </div>
                  }
                />
              </>
            ) : null}

            {riskCapital ? (
              <SummaryMetric
                className={metricsCardClassName}
                label="Total risk capital"
                value={formatUsdValue(
                  riskCapital.total_risk_capital_usd ?? '0',
                )}
                detail={
                  <div className={metricDetailClassName}>
                    <div
                      className={css({ fontSize: 'sm', color: 'text.muted' })}
                    >
                      Required{' '}
                      {formatUsdValue(riskCapital.required_risk_capital_usd)}
                      {parseNumericValue(riskCapital.encumbrance_ratio) !== null
                        ? ` · Encumbrance ${formatRatioPercent(riskCapital.encumbrance_ratio)}`
                        : ''}
                    </div>
                    <MetricCardTrend
                      chart={totalCapitalChart}
                      isLoading={isChartsLoading}
                      errorMessage={null}
                    />
                  </div>
                }
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
                      <div className={metricDetailClassName}>
                        <div
                          className={css({
                            display: 'flex',
                            flexWrap: 'wrap',
                            alignItems: 'baseline',
                            gap: '1',
                            fontSize: 'sm',
                            color: 'text.muted',
                            // The tooltip trigger is a 44px-min tap target; inline
                            // here it would inflate the row and drop the text below
                            // the other cards' single-line subtitles. Collapse it to
                            // the text line height so the baselines align.
                            '& button': { minHeight: 'auto', py: '0' },
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
                        <MetricCardTrend
                          chart={primeDebtChart}
                          isLoading={isChartsLoading}
                          errorMessage={chartsErrorMessage}
                        />
                      </div>
                    )
                  }
                />
              </>
            ) : null}
          </div>
        ) : null}
        {!showTopMetricsSkeleton && riskCapital ? (
          <p
            className={css({
              m: 0,
              fontSize: 'xs',
              color: 'text.muted',
            })}
          >
            Model-derived ({riskCapital.model}, 15% stress) ·{' '}
            {parseNumericValue(riskCapital.modeled_pct) !== null
              ? formatRatioPercent(riskCapital.modeled_pct)
              : 'partial'}{' '}
            of exposure modeled
          </p>
        ) : null}
        {!showTopMetricsSkeleton && riskCapitalErrorMessage ? (
          <ErrorState
            title="Risk capital is unavailable"
            description="The risk capital endpoint failed for this session."
            errorMessage={riskCapitalErrorMessage}
          />
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
