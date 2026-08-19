import {
  Grid,
  XYChart,
  LineSeries,
  AreaSeries,
  Tooltip,
  Axis,
  buildChartTheme,
  chartTokens,
  useContainerWidth,
} from '@archon-research/charting';
import {
  Badge,
  type ColumnDef,
  DataTable,
  EmptyState,
  ErrorState,
  SearchInput,
  SkeletonStack,
  type SortingState,
  useDataTable,
} from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getActionColorClass, getActionIcon } from '../../lib/activity';
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
  tableHeaderTypographyClassName,
  TokenAddress,
  TokenLogo,
} from '../shared';
import { TabNotePanel } from './tabs/TabStatePanels';

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
  isMultiChainPrime: boolean;
  noticeMessage: string | null;
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

const chartEmptyMessageClassName = css({
  m: 0,
  mt: '2',
  fontSize: 'xs',
  color: 'text.muted',
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
  if (isLoading) {
    // A single block at the chart's own footprint, so the placeholder fills the
    // same space and there's no jump (or floating box) when the real chart loads
    // in.
    return (
      <SkeletonStack
        count={1}
        itemHeight={CHART_HEIGHT}
        className={css({ mt: '2' })}
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

  return <MetricCardChart chart={chart} />;
}

// Split out of MetricCardTrend so the measured element mounts with the component
// that measures it: `useContainerWidth` observes on mount, and above the guards
// there is no node to observe yet.
function MetricCardChart({ chart }: { chart: MetricChartSpec }) {
  // Until the first measurement the kit's fallback width applies, which can
  // overhang a narrow card for a frame — clipped rather than allowed to widen
  // the card under the reader.
  const [measureRef, chartWidth] = useContainerWidth();
  const chartTheme = useMemo(
    () => buildSingleSeriesTheme(chart.stroke),
    [chart.stroke],
  );

  const values = chart.data.map((point) => point.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);

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
      className={css({
        mt: '2',
        width: 'full',
        minWidth: 0,
        overflowX: 'hidden',
      })}
    >
      <XYChart
        theme={chartTheme}
        width={chartWidth}
        height={CHART_HEIGHT}
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
                <div className={chartTooltipTitleClassName}>{datum.label}</div>
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
    </div>
  );
}

// Fill override for the `Badge` these chips render as: `Badge`'s `colorPalette`
// ships six status-flavoured hues, so it cannot give five strategy categories
// distinct fills, and its red would read as an alarm on a routine category. The
// `categorical.*` tokens encode grouping without status meaning, and their hue
// order matches `chart.series`, so a chip and its series line read as the same
// category. Everything else about the chip — radius, weight, size, padding — is
// the recipe's.
//
// One literal `css()` call per category, evaluated at module scope, so the cell
// picks a finished class name: see `lib/activity.tsx` for why Panda cannot
// extract a token path handed in as a variable.
const CATEGORY_CHIP_CLASS: Record<AllocationCategory | 'unknown', string> = {
  allocation: css({ bg: 'categorical.1.bg', color: 'categorical.1.fg' }),
  pol: css({ bg: 'categorical.2.bg', color: 'categorical.2.fg' }),
  psm3: css({ bg: 'categorical.3.bg', color: 'categorical.3.fg' }),
  asset: css({ bg: 'categorical.4.bg', color: 'categorical.4.fg' }),
  custody: css({ bg: 'categorical.5.bg', color: 'categorical.5.fg' }),
  // No override: `Badge`'s own subtle × neutral default is this fill.
  unknown: '',
};

function getCategoryChipClass(
  category: AllocationCategory | undefined,
): string {
  // `AllocationCategory` is a compile-time union over an unvalidated API response,
  // so a category the backend adds later arrives as an unlisted string. Keying on
  // own-property presence rather than `?? 'unknown'` means that renders the neutral
  // chip instead of an unstyled one -- matching how getCategoryLabel already
  // degrades.
  return category !== undefined && Object.hasOwn(CATEGORY_CHIP_CLASS, category)
    ? CATEGORY_CHIP_CLASS[category]
    : CATEGORY_CHIP_CLASS.unknown;
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
            // Tabular figures so amounts align down the column. Not the
            // column's `meta.mono`: this cell is a composite (logo, value,
            // address) and mono would restyle all of it.
            fontVariantNumeric: 'tabular-nums',
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

  const actionColorClass = getActionColorClass(
    allocation.latest_activity_action,
  );
  const actionIcon = getActionIcon(allocation.latest_activity_action);
  const magnitude = formatActivityMagnitude(allocation);

  return (
    <div>
      <div className={flex({ align: 'center', gap: '1.5' })}>
        {actionIcon ? (
          <span
            className={cx(css({ display: 'inline-flex' }), actionColorClass)}
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
            className={cx(
              css({
                fontSize: 'xs',
                fontWeight: 'medium',
                whiteSpace: 'nowrap',
              }),
              actionColorClass,
            )}
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

  return (
    <Badge className={getCategoryChipClass(category)}>
      {getCategoryLabel(category)}
    </Badge>
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

// riskByReceiptTokenId is built from a risk-capital call scoped to
// selectedPrime's own chain, so an allocation on a different chain has no
// entry there for the same reason a genuine non-applicable allocation
// doesn't: the map simply has nothing for its receipt_token_id. Distinguish
// the two so a real risk capital figure that is merely uncomputed for this
// chain doesn't read as the same "n/a" as an allocation no risk model
// applies to.
//
// The receipt_token_id check gates this to rows that could ever carry a
// figure. A null receipt_token_id (the Anchorage custody row, and every
// direct/bare holding) can never key into riskByReceiptTokenId regardless of
// chain, so without this check a mainnet-primary prime would flag its own
// off-chain custody row (chain_id 0) as a cross-chain mismatch and claim a
// figure exists but merely wasn't fetched, when "n/a" is the correct read.
function isRiskCapitalChainMismatch(
  selectedPrime: Prime | null,
  allocation: Allocation,
): boolean {
  return (
    allocation.receipt_token_id != null &&
    selectedPrime !== null &&
    allocation.chain_id !== selectedPrime.chain_id
  );
}

function AllocationRiskCapitalCell({
  entry,
  isChainMismatch,
}: {
  entry: AllocationRiskCapital | undefined;
  isChainMismatch: boolean;
}) {
  if (isChainMismatch) {
    return (
      <p
        title="Risk capital is not yet available for non-mainnet allocations."
        className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}
      >
        Not yet available
      </p>
    );
  }

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
  selectedPrime: Prime | null,
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
          getValue: (allocation) =>
            parseNumericValue(allocation.amount_usd) ?? NaN,
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
      // A chain-mismatched row sorts below genuine zeroes (-1) rather than
      // tying with them, since it isn't a real zero — it's a figure this
      // session never fetched.
      accessorFn: (allocation) =>
        isRiskCapitalChainMismatch(selectedPrime, allocation)
          ? -1
          : (appliedRiskCapitalUsd(riskByReceiptTokenId, allocation) ?? 0),
      cell: ({ row }) => (
        <AllocationRiskCapitalCell
          entry={lookupAllocationRiskCapital(
            riskByReceiptTokenId,
            row.original,
          )}
          isChainMismatch={isRiskCapitalChainMismatch(
            selectedPrime,
            row.original,
          )}
        />
      ),
      // No bar for n/a or chain-mismatched rows: NaN suppresses it (see
      // Balance for why not null).
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) =>
            isRiskCapitalChainMismatch(selectedPrime, allocation)
              ? NaN
              : (appliedRiskCapitalUsd(riskByReceiptTokenId, allocation) ??
                NaN),
          getValueText: () => null,
        },
        // Single-value USD cell, so the column can take mono + tabular figures
        // wholesale.
        mono: true,
        align: 'right',
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
  isMultiChainPrime,
  noticeMessage,
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
        selectedPrime,
      ),
    [chainLabels, localProtocols, riskByReceiptTokenId, selectedPrime],
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
        {noticeMessage === null ? null : (
          <TabNotePanel message={noticeMessage} />
        )}
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
                    {isMultiChainPrime ? (
                      <p className={chartEmptyMessageClassName}>
                        Trend unavailable for multi-chain primes.
                      </p>
                    ) : (
                      <MetricCardTrend
                        chart={allocationActivityChart}
                        isLoading={isChartsLoading}
                        // chartsErrorMessage tracks the primary (prime-debt) series
                        // only; supplementary cards degrade to their own fallback.
                        errorMessage={null}
                      />
                    )}
                  </div>
                }
              />
            ) : null}

            {riskCapital ? (
              <>
                <SummaryMetric
                  className={metricsCardClassName}
                  label="Exposure"
                  value={formatUsdValue(riskCapital.prime_exposure_usd)}
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

            {/* Takes the place of the two cards risk capital feeds, so a failed
                metric stays one cell wide in the rail instead of becoming a
                full-width banner under it. */}
            {!riskCapital && riskCapitalErrorMessage ? (
              // `alignSelf` so the panel is only as tall as its message: a grid
              // item would otherwise stretch to the chart cards' height and
              // render as a large empty block.
              <ErrorState
                className={css({ alignSelf: 'start' })}
                tone="critical"
                size="inline"
                title="Risk capital is unavailable"
                description="The risk capital endpoint failed for this session."
                errorMessage={riskCapitalErrorMessage}
              />
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
                      {formatUsdValue(
                        riskCapital.prime_required_risk_capital_usd,
                      )}
                      {parseNumericValue(
                        riskCapital.prime_encumbrance_ratio,
                      ) !== null
                        ? ` · Encumbrance ${formatRatioPercent(riskCapital.prime_encumbrance_ratio)}`
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
            {parseNumericValue(riskCapital.prime_modeled_pct) !== null
              ? formatRatioPercent(riskCapital.prime_modeled_pct)
              : 'partial'}{' '}
            of exposure modeled
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
              bg: 'bg.neutral',
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
            tone="critical"
            size="inline"
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
              density="compact"
              // Six nowrap columns push min-content well past this, so it binds
              // only on the loading skeleton, which has no intrinsic width.
              minWidth="48rem"
              skeletonConfig={{ rows: 8, firstColumnTall: true }}
            />
          </div>
        ) : null}
      </div>
    </PageShell>
  );
}
