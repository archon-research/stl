import {
  Grid,
  XYChart,
  LineSeries,
  AreaSeries,
  Tooltip,
  Axis,
  buildChartTheme,
  type ChartColor,
  type ChartColorToken,
  chartColorToken,
  chartTokens,
  resolveChartColor,
  useContainerWidth,
} from '@archon-research/charting';
import { DataContext, ReferenceBand } from '@archon-research/charting';
import { ErrorState, SkeletonStack } from '@archon-research/design-system';
import type { CSSProperties } from 'react';
import { useContext, useMemo } from 'react';

import { css } from '#styled-system/css';

import { balancedColumns } from '../../lib/dashboard';
import { SummaryMetric } from '../shared';

export type ChartDatum = {
  label: string;
  value: number;
};

export type MetricChartKey =
  | 'allocation-activity-volume'
  | 'risk-capital'
  | 'total-capital'
  | 'prime-debt-exposure'
  | 'prime-collateral'
  | 'encumbrance-ratio';

// 'fallback' is a synthetic constant placeholder (current value repeated)
// shown when no real history is available; 'series' is a real time series.
// The card drops the area fill for fallbacks so they read as a flat baseline
// rather than a filled block.
export type MetricChartKind = 'series' | 'fallback';

export type MetricChartSpec = {
  key: MetricChartKey;
  data: ChartDatum[];
  // A token name, not a colour: `chartColorToken` resolves it where a raw visx
  // prop or a `style` object needs the CSS value.
  stroke: ChartColorToken;
  formatValue: (value: number) => string;
  kind: MetricChartKind;
  // Ordered ascending. Each draws a dashed limit with the region past it shaded,
  // so overlapping severities read as escalating shade.
  //
  thresholds?: { value: number; label?: string; stroke?: ChartColor }[];
  // The provenance not drawn as the primary series, for the same buckets, under
  // `source=both`. A second line rather than a second card: the point is the gap
  // between them. `ChartColor`, not a series token: it is deliberately not one of
  // the series hues, so it cannot be mistaken for a quantity of its own.
  comparison?: { data: ChartDatum[]; stroke: ChartColor } | null;
};

// Every card the metrics band can render. Its length drives both the loading
// placeholders and the column count, so neither can drift from the cards.
export const TOP_METRIC_CARDS = [
  'total-allocation',
  'exposure',
  'total-risk-capital',
  'prime-collateral',
  'encumbrance',
  'prime-debt',
] as const;

export type TopMetricCard = (typeof TOP_METRIC_CARDS)[number];

// A card that is loading or unavailable still knows which metric it is, which
// is the difference between "six grey boxes" and a page you can read early.
export const TOP_METRIC_CARD_LABELS: Record<TopMetricCard, string> = {
  'total-allocation': 'Total allocation',
  exposure: 'Exposure',
  'total-risk-capital': 'Total risk capital',
  'prime-collateral': 'Prime collateral',
  encumbrance: 'Encumbrance',
  'prime-debt': 'Prime debt exposure',
};

export const metricsGridClassName = css({
  display: 'grid',
  gridTemplateColumns: {
    base: '1fr',
    lg: 'repeat(var(--metric-columns-lg), minmax(0, 1fr))',
    '2xl': 'repeat(var(--metric-columns-2xl), minmax(0, 1fr))',
  },
  gap: '3',
});

// The counts are per-breakpoint and computed, so they ride custom properties:
// panda generates its classes at build time and cannot see a runtime value.
export function metricsGridStyle(count: number): CSSProperties {
  return {
    '--metric-columns-lg': balancedColumns(count, 2),
    '--metric-columns-2xl': balancedColumns(count, 4),
  } as CSSProperties;
}

export function findMetricChart(
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

export const chartEmptyMessageClassName = css({
  m: 0,
  mt: '2',
  fontSize: 'xs',
  color: 'text.muted',
});

const CHART_HEIGHT = 236;

type ThresholdEntry = NonNullable<MetricChartSpec['thresholds']>[number];

const THRESHOLD_LABEL_FONT_SIZE = 11;

/**
 * Threshold labels, drawn here rather than by `ReferenceBand`.
 *
 * The kit pins every label to the same x at its own line's y, so two limits a
 * few percentage points apart land on top of each other. Splitting them — the
 * highest above its line, the rest below theirs — puts each on the outside of
 * the band it bounds and separates them by two label heights.
 *
 * Reads the committed scale from `DataContext` instead of recomputing it: the
 * chart passes `nice`, which adjusts the domain, so a locally derived scale
 * would place the text off its own line.
 */
function ThresholdLabels({ thresholds }: { thresholds: ThresholdEntry[] }) {
  const context = useContext(DataContext);
  const yScale = context?.yScale as
    | ((value: number) => number | undefined)
    | undefined;
  const marginLeft = context?.margin?.left;

  if (yScale === undefined || marginLeft === undefined) {
    return null;
  }

  const labelled = thresholds.filter((entry) => entry.label !== undefined);
  if (labelled.length === 0) {
    return null;
  }

  const highest = Math.max(...labelled.map((entry) => entry.value));

  return (
    <g data-part="threshold-labels" pointerEvents="none">
      {labelled.map((entry) => {
        const y = yScale(entry.value);
        if (y === undefined || !Number.isFinite(y)) {
          return null;
        }

        const isHighest = entry.value === highest;
        return (
          <text
            key={`threshold-label-${entry.value}`}
            x={marginLeft + 6}
            y={y + (isHighest ? -6 : THRESHOLD_LABEL_FONT_SIZE + 3)}
            fill={
              entry.stroke === undefined
                ? undefined
                : resolveChartColor(entry.stroke)
            }
            fontSize={THRESHOLD_LABEL_FONT_SIZE}
          >
            {entry.label}
          </text>
        );
      })}
    </g>
  );
}

/**
 * A card whose figure has not arrived yet.
 *
 * Built from the same frame as the real card rather than a single grey block:
 * the label is known up front, so the page reads as itself while it loads and
 * nothing moves when the figures land.
 */
// Not `SkeletonStack`: it fills its items with `surface.subtle`, which is this
// card's own fill, and takes no tone — so its placeholders are invisible here
// and neither a composed class nor a descendant override outranks the kit's own
// layer.
const placeholderClassName = css({
  bg: 'border.subtle',
  borderRadius: 'sm',
  animation: 'pulse',
});

function Placeholder({ width, height }: { width: string; height: number }) {
  return (
    <div
      className={placeholderClassName}
      // Sizes vary per slot, so they ride the style attribute: Panda generates
      // its classes at build time and cannot see a value passed in.
      style={{ width, height: `${height}px` }}
    />
  );
}

export function MetricCardSkeleton({ label }: { label: string }) {
  return (
    <SummaryMetric
      className={metricsCardClassName}
      label={label}
      // Widths are a typical figure and subtitle rather than the full column: a
      // placeholder the width of the card reads as a filled card, not a loading
      // one.
      value={<Placeholder width="8rem" height={28} />}
      detail={
        <div className={metricDetailClassName}>
          <Placeholder width="12rem" height={16} />
          <Placeholder width="100%" height={CHART_HEIGHT} />
        </div>
      }
    />
  );
}

/**
 * A card whose figure could not be fetched.
 *
 * Keeps the card's frame and height so a row does not reflow when a retry
 * succeeds, and names the metric so which one failed is visible.
 */
export function MetricCardError({
  label,
  title,
  description,
  errorMessage,
}: {
  label: string;
  title: string;
  description: string;
  errorMessage: string | null;
}) {
  return (
    <SummaryMetric
      className={metricsCardClassName}
      label={label}
      value="—"
      detail={
        <div className={metricDetailClassName}>
          <ErrorState
            tone="critical"
            size="inline"
            title={title}
            description={description}
            errorMessage={errorMessage ?? undefined}
          />
        </div>
      }
    />
  );
}

export function MetricCardTrend({
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
  // `AreaSeries`/`LineSeries` and the tooltip `style` are raw visx surfaces that
  // resolve no token names, so the token becomes a CSS value exactly once here.
  const strokeColor = chartColorToken(chart.stroke);
  const chartTheme = useMemo(
    () => buildSingleSeriesTheme(strokeColor),
    [strokeColor],
  );

  const values = [
    ...chart.data.map((point) => point.value),
    ...(chart.comparison?.data ?? []).map((point) => point.value),
  ];
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);

  // A constant series (the current-value fallback) has a degenerate [v, v]
  // domain whose area would fill the whole plot as a solid block; pad it so the
  // line sits centered, and drop the area fill so it reads as a flat baseline.
  const isFlat = minValue === maxValue;
  // Proportional, so a 0-1 ratio does not get a whole unit of padding; the
  // literal is only reached when the value is exactly zero.
  const flatPad = Math.abs(minValue) * 0.5 || 1;
  const [domainMin, domainMax] = isFlat
    ? [minValue - flatPad, maxValue + flatPad]
    : [minValue, maxValue];

  const thresholds = chart.thresholds ?? [];
  const yDomain: [number, number] = (() => {
    if (thresholds.length === 0) {
      return [domainMin, domainMax];
    }
    // A limit outside the domain renders off-plot, reading as "no threshold"
    // rather than "well within it" — but pinning the topmost one to the domain
    // edge is no better: ReferenceBand drops the whole band, dashed line and
    // label included, once its breach region has zero height. So the
    // series-below-the-limit case, which is the one worth showing, needs
    // headroom above the highest limit.
    const limits = thresholds.map((entry) => entry.value);
    const highest = Math.max(...limits);
    const low = Math.min(domainMin, ...limits);
    const high = Math.max(domainMax, highest);
    const headroom = (high - low) * 0.1 || Math.abs(highest) * 0.1 || 1;
    return [low, high === highest ? high + headroom : high];
  })();

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
        {/* No fill when a second series is present: it anchors the domain at
            zero, which compresses both lines into a band at the plot top, and a
            comparison does not need a filled magnitude anyway. */}
        {chart.kind === 'fallback' || chart.comparison ? null : (
          <AreaSeries
            dataKey={`${chart.key}-area`}
            data={chart.data as ChartDatum[]}
            xAccessor={(d: ChartDatum) => d.label}
            yAccessor={(d: ChartDatum) => d.value}
            fill={strokeColor}
            fillOpacity={0.18}
            lineProps={{ stroke: 'none' }}
          />
        )}
        <LineSeries
          dataKey={chart.key}
          data={chart.data as ChartDatum[]}
          xAccessor={(d: ChartDatum) => d.label}
          yAccessor={(d: ChartDatum) => d.value}
          stroke={strokeColor}
        />
        {chart.comparison && chart.comparison.data.length > 0 ? (
          // Dashed and unfilled: the same quantity as the primary series, from
          // the other provenance, which should not read as a second quantity
          // stacked on the first.
          <LineSeries
            dataKey={`${chart.key}-comparison`}
            data={chart.comparison.data}
            xAccessor={(d: ChartDatum) => d.label}
            yAccessor={(d: ChartDatum) => d.value}
            stroke={resolveChartColor(chart.comparison.stroke)}
            // Heavier than a hairline: where the two provenances agree the
            // lines coincide exactly, and the series colour showing through the
            // gaps is what tells a reader that is what happened.
            strokeWidth={1.5}
            strokeDasharray="6 4"
          />
        ) : null}
        {thresholds.map((entry) => (
          <ReferenceBand
            key={`threshold-${entry.value}`}
            mode="threshold"
            value={entry.value}
            breach="above"
            stroke={entry.stroke}
          />
        ))}
        <ThresholdLabels thresholds={thresholds} />
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
                  style={{ color: strokeColor }}
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

// Card and detail framing, shared by every metric card so one cannot drift from
// its neighbours.
export const metricsCardClassName = css({
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

export const metricDetailClassName = css({
  display: 'grid',
  gridTemplateRows: 'auto 1fr',
  gap: '2',
  minHeight: '17rem',
  // One line whether or not it has text, since only some cards carry an
  // observation stamp and an empty row lifts the chart out of line with its
  // row-mates. `1lh` resolves against the caption's own font, not the card's.
  '& > :first-child': { minHeight: '1lh' },
});
