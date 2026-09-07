import {
  AreaSeries,
  Axis,
  buildChartTheme,
  ChartCursorLayer,
  ChartLegend,
  type ChartLegendItem,
  chartColorToken,
  type ChartColorToken,
  chartTokens,
  DataContext,
  Grid,
  LineSeries,
  ReferenceBand,
  resolveChartColor,
  Tooltip,
  useContainerWidth,
  useHoveredTimestamp,
  useSyncedCursorHandlers,
  XYChart,
} from '@archon-research/charting';
import { SkeletonStack } from '@archon-research/design-system';
import { useContext, useMemo } from 'react';

import { css } from '#styled-system/css';

import {
  CHART_HEIGHT,
  type ChartDatum,
  type MetricChartSpec,
} from './metricCards';

/**
 * The plotted half of a metric card: its legend key and its trend chart.
 *
 * Split from `metricCards.tsx` so the card frame, the grid and the skeletons
 * stay free of `@archon-research/charting`. That is what lets the whole metrics
 * band be a dynamic import while the placeholder it falls back to is not.
 */

// A threshold that names no colour of its own is drawn in the axis hue, so its
// key has to be too.
const THRESHOLD_LEGEND_COLOR: ChartColorToken = 'chart.axis';

/**
 * Whether `MetricCardTrend` draws the chart rather than one of its three
 * fallbacks. The legend hangs off the same answer: a key beside a skeleton, an
 * error line, or "no trend data" names series that nothing drew.
 */
function drawsMetricChart(
  chart: MetricChartSpec | null,
  isLoading: boolean,
  errorMessage: string | null,
): chart is MetricChartSpec {
  return (
    !isLoading &&
    errorMessage === null &&
    chart !== null &&
    chart.data.length > 0
  );
}

/**
 * The chart's key: its series, and nothing else.
 *
 * Limits are deliberately absent. Each already draws its own label against its
 * line, where the value it marks is legible, and a named one repeats in the
 * cursor tooltip — a third copy in the header only crowded the row and pushed
 * the key off the title line.
 */
function metricLegendItems(
  chart: MetricChartSpec,
  seriesLabel: string,
): ChartLegendItem[] {
  return [{ id: chart.key, label: seriesLabel, color: chart.stroke }];
}

/** The card header's chart key. Renders nothing when no chart is drawn. */
export function MetricCardLegend({
  chart,
  seriesLabel,
  isLoading,
  errorMessage = null,
}: {
  chart: MetricChartSpec | null;
  seriesLabel: string;
  isLoading: boolean;
  errorMessage?: string | null;
}) {
  if (!drawsMetricChart(chart, isLoading, errorMessage)) {
    return null;
  }

  // `shape="line"` matches what the plot draws — every series here is a line —
  // and `dash` carries the solid-series vs dashed-limit distinction the
  // chart itself uses. Unwrapped: `MetricCard`'s legend row owns the styling,
  // so the row still holds its height when this renders nothing.
  return (
    <ChartLegend
      interactive={false}
      shape="line"
      items={metricLegendItems(chart, seriesLabel)}
    />
  );
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
  width: 'fit',
  minW: '32',
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
  m: '0',
  mt: '2',
  fontSize: 'xs',
  color: 'text.muted',
});

// Shared with the cursor tooltip, which positions itself inside these margins —
// a second copy would drift and park the tooltip outside the plot.
const CHART_MARGIN = { top: 8, right: 24, bottom: 76, left: 64 };

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
// visx types the context's scales as the union of every d3 scale, which is not
// callable with a plain number; this chart's y scale is always numeric.
function isNumericScale(
  scale: unknown,
): scale is (value: number) => number | undefined {
  return typeof scale === 'function';
}

function ThresholdLabels({ thresholds }: { thresholds: ThresholdEntry[] }) {
  const context = useContext(DataContext);
  const yScale = context?.yScale;
  const marginLeft = context?.margin?.left;

  if (!isNumericScale(yScale) || marginLeft === undefined) {
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
          m: '0',
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

  const values = [...chart.data.map((point) => point.value)];
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);

  // A constant series (the current-value placeholder shown when no history is
  // available) has a degenerate [v, v] domain whose area would fill the whole
  // plot as a solid block; pad it so the line sits centered in the plot.
  const isFlat = minValue === maxValue;
  // Proportional, so a 0-1 ratio does not get a whole unit of padding; the
  // literal is only reached when the value is exactly zero.
  const flatPad = Math.abs(minValue) * 0.5 || 1;
  const [domainMin, domainMax] = isFlat
    ? [minValue - flatPad, maxValue + flatPad]
    : [minValue, maxValue];

  const thresholds = chart.thresholds ?? [];
  const tooltipThresholds = thresholds.filter((entry) => entry.showInTooltip);
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

  // A series joins the synced cursor only if every point is a real observation.
  // The placeholder a card falls back to has no instants, so it plots against
  // its labels and shows no crosshair rather than inventing a time to snap to.
  const stops = useMemo(() => {
    const timestamps = chart.data.map((point) => point.timestamp);
    return timestamps.every((value): value is number => value !== null)
      ? [...timestamps].sort((left, right) => left - right)
      : null;
  }, [chart.data]);

  // The band domain is the instant, so the axis needs the label back. Built
  // from the same points the scale was, so a tick can never label the wrong
  // bucket.
  const labelAt = useMemo(
    () =>
      new Map(
        chart.data.flatMap((point) =>
          point.timestamp === null ? [] : [[point.timestamp, point.label]],
        ),
      ),
    [chart.data],
  );

  const valueAt = useMemo(() => {
    const byTimestamp = new Map(
      chart.data.flatMap((point) =>
        point.timestamp === null ? [] : [[point.timestamp, point.value]],
      ),
    );
    return (x: number) => byTimestamp.get(x) ?? null;
  }, [chart.data]);

  const [hoveredTimestamp] = useHoveredTimestamp();
  // Reads the instant off the hovered datum rather than inverting a pixel, so
  // sibling cards line up on the bucket a reader is actually over even though
  // they bucket at different resolutions and start at different points.
  const cursorHandlers = useSyncedCursorHandlers<ChartDatum>(
    (point) => point.timestamp ?? Number.NaN,
  );

  const xAccessor = (point: ChartDatum): string | number =>
    stops === null || point.timestamp === null ? point.label : point.timestamp;

  const cursorSeries = [{ id: chart.key, color: chart.stroke, valueAt }];

  return (
    <div
      ref={measureRef}
      className={css({
        mt: '2',
        width: 'full',
        minWidth: '0',
        overflowX: 'hidden',
      })}
    >
      <XYChart
        theme={chartTheme}
        width={chartWidth}
        height={CHART_HEIGHT}
        margin={CHART_MARGIN}
        xScale={{ type: 'band', paddingInner: 0.2 }}
        yScale={{ type: 'linear', domain: yDomain, nice: !isFlat }}
        // Withheld from a placeholder: its points carry no instant, so the
        // accessor would publish NaN to the shared cursor. `nearestStop`
        // compares against NaN, every comparison is false, and it returns the
        // upper stop rather than clearing — so hovering a card with no history
        // jumped every other card's crosshair to an arbitrary bucket.
        onPointerMove={
          stops === null ? undefined : cursorHandlers.onPointerMove
        }
        onPointerOut={stops === null ? undefined : cursorHandlers.onPointerOut}
      >
        <Grid columns={false} numTicks={3} />
        <Axis
          orientation="bottom"
          numTicks={4}
          hideTicks
          tickFormat={(value: string | number) =>
            typeof value === 'number' ? (labelAt.get(value) ?? '') : value
          }
          tickLabelProps={() => ({
            fontSize: 10,
            textAnchor: 'end',
            angle: -35,
            dx: '-0.25em',
            dy: '0.25em',
            fill: 'var(--colors-text-muted)',
          })}
        />
        {/* The same soft fill under every line. The explicit `yDomain` above
            means the fill never moves the scale. */}
        <AreaSeries
          dataKey={`${chart.key}-area`}
          data={chart.data as ChartDatum[]}
          xAccessor={xAccessor}
          yAccessor={(d: ChartDatum) => d.value}
          fill={strokeColor}
          fillOpacity={0.18}
          lineProps={{ stroke: 'none' }}
        />
        <LineSeries
          dataKey={chart.key}
          data={chart.data as ChartDatum[]}
          xAccessor={xAccessor}
          yAccessor={(d: ChartDatum) => d.value}
          stroke={strokeColor}
        />
        {thresholds.map((entry) => (
          <ReferenceBand
            key={`threshold-${entry.value}`}
            mode="threshold"
            value={entry.value}
            breach="above"
            stroke={entry.stroke}
            // No breach fill: shading everything past the limit made a small
            // card read as mostly-in-breach even at a healthy ratio.
            fill="transparent"
          />
        ))}
        <ThresholdLabels thresholds={thresholds} />
        {stops === null ? (
          // No shared cursor without instants to share, so the placeholder
          // keeps visx's own tooltip and its local crosshair.
          <Tooltip
            snapTooltipToDatumX
            snapTooltipToDatumY
            showVerticalCrosshair
            showSeriesGlyphs
            renderTooltip={({
              tooltipData,
            }: {
              tooltipData?: { nearestDatum?: { datum: ChartDatum } };
            }) => {
              const datum = tooltipData?.nearestDatum?.datum;
              if (!datum) return null;
              return (
                <div className={chartTooltipSurfaceClassName}>
                  <div className={chartTooltipTitleClassName}>
                    {datum.label}
                  </div>
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
        ) : (
          // Driven by the shared timestamp rather than this chart's own pointer
          // state, so hovering any card moves the crosshair in all of them. It
          // replaces the visx tooltip's crosshair rather than joining it — two
          // would draw two lines at the same instant.
          <ChartCursorLayer
            stops={stops}
            cursor={hoveredTimestamp}
            series={cursorSeries}
          >
            {({ x, left, points }) => {
              const label = labelAt.get(x);
              if (label === undefined) return null;
              // `left` is the crosshair's own SVG x; the overlay it lands in
              // spans the whole plot, so without placing the card here every
              // tooltip parks at the plot's origin instead of following the
              // line. Flipped to the near side past halfway so it cannot run
              // off the card's right edge.
              //
              // Pinned to the plot top rather than tracking the readout dot:
              // the plot is 152px tall, so a card centred on a dot near either
              // edge is clipped, and one that slides with a wiggling series is
              // harder to read than one that stays put.
              const flip = left > chartWidth / 2;
              return (
                <div
                  className={chartTooltipSurfaceClassName}
                  style={{
                    position: 'absolute',
                    left,
                    top: CHART_MARGIN.top,
                    transform: `translateX(${flip ? 'calc(-100% - 10px)' : '10px'})`,
                    whiteSpace: 'nowrap',
                    pointerEvents: 'none',
                  }}
                >
                  <div className={chartTooltipTitleClassName}>{label}</div>
                  {points.map((point) => (
                    <div
                      key={point.id}
                      className={chartTooltipValueClassName}
                      style={{ color: point.color }}
                    >
                      {chart.formatValue(point.value)}
                    </div>
                  ))}
                  {/* Unlabelled, like the series rows above it: each figure
                      wears its own mark's colour, which is what says whether it
                      is the line or the limit. A word here would have labelled
                      one row out of two and read as a table with a missing
                      header. The plot names the limit at its own line. */}
                  {tooltipThresholds.map((entry) => (
                    <div
                      key={`tooltip-threshold-${entry.value}`}
                      className={chartTooltipValueClassName}
                      style={{
                        color: resolveChartColor(
                          entry.stroke ?? THRESHOLD_LEGEND_COLOR,
                        ),
                      }}
                    >
                      {chart.formatValue(entry.value)}
                    </div>
                  ))}
                </div>
              );
            }}
          </ChartCursorLayer>
        )}
      </XYChart>
    </div>
  );
}
