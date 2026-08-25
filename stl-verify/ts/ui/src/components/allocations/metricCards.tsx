import {
  Grid,
  XYChart,
  LineSeries,
  AreaSeries,
  Tooltip,
  Axis,
  ChartCursorLayer,
  ChartLegend,
  buildChartTheme,
  type ChartColor,
  type ChartColorToken,
  type ChartLegendItem,
  chartColorToken,
  chartTokens,
  resolveChartColor,
  useContainerWidth,
  useHoveredTimestamp,
  useSyncedCursorHandlers,
} from '@archon-research/charting';
import { DataContext, ReferenceBand } from '@archon-research/charting';
import {
  ErrorState,
  InfoPopover,
  SkeletonStack,
  StatTile,
} from '@archon-research/design-system';
import { Info } from 'lucide-react';
import type { CSSProperties, ReactNode } from 'react';
import { useContext, useMemo } from 'react';

import { css } from '#styled-system/css';

import { balancedColumns, formatFreshnessLabel } from '../../lib/dashboard';
import { preferReference } from '../../lib/provenance';

export type ChartDatum = {
  label: string;
  value: number;
  // The instant the point describes, or `null` for a synthetic placeholder
  // point that stands in for absent history. Only a series whose points all
  // carry one joins the synced cursor — a placeholder has no instant to snap
  // to, and reading one off it would date a figure that was never observed
  // then.
  timestamp: number | null;
};

export type MetricChartKey =
  | 'allocation-activity-volume'
  | 'risk-capital'
  | 'total-capital'
  | 'prime-debt-exposure'
  | 'prime-collateral'
  | 'encumbrance-ratio';

export type MetricChartSpec = {
  key: MetricChartKey;
  data: ChartDatum[];
  // A token name, not a colour: `chartColorToken` resolves it where a raw visx
  // prop or a `style` object needs the CSS value.
  stroke: ChartColorToken;
  formatValue: (value: number) => string;
  // Ordered ascending. Each draws a dashed limit line with a labelled edge.
  // `name` opts the limit into the cursor tooltip, where it is reported beside
  // the value being read against it; a limit without one stays on the plot.
  thresholds?: {
    value: number;
    label?: string;
    name?: string;
    stroke?: ChartColor;
  }[];
};

// Every card the metrics band knows how to build. Not what it shows: see
// `VISIBLE_TOP_METRIC_CARDS`, which is what drives the grid.
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

// The cards actually placed in the grid. Its length drives both the loading
// placeholders and the column count, so neither can drift from what is shown.
//
// Exposure and Prime collateral are withheld for now: their figures duplicate
// what Total allocation and the risk-capital cards already state, and six cards
// crowded the band. Their bodies are intact in `HiddenMetricCards.tsx` and
// `PrimeMetricsBand` still builds them, so switching one back on is a matter of
// dropping it from the exclusion below.
const HIDDEN_TOP_METRIC_CARDS: readonly TopMetricCard[] = [
  'exposure',
  'prime-collateral',
];

export const VISIBLE_TOP_METRIC_CARDS: readonly TopMetricCard[] =
  TOP_METRIC_CARDS.filter((card) => !HIDDEN_TOP_METRIC_CARDS.includes(card));

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

// Card header: the metric's name and its explainer on one line, the chart's
// legend right-aligned on a second. Beside the explainer is where it belongs and
// where it started, but at four columns a card is ~235px wide and a two-series
// key with a threshold runs 200px on its own — "Total risk capital" came out as
// "TOTA…". A line of its own is the same corner of the card with room to read.
const cardHeaderClassName = css({
  display: 'flex',
  width: '100%',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: '2',
});

// Truncation is the last resort, not the layout: with only the info glyph
// beside it every card's title fits, and this is what keeps a longer one from
// wrapping the header and dropping this card's chart below its row-mates'.
const cardTitleClassName = css({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
});

const cardActionsClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  gap: '2',
  flexShrink: 0,
});

const cardLegendRowClassName = css({
  display: 'flex',
  alignItems: 'center',
  // Never wraps to a line of its own: the key is one short entry now that the
  // limits label themselves on the plot, and a wrapped header drops this card's
  // chart below its row-mates'.
  flexShrink: 0,
  whiteSpace: 'nowrap',
  // The StatTile label slot uppercases and letter-spaces everything inside it,
  // which reads "indexed" as a second heading. A legend is a key, not a title.
  textTransform: 'none',
  letterSpacing: 'normal',
  // The kit legend sizes its own container inline (13px), which no inherited
  // rule can reach; `!important` from the stylesheet is what outranks a plain
  // inline declaration, and brings it down to the card's micro type.
  '& > div': { fontSize: 'var(--font-sizes-xs) !important' },
});

// Same slot contract as `SummaryMetric`'s: a block child of the StatTile `sub`
// row is sized by its content, so the detail's chart column needs `flex` to
// keep filling the tile.
const cardDetailSlotClassName = css({
  flex: '1',
  minWidth: 0,
  overflowWrap: 'anywhere',
});

/**
 * The frame every metric card shares.
 *
 * The same `StatTile` the shared `SummaryMetric` wraps, with one addition: a
 * header that carries the chart's legend under the metric's name. It lives here
 * rather than as a new `SummaryMetric` prop because that component takes a
 * plain-string label and the rest of the app uses it for figures with no chart
 * at all — a legend has no meaning there.
 */
export function MetricCard({
  label,
  value,
  detail,
  legend,
  info,
  infoHref,
  infoLinkText,
}: {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  /** The chart's key, from `MetricCardLegend`. Its row is held open either way. */
  legend?: ReactNode;
  /** Opens a click-through explanation of the metric beside the label. */
  info?: ReactNode;
  /** Verified Sky Atlas anchor for the metric's definition, when one exists. */
  infoHref?: string;
  /** Link text for `infoHref`, e.g. the Atlas document number. */
  infoLinkText?: string;
}) {
  return (
    <StatTile
      className={metricsCardClassName}
      labelCase="upper"
      label={
        <span className={cardHeaderClassName}>
          <span className={cardTitleClassName}>{label}</span>
          <span className={cardActionsClassName}>
            <span className={cardLegendRowClassName}>{legend}</span>
            {info === undefined ? null : (
              <InfoPopover
                label={`About ${label}`}
                placement="top-end"
                trigger={<Info size={14} aria-hidden />}
                {...(infoHref === undefined
                  ? {}
                  : { href: infoHref, linkText: infoLinkText })}
                className={css({
                  display: 'inline-flex',
                  color: 'text.muted',
                  _hover: { color: 'text.strong' },
                })}
              >
                {info}
              </InfoPopover>
            )}
          </span>
        </span>
      }
      value={value}
      sub={
        // Falsy, not nullish: `''` and `0` must render nothing, or the tile
        // gains an empty `sub` slot and the extra grid gap that comes with it.
        !detail ? undefined : (
          <span className={cardDetailSlotClassName}>{detail}</span>
        )
      }
    />
  );
}

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

// Absent when the figure is STL's own: only the reference feed carries an
// observation instant, and the on-chain series is as current as its last block.
export function observedCaption(observedAt: string | null): string | null {
  return observedAt === null
    ? null
    : `Observed ${formatFreshnessLabel(observedAt)}`;
}

/**
 * A headline figure, Sky's preferred, and which provenance it came from.
 *
 * Callers need the provenance as well as the number: the observation stamp is
 * the reference feed's own, so it may only caption a figure from that feed.
 */
export function preferredFigure(
  skyValue: string | null | undefined,
  stlValue: string | null | undefined,
): { value: string | null; fromReference: boolean } {
  return {
    value: preferReference(skyValue, stlValue),
    fromReference: skyValue != null,
  };
}

export const metricCaptionClassName = css({
  fontSize: 'sm',
  color: 'text.muted',
});

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

// Two columns so a named row's label and figure line up down the card, and the
// figures stay readable against each other rather than against the labels.
const chartTooltipRowClassName = css({
  display: 'flex',
  alignItems: 'baseline',
  justifyContent: 'space-between',
  gap: '3',
  fontSize: 'sm',
});

const chartTooltipRowLabelClassName = css({
  color: 'text.muted',
  fontSize: 'xs',
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
    <MetricCard
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
    <MetricCard
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
  const namedThresholds = thresholds.filter(
    (entry): entry is (typeof thresholds)[number] & { name: string } =>
      entry.name !== undefined,
  );
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
    stops === null ? point.label : (point.timestamp as number);

  const cursorSeries = [{ id: chart.key, color: chart.stroke, valueAt }];

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
        margin={CHART_MARGIN}
        xScale={{ type: 'band', paddingInner: 0.2 }}
        yScale={{ type: 'linear', domain: yDomain, nice: !isFlat }}
        onPointerMove={cursorHandlers.onPointerMove}
        onPointerOut={cursorHandlers.onPointerOut}
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
                  {/* A named limit is reported here because the value above is
                      read against it — the plot's own label states it once, at
                      the line, which is easy to miss on a small card. */}
                  {namedThresholds.map((entry) => (
                    <div
                      key={`tooltip-threshold-${entry.value}`}
                      className={chartTooltipRowClassName}
                    >
                      <span className={chartTooltipRowLabelClassName}>
                        {entry.name}
                      </span>
                      <span
                        style={{
                          color: resolveChartColor(
                            entry.stroke ?? THRESHOLD_LEGEND_COLOR,
                          ),
                        }}
                      >
                        {chart.formatValue(entry.value)}
                      </span>
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
