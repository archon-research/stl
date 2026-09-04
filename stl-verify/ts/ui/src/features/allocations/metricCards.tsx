import type { ChartColor, ChartColorToken } from '@archon-research/charting';
import {
  ErrorState,
  InfoPopover,
  StatTile,
} from '@archon-research/design-system';
import { Info } from 'lucide-react';
import type { CSSProperties, ReactNode } from 'react';

import { css } from '#styled-system/css';

import {
  balancedColumns,
  formatFreshnessLabel,
} from '../../shared/lib/dashboard';
import { preferReference } from '../../shared/lib/provenance';

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
  // Why the card has nothing to draw, for a card that cannot stand itself up
  // from a current value: without it a failed read plots as the empty state.
  errorMessage?: string | null;
  // Ordered ascending. Each draws a dashed limit line with a labelled edge.
  // `showInTooltip` also reports it at the cursor, in its own stroke — for a
  // limit the series is read directly against. Off by default: a limit the
  // reader is not comparing against only crowds the readout.
  thresholds?: {
    value: number;
    label?: string;
    showInTooltip?: boolean;
    stroke?: ChartColor;
  }[];
};

// Every card the metrics band knows how to build. Not what it shows: see
// `VISIBLE_TOP_METRIC_CARDS`, which is what drives the grid.
const TOP_METRIC_CARDS = [
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
export function metricsGridStyle(
  count: number,
): CSSProperties &
  Record<'--metric-columns-lg' | '--metric-columns-2xl', number> {
  return {
    '--metric-columns-lg': balancedColumns(count, 2),
    '--metric-columns-2xl': balancedColumns(count, 4),
  };
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
  width: 'full',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: '2',
});

// Truncation is the last resort, not the layout: with only the info glyph
// beside it every card's title fits, and this is what keeps a longer one from
// wrapping the header and dropping this card's chart below its row-mates'.
const cardTitleClassName = css({
  minWidth: '0',
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
  minWidth: '0',
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

// The plot's own height, but it lives here rather than with the plot: the
// skeleton reserves the same box, and two copies is how a loading card came to
// be a different height from the one that replaced it.
export const CHART_HEIGHT = 236;

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
 * The band before any figure has arrived: every card the grid places, loading.
 *
 * Chart-free, which is the point — it is what stands in while the band and the
 * charting package it pulls are still on the wire.
 */
export function MetricsBandSkeleton() {
  return (
    <div
      className={metricsGridClassName}
      style={metricsGridStyle(VISIBLE_TOP_METRIC_CARDS.length)}
    >
      {VISIBLE_TOP_METRIC_CARDS.map((card) => (
        <MetricCardSkeleton
          key={`metrics-skeleton-${card}`}
          label={TOP_METRIC_CARD_LABELS[card]}
        />
      ))}
    </div>
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

// Card and detail framing, shared by every metric card so one cannot drift from
// its neighbours.
const metricsCardClassName = css({
  borderRadius: 'sm',
  borderStyle: 'solid',
  borderWidth: '1px',
  borderColor: 'border.default',
  bg: 'surface.subtle',
  p: { base: '3', md: '3.5' },
  // No 'none' shadow token.
  boxShadow: '[none]',
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
  // Between the 16rem and 18rem steps.
  minHeight: '[17rem]',
  // One line whether or not it has text, since only some cards carry an
  // observation stamp and an empty row lifts the chart out of line with its
  // row-mates. `1lh` resolves against the caption's own font, not the card's.
  '& > :first-child': { minHeight: '[1lh]' },
});
