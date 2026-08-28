import { SyncedChartGroup } from '@archon-research/charting';
import { Badge, type BadgeColorPalette } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css, cx } from '#styled-system/css';

import {
  type EncumbranceSeverity,
  formatFreshnessLabel,
  formatRatioPercent,
  formatUsdValue,
  formatWadValue,
} from '../../shared/lib/dashboard';
import type { PrimeRiskCapital } from '../../shared/types/allocation';
import { ExposureCard, PrimeCollateralCard } from './HiddenMetricCards';
import {
  MetricCard,
  MetricCardError,
  MetricCardLegend,
  MetricCardSkeleton,
  MetricCardTrend,
  type MetricChartSpec,
  metricCaptionClassName,
  metricDetailClassName,
  metricsGridClassName,
  metricsGridStyle,
  preferredFigure,
  TOP_METRIC_CARD_LABELS,
  type TopMetricCard,
  VISIBLE_TOP_METRIC_CARDS,
} from './metricCards';

// Only what the band reads. The caller's summary carries more (a latest-activity
// timestamp), but asking for it here would couple this to a field it never uses.
type AllocationTotals = {
  allocationCount: number;
  totalUsd: number;
};

type BandCharts = {
  activity: MetricChartSpec | null;
  exposure: MetricChartSpec | null;
  totalCapital: MetricChartSpec | null;
  collateral: MetricChartSpec | null;
  encumbrance: MetricChartSpec | null;
  debt: MetricChartSpec | null;
};

type PrimeMetricsBandProps = {
  isSkeleton: boolean;
  hasTopMetrics: boolean;
  summary: AllocationTotals | null;
  overallSummary: AllocationTotals | null;
  hasSearchQuery: boolean;
  riskCapital: PrimeRiskCapital | null;
  // Shared by exposure, total risk capital and encumbrance.
  capitalObservedAt: string | null;
  riskCapitalErrorMessage: string | null;
  summaryErrorMessage: string | null;
  primeDebtErrorMessage: string | null;
  hasPrime: boolean;
  collateral: {
    usd: number | null;
    observedAt: string | null;
    isLoading: boolean;
  };
  encumbrance: {
    ratio: number | null;
    caption: string | null;
    severity: EncumbranceSeverity;
  };
  debt: {
    wad: string | null | undefined;
    ilkLabel: string | null;
    isLoading: boolean;
  };
  charts: BandCharts;
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
};

/**
 * One metric's cell, whatever state it is in.
 *
 * The grid is always the full set of cards: a missing cell shifted every one
 * after it, so a card that cannot render holds its place as an error or a
 * placeholder instead of disappearing.
 */
function MetricCardCell({
  card,
  rendered,
  errorMessage,
}: {
  card: TopMetricCard;
  rendered: ReactNode;
  errorMessage: string | null;
}) {
  if (rendered !== null) {
    return rendered;
  }

  const label = TOP_METRIC_CARD_LABELS[card];
  return errorMessage === null ? (
    <MetricCardSkeleton label={label} />
  ) : (
    <MetricCardError
      label={label}
      title={`${label} is unavailable`}
      description="Change the time range to retry."
      errorMessage={errorMessage}
    />
  );
}

// chartsErrorMessage tracks the primary (prime-debt) series only; every
// supplementary card degrades to its own fallback instead of reporting an error
// that did not happen to it.
function TotalAllocationCard({
  summary,
  overallSummary,
  hasSearchQuery,
  chart,
  isChartsLoading,
}: {
  summary: AllocationTotals;
  overallSummary: AllocationTotals | null;
  hasSearchQuery: boolean;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
}) {
  const isFiltered = hasSearchQuery && overallSummary !== null;

  return (
    <MetricCard
      label="Total allocation"
      info="The allocation rows below, added up: the USD value of every position the prime holds — this is the sum of the Exposure column, so it always matches the table. Each row contributes Verify's value where it has one and Legacy's where it does not."
      legend={
        <MetricCardLegend
          chart={chart}
          seriesLabel="allocation"
          isLoading={isChartsLoading}
        />
      }
      value={
        isFiltered
          ? `${formatUsdValue(summary.totalUsd)} / ${formatUsdValue(overallSummary.totalUsd)}`
          : formatUsdValue(summary.totalUsd)
      }
      detail={
        <div className={metricDetailClassName}>
          <div className={metricCaptionClassName}>
            {isFiltered
              ? `${summary.allocationCount}/${overallSummary.allocationCount} allocations`
              : `${summary.allocationCount} allocations`}
          </div>
          <MetricCardTrend
            chart={chart}
            isLoading={isChartsLoading}
            errorMessage={null}
          />
        </div>
      }
    />
  );
}

function TotalRiskCapitalCard({
  riskCapital,
  observedAt,
  chart,
  isChartsLoading,
}: {
  riskCapital: PrimeRiskCapital;
  observedAt: string | null;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
}) {
  const total = preferredFigure(
    riskCapital.reference_total_risk_capital_usd,
    riskCapital.total_risk_capital_usd,
  );
  const required = preferredFigure(
    riskCapital.reference_prime_required_risk_capital_usd,
    riskCapital.prime_required_risk_capital_usd,
  );
  // The stamp is the reference feed's, and it sits on a line covering both
  // figures, so it is withheld unless both came from that feed.
  const capitalObservedAt =
    total.fromReference && required.fromReference ? observedAt : null;

  return (
    <MetricCard
      label="Total risk capital"
      info="The treasury USDS held in the prime's SubProxy — the capital available to absorb losses. The dashed line marks the required risk capital it is measured against."
      infoHref="https://sky-atlas.io/#6f6b25d6-f73c-4733-ba37-12a0a411433c"
      infoLinkText="Sky Atlas A.3.2.1.2.1 →"
      legend={
        <MetricCardLegend
          chart={chart}
          seriesLabel="total capital"
          isLoading={isChartsLoading}
        />
      }
      value={formatUsdValue(total.value ?? '0')}
      detail={
        <div className={metricDetailClassName}>
          <div className={metricCaptionClassName}>
            Required {formatUsdValue(required.value)}
            {capitalObservedAt === null
              ? null
              : ` · observed ${formatFreshnessLabel(capitalObservedAt)}`}
          </div>
          <MetricCardTrend
            chart={chart}
            isLoading={isChartsLoading}
            errorMessage={null}
          />
        </div>
      }
    />
  );
}

// One chip per band, styled like the table's category chips. Badge has no
// orange palette, so the low breach overrides its fill with the chart set's
// orange (`identity.8`) — literal css(): see lib/activity.tsx for the trap.
const ENCUMBRANCE_BAND_CHIP: Record<
  EncumbranceSeverity,
  { label: string; colorPalette: BadgeColorPalette; className?: string }
> = {
  healthy: { label: 'Healthy', colorPalette: 'green' },
  'at-risk': { label: 'At risk', colorPalette: 'amber' },
  low: {
    label: 'Low severity breach',
    colorPalette: 'red',
    className: css({ bg: 'identity.8', color: 'white' }),
  },
  high: { label: 'High severity breach', colorPalette: 'red' },
};

function EncumbranceCard({
  ratio,
  caption,
  severity,
  chart,
  isChartsLoading,
}: {
  ratio: number | null;
  caption: string | null;
  severity: EncumbranceSeverity;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
}) {
  const chip = ENCUMBRANCE_BAND_CHIP[severity];
  return (
    <MetricCard
      label="Encumbrance ratio"
      info="Required risk capital as a share of total risk capital. The Sky Atlas defines at or above 100% as a Low Severity Breach and above 103% as a High Severity Breach; 80–100% is flagged At risk here as an early warning."
      infoHref="https://sky-atlas.io/#5435f680-aaaa-461a-bcae-4056bb8964d9"
      infoLinkText="Sky Atlas A.3.2.2.7.2.1.1.1 →"
      legend={
        <MetricCardLegend
          chart={chart}
          seriesLabel="encumbrance"
          isLoading={isChartsLoading}
        />
      }
      value={
        <>
          {formatRatioPercent(ratio)}
          {/* No figure, no health claim: absence is explained in the caption. */}
          {ratio === null ? null : (
            <Badge
              size="md"
              variant={severity === 'high' ? 'solid' : 'subtle'}
              colorPalette={chip.colorPalette}
              // Sized against the 2xl figure beside it, which Badge's own
              // steps stop short of.
              className={cx(
                css({ fontSize: 'md', px: '2.5', py: '1' }),
                chip.className,
              )}
            >
              {chip.label}
            </Badge>
          )}
        </>
      }
      detail={
        <div className={metricDetailClassName}>
          {/* Rendered even when empty: siblings all carry a caption line, and
              dropping it floated this card's chart above their baseline. */}
          <div className={metricCaptionClassName}>{caption ?? '\u00A0'}</div>
          <MetricCardTrend
            chart={chart}
            isLoading={isChartsLoading}
            errorMessage={null}
          />
        </div>
      }
    />
  );
}

function PrimeDebtCard({
  wad,
  ilkLabel,
  isLoading,
  chart,
  isChartsLoading,
  chartsErrorMessage,
}: {
  wad: string | null | undefined;
  ilkLabel: string | null;
  isLoading: boolean;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
}) {
  return (
    <MetricCard
      label="Prime debt exposure"
      info="What the prime has drawn against its allocator vault: the minted debt for its ilk, in USDS terms. The indexed figure is read from chain state; the reference figure is the legacy feed's own reported debt."
      infoHref="https://sky-atlas.io/#1c09308d-b7cd-495c-b547-baf628a6e323"
      infoLinkText="Sky Atlas A.3.7.1.2 →"
      legend={
        <MetricCardLegend
          chart={chart}
          seriesLabel="debt"
          isLoading={isChartsLoading}
          errorMessage={chartsErrorMessage}
        />
      }
      value={isLoading ? 'Loading...' : formatWadValue(wad)}
      detail={
        isLoading ? (
          'Fetching latest debt snapshot'
        ) : (
          <div className={metricDetailClassName}>
            {/* The ilk alone. The raw WAD that used to sit beside it — with a
                tooltip and an explorer link — was read as the prime's address
                when it is the unrounded debt the headline already states. */}
            <div className={metricCaptionClassName}>{ilkLabel ?? '\u00A0'}</div>
            <MetricCardTrend
              chart={chart}
              isLoading={isChartsLoading}
              errorMessage={chartsErrorMessage}
            />
          </div>
        )
      }
    />
  );
}

/**
 * The metrics rail above the allocations table.
 *
 * Which cards appear is data-dependent, so the column count is passed in rather
 * than counted here: the caller already knows, and computing it twice is how the
 * skeleton came to disagree with what replaced it.
 */
export function PrimeMetricsBand({
  isSkeleton,
  hasTopMetrics,
  summary,
  overallSummary,
  hasSearchQuery,
  riskCapital,
  capitalObservedAt,
  riskCapitalErrorMessage,
  summaryErrorMessage,
  primeDebtErrorMessage,
  hasPrime,
  collateral,
  encumbrance,
  debt,
  charts,
  isChartsLoading,
  chartsErrorMessage,
}: PrimeMetricsBandProps) {
  if (isSkeleton) {
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

  if (!hasTopMetrics) {
    return null;
  }

  // Which fetch explains a card being absent. A card with no data and no
  // explanation is still loading, not failed.
  const CARD_ERROR_SOURCE: Record<TopMetricCard, string | null> = {
    'total-allocation': summaryErrorMessage,
    exposure: riskCapitalErrorMessage,
    'total-risk-capital': riskCapitalErrorMessage,
    'prime-collateral': null,
    encumbrance: riskCapitalErrorMessage,
    'prime-debt': primeDebtErrorMessage,
  };

  // Every card the band knows how to build, including the two the grid does not
  // place: keeping them in the record is what keeps them compiled and honest
  // against the props around them, so turning one back on is one edit in
  // `metricCards.tsx` rather than a rewrite here.
  const renderedCards: Record<TopMetricCard, ReactNode> = {
    'total-allocation':
      summary === null ? null : (
        <TotalAllocationCard
          summary={summary}
          overallSummary={overallSummary}
          hasSearchQuery={hasSearchQuery}
          chart={charts.activity}
          isChartsLoading={isChartsLoading}
        />
      ),
    exposure:
      riskCapital === null ? null : (
        <ExposureCard
          riskCapital={riskCapital}
          observedAt={capitalObservedAt}
          chart={charts.exposure}
          isChartsLoading={isChartsLoading}
        />
      ),
    'total-risk-capital':
      riskCapital === null ? null : (
        <TotalRiskCapitalCard
          riskCapital={riskCapital}
          observedAt={capitalObservedAt}
          chart={charts.totalCapital}
          isChartsLoading={isChartsLoading}
        />
      ),
    'prime-collateral': !hasPrime ? null : (
      <PrimeCollateralCard
        usd={collateral.usd}
        observedAt={collateral.observedAt}
        isLoading={collateral.isLoading}
        chart={charts.collateral}
        isChartsLoading={isChartsLoading}
      />
    ),
    encumbrance:
      riskCapital === null ? null : (
        <EncumbranceCard
          ratio={encumbrance.ratio}
          caption={encumbrance.caption}
          severity={encumbrance.severity}
          chart={charts.encumbrance}
          isChartsLoading={isChartsLoading}
        />
      ),
    // Withheld on a failed snapshot as well as with no prime: the cell falls
    // through to the error box only when nothing is rendered, and a card showing
    // an em dash reads as "no debt" rather than "we could not read it".
    'prime-debt':
      !hasPrime || primeDebtErrorMessage !== null ? null : (
        <PrimeDebtCard
          wad={debt.wad}
          ilkLabel={debt.ilkLabel}
          isLoading={debt.isLoading}
          chart={charts.debt}
          isChartsLoading={isChartsLoading}
          chartsErrorMessage={chartsErrorMessage}
        />
      ),
  };

  return (
    // One cursor across the band: the cards plot the same prime over the same
    // window, so a reader comparing them is asking what every card said at one
    // instant. Hovering each in turn to find it is the question asked badly.
    <SyncedChartGroup>
      <div
        className={metricsGridClassName}
        style={metricsGridStyle(VISIBLE_TOP_METRIC_CARDS.length)}
      >
        {VISIBLE_TOP_METRIC_CARDS.map((card) => (
          <MetricCardCell
            key={`metric-card-${card}`}
            card={card}
            rendered={renderedCards[card]}
            errorMessage={CARD_ERROR_SOURCE[card]}
          />
        ))}
      </div>
    </SyncedChartGroup>
  );
}
