import { Badge, type BadgeColorPalette } from '@archon-research/design-system';
import { ExternalLink } from 'lucide-react';
import type { ReactNode } from 'react';

import { css, cx } from '#styled-system/css';

import {
  type EncumbranceSeverity,
  formatFreshnessLabel,
  formatRatioPercent,
  formatRawWadLabel,
  formatUsdValue,
  formatWadValue,
} from '../../lib/dashboard';
import { preferReference } from '../../lib/provenance';
import type { PrimeRiskCapital } from '../../types/allocation';
import { AppTooltip, SummaryMetric } from '../shared';
import {
  MetricCardError,
  MetricCardSkeleton,
  MetricCardTrend,
  type MetricChartSpec,
  metricDetailClassName,
  metricsCardClassName,
  metricsGridClassName,
  metricsGridStyle,
  TOP_METRIC_CARD_LABELS,
  TOP_METRIC_CARDS,
  type TopMetricCard,
} from './metricCards';

// Only what the band reads. The caller's summary carries more (a latest-activity
// timestamp), but asking for it here would couple this to a field it never uses.
export type AllocationTotals = {
  allocationCount: number;
  // Rows Sky alone reports, which the table shows and the total excludes.
  referenceOnlyCount: number;
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
    /** Explorer page of the proxy the debt is read for; null hides the link. */
    explorerUrl: string | null;
  };
  charts: BandCharts;
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
};

const captionClassName = css({ fontSize: 'sm', color: 'text.muted' });

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
    <SummaryMetric
      className={metricsCardClassName}
      label="Total allocation"
      value={
        isFiltered
          ? `${formatUsdValue(summary.totalUsd)} / ${formatUsdValue(overallSummary.totalUsd)}`
          : formatUsdValue(summary.totalUsd)
      }
      detail={
        <div className={metricDetailClassName}>
          <div className={captionClassName}>
            {isFiltered
              ? `${summary.allocationCount}/${overallSummary.allocationCount} allocations`
              : `${summary.allocationCount} allocations`}
            {summary.referenceOnlyCount > 0
              ? ` · ${summary.referenceOnlyCount} reported only by Sky`
              : null}
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

// Absent when the figure is STL's own: only the reference feed carries an
// observation instant, and the on-chain series is as current as its last block.
function observedCaption(observedAt: string | null): string | null {
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
function preferredFigure(
  skyValue: string | null | undefined,
  stlValue: string | null | undefined,
): { value: string | null; fromReference: boolean } {
  return {
    value: preferReference(skyValue, stlValue),
    fromReference: skyValue != null,
  };
}

function ExposureCard({
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
  const exposure = preferredFigure(
    riskCapital.reference_prime_exposure_usd,
    riskCapital.prime_exposure_usd,
  );

  return (
    <SummaryMetric
      className={metricsCardClassName}
      label="Exposure"
      value={formatUsdValue(exposure.value)}
      detail={
        <div className={metricDetailClassName}>
          <div className={captionClassName}>
            {observedCaption(exposure.fromReference ? observedAt : null)}
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
    <SummaryMetric
      className={metricsCardClassName}
      label="Total risk capital"
      value={formatUsdValue(total.value ?? '0')}
      detail={
        <div className={metricDetailClassName}>
          <div className={captionClassName}>
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

function PrimeCollateralCard({
  usd,
  observedAt,
  isLoading,
  chart,
  isChartsLoading,
}: {
  usd: number | null;
  observedAt: string | null;
  isLoading: boolean;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
}) {
  return (
    <SummaryMetric
      className={metricsCardClassName}
      label="Prime collateral"
      // The value is a reduce from zero, so "not fetched yet" and "holds
      // nothing" are the same number until the fetch lands.
      value={isLoading ? 'Loading...' : formatUsdValue(usd)}
      detail={
        <div className={metricDetailClassName}>
          <div className={captionClassName}>{observedCaption(observedAt)}</div>
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
    <SummaryMetric
      className={metricsCardClassName}
      label="Encumbrance ratio"
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
          {caption === null ? null : (
            <div className={cx(css({ fontSize: 'sm', color: 'text.muted' }))}>
              {caption}
            </div>
          )}
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

const debtCaptionClassName = css({
  display: 'flex',
  // Wrapping this caption -- the longest of the six -- cost a second line and
  // dropped the chart below its row-mates. Truncating hides nothing: the raw
  // WAD is already abbreviated behind a tooltip.
  flexWrap: 'nowrap',
  minWidth: 0,
  alignItems: 'baseline',
  gap: '1',
  fontSize: 'sm',
  color: 'text.muted',
  // The tooltip trigger is a 44px-min tap target; inline here it would inflate
  // the row and drop the text below the other cards' single-line subtitles.
  // Collapse it to the text line height so the baselines align.
  '& button': { minHeight: 'auto', py: '0' },
  '& > *': {
    minWidth: 0,
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
});

function PrimeDebtCard({
  wad,
  ilkLabel,
  explorerUrl,
  isLoading,
  chart,
  isChartsLoading,
  chartsErrorMessage,
}: {
  wad: string | null | undefined;
  ilkLabel: string | null;
  explorerUrl: string | null;
  isLoading: boolean;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
}) {
  const rawWadLabel = wad ? `Exact raw WAD ${wad}` : 'Raw WAD unavailable';

  return (
    <SummaryMetric
      className={metricsCardClassName}
      label="Prime debt exposure"
      value={isLoading ? 'Loading...' : formatWadValue(wad)}
      detail={
        isLoading ? (
          'Fetching latest debt snapshot'
        ) : (
          <div className={metricDetailClassName}>
            <div className={debtCaptionClassName}>
              {ilkLabel === null ? null : (
                <>
                  <span>{ilkLabel}</span>
                  <span aria-hidden="true">·</span>
                </>
              )}
              <AppTooltip
                ariaLabel={rawWadLabel}
                trigger={
                  <span
                    className={css({
                      textDecoration: 'underline',
                      textDecorationStyle: 'dotted',
                      textUnderlineOffset: '2px',
                    })}
                  >
                    {formatRawWadLabel(wad)}
                  </span>
                }
                content={wad ? `Exact raw WAD: ${wad}` : 'Raw WAD unavailable'}
              />
              {explorerUrl === null ? null : (
                // Beside the tooltip rather than inside it: hover content
                // dismisses on pointer-leave, so a link there is unclickable.
                <a
                  href={explorerUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="View the proxy wallet on the block explorer"
                  title="View the proxy wallet on the block explorer"
                  className={css({
                    display: 'inline-flex',
                    alignItems: 'center',
                    color: 'text.link',
                    _hover: { color: 'text.interactive' },
                  })}
                >
                  <ExternalLink size={12} />
                </a>
              )}
            </div>
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
        style={metricsGridStyle(TOP_METRIC_CARDS.length)}
      >
        {TOP_METRIC_CARDS.map((card) => (
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
          explorerUrl={debt.explorerUrl}
          isLoading={debt.isLoading}
          chart={charts.debt}
          isChartsLoading={isChartsLoading}
          chartsErrorMessage={chartsErrorMessage}
        />
      ),
  };

  return (
    <div
      className={metricsGridClassName}
      style={metricsGridStyle(TOP_METRIC_CARDS.length)}
    >
      {TOP_METRIC_CARDS.map((card) => (
        <MetricCardCell
          key={`metric-card-${card}`}
          card={card}
          rendered={renderedCards[card]}
          errorMessage={CARD_ERROR_SOURCE[card]}
        />
      ))}
    </div>
  );
}
