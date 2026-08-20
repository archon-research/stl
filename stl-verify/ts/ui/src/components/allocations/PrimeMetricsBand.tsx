import { ErrorState } from '@archon-research/design-system';

import { css } from '#styled-system/css';

import {
  type EncumbranceSeverity,
  formatFreshnessLabel,
  formatRatioPercent,
  formatRawWadLabel,
  formatUsdValue,
  formatWadValue,
} from '../../lib/dashboard';
import type { PrimeRiskCapital } from '../../types/allocation';
import { AppTooltip, SummaryMetric } from '../shared';
import {
  MetricCardTrend,
  type MetricChartSpec,
  metricDetailClassName,
  metricsCardClassName,
  metricsGridClassName,
  metricsGridStyle,
  TOP_METRIC_CARDS,
} from './metricCards';

// Only what the band reads. The caller's summary carries more (a latest-activity
// timestamp), but asking for it here would couple this to a field it never uses.
export type AllocationTotals = {
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
  visibleCardCount: number;
  summary: AllocationTotals | null;
  overallSummary: AllocationTotals | null;
  hasSearchQuery: boolean;
  riskCapital: PrimeRiskCapital | null;
  // Shared by exposure, total risk capital and encumbrance.
  capitalObservedAt: string | null;
  riskCapitalErrorMessage: string | null;
  hasPrime: boolean;
  collateral: {
    usd: number | null;
    observedAt: string | null;
    isLoading: boolean;
  };
  encumbrance: {
    ratio: number | null;
    caption: string;
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

const captionClassName = css({ fontSize: 'sm', color: 'text.muted' });

const skeletonCardClassName = css({
  height: '88px',
  borderRadius: 'md',
  borderStyle: 'solid',
  borderWidth: '1px',
  borderColor: 'border.subtle',
  bg: 'surface.subtle',
});

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
  return (
    <SummaryMetric
      className={metricsCardClassName}
      label="Exposure"
      value={formatUsdValue(riskCapital.prime_exposure_usd)}
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
  return (
    <SummaryMetric
      className={metricsCardClassName}
      label="Total risk capital"
      value={formatUsdValue(riskCapital.total_risk_capital_usd ?? '0')}
      detail={
        <div className={metricDetailClassName}>
          <div className={captionClassName}>
            Required{' '}
            {formatUsdValue(riskCapital.prime_required_risk_capital_usd)}
            {observedAt === null
              ? null
              : ` · observed ${formatFreshnessLabel(observedAt)}`}
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

// Low and high are distinct tones: the Atlas treats them as different breaches
// with separately measured durations, so one colour for both would flatten that.
const encumbranceCaptionTone: Record<EncumbranceSeverity, string> = {
  none: 'text.muted',
  low: 'text.warning',
  high: 'text.critical',
};

function EncumbranceCard({
  ratio,
  caption,
  severity,
  chart,
  isChartsLoading,
}: {
  ratio: number | null;
  caption: string;
  severity: EncumbranceSeverity;
  chart: MetricChartSpec | null;
  isChartsLoading: boolean;
}) {
  return (
    <SummaryMetric
      className={metricsCardClassName}
      label="Encumbrance"
      value={formatRatioPercent(ratio)}
      detail={
        <div className={metricDetailClassName}>
          <div
            className={css({
              fontSize: 'sm',
              color: encumbranceCaptionTone[severity],
            })}
          >
            {caption}
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

const debtCaptionClassName = css({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'baseline',
  gap: '1',
  fontSize: 'sm',
  color: 'text.muted',
  // The tooltip trigger is a 44px-min tap target; inline here it would inflate
  // the row and drop the text below the other cards' single-line subtitles.
  // Collapse it to the text line height so the baselines align.
  '& button': { minHeight: 'auto', py: '0' },
});

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
  visibleCardCount,
  summary,
  overallSummary,
  hasSearchQuery,
  riskCapital,
  capitalObservedAt,
  riskCapitalErrorMessage,
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
          <div
            key={`metrics-skeleton-${card}`}
            className={skeletonCardClassName}
          />
        ))}
      </div>
    );
  }

  if (!hasTopMetrics) {
    return null;
  }

  return (
    <div
      className={metricsGridClassName}
      style={metricsGridStyle(visibleCardCount)}
    >
      {summary ? (
        <TotalAllocationCard
          summary={summary}
          overallSummary={overallSummary}
          hasSearchQuery={hasSearchQuery}
          chart={charts.activity}
          isChartsLoading={isChartsLoading}
        />
      ) : null}

      {riskCapital ? (
        <ExposureCard
          riskCapital={riskCapital}
          observedAt={capitalObservedAt}
          chart={charts.exposure}
          isChartsLoading={isChartsLoading}
        />
      ) : null}

      {/* Takes the place of the two cards risk capital feeds, so a failed metric
          stays one cell wide in the rail instead of becoming a full-width banner
          under it. `alignSelf` so the panel is only as tall as its message: a
          grid item would otherwise stretch to the chart cards' height. */}
      {!riskCapital && riskCapitalErrorMessage ? (
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
        <TotalRiskCapitalCard
          riskCapital={riskCapital}
          observedAt={capitalObservedAt}
          chart={charts.totalCapital}
          isChartsLoading={isChartsLoading}
        />
      ) : null}

      {hasPrime ? (
        <PrimeCollateralCard
          usd={collateral.usd}
          observedAt={collateral.observedAt}
          isLoading={collateral.isLoading}
          chart={charts.collateral}
          isChartsLoading={isChartsLoading}
        />
      ) : null}

      {riskCapital ? (
        <EncumbranceCard
          ratio={encumbrance.ratio}
          caption={encumbrance.caption}
          severity={encumbrance.severity}
          chart={charts.encumbrance}
          isChartsLoading={isChartsLoading}
        />
      ) : null}

      {hasPrime ? (
        <PrimeDebtCard
          wad={debt.wad}
          ilkLabel={debt.ilkLabel}
          isLoading={debt.isLoading}
          chart={charts.debt}
          isChartsLoading={isChartsLoading}
          chartsErrorMessage={chartsErrorMessage}
        />
      ) : null}
    </div>
  );
}
