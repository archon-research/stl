import { formatUsdValue } from '../../shared/lib/dashboard';
import type { PrimeRiskCapital } from '../../shared/types/allocation';
import {
  MetricCard,
  MetricCardLegend,
  MetricCardTrend,
  type MetricChartSpec,
  metricCaptionClassName,
  metricDetailClassName,
  observedCaption,
  preferredFigure,
} from './metricCards';

/**
 * The two metric cards the band builds but does not place.
 *
 * Exposure restates Total allocation from the risk framework's side, and Prime
 * collateral is a daily Sky feed carried forward between observations; with all
 * six cards up the band read as a wall of figures rather than a summary. They
 * are parked here rather than deleted: both are complete, type-checked, and
 * still constructed by `PrimeMetricsBand` — switching one back on is a matter
 * of dropping it from `HIDDEN_TOP_METRIC_CARDS` in `metricCards.tsx`, which is
 * what decides who gets a cell in the grid.
 */

export function ExposureCard({
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
    <MetricCard
      label="Exposure"
      info="The prime's total USD exposure as the risk framework reports it — one top-down figure, not a row sum. Legacy's figure is preferred where reported, so it can differ from Total allocation in coverage and observation time."
      legend={
        <MetricCardLegend
          chart={chart}
          seriesLabel="exposure"
          isLoading={isChartsLoading}
        />
      }
      value={formatUsdValue(exposure.value)}
      detail={
        <div className={metricDetailClassName}>
          <div className={metricCaptionClassName}>
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

export function PrimeCollateralCard({
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
    <MetricCard
      label="Prime collateral"
      info="The asset value the legacy monitor reports standing behind the prime — the upstream PRIME COLLATERAL figure. A daily feed, carried forward between observations."
      legend={
        <MetricCardLegend
          chart={chart}
          seriesLabel="collateral"
          isLoading={isChartsLoading}
        />
      }
      // The value is a reduce from zero, so "not fetched yet" and "holds
      // nothing" are the same number until the fetch lands.
      value={isLoading ? 'Loading...' : formatUsdValue(usd)}
      detail={
        <div className={metricDetailClassName}>
          <div className={metricCaptionClassName}>
            {observedCaption(observedAt)}
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
