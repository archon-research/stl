import type { ChartColorToken } from '@archon-research/charting';

import {
  ENCUMBRANCE_AT_RISK_THRESHOLD,
  ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD,
  ENCUMBRANCE_LOW_SEVERITY_THRESHOLD,
  encumbranceSeverity,
  formatChartTimestampLabel,
  formatCompactNumber,
  formatCompactUsd,
  formatRatioPercent,
  parseNumericValue,
  toChartSeries,
  wadToUnits,
} from '../../shared/lib/dashboard';
import { preferReference } from '../../shared/lib/provenance';
import type {
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
} from '../../shared/types/allocation';
import type { TimeRange } from '../../shared/ui';
import type { ChartDatum, MetricChartSpec } from './metricCards';
import type { PrimeChartSeries } from './usePrimeChartSeries';

export type MetricChartInputs = {
  series: PrimeChartSeries;
  riskCapital: PrimeRiskCapital | null;
  referenceDebt: PrimeDebtBucket | null;
  primeDebtSnapshot: PrimeDebtSnapshot | null;
  showsReferenceNow: boolean;
  timeRange: TimeRange;
};

/**
 * The trend chart each metric card draws, in card order.
 *
 * Cards with nothing to draw are dropped, so the grid never reserves space for
 * an empty plot — unless the read behind one failed, which is not nothing to
 * say. Pure, so the card set is derivable in a test without a router or a
 * query client.
 */
export function buildMetricCharts({
  series,
  riskCapital,
  referenceDebt,
  primeDebtSnapshot,
  showsReferenceNow,
  timeRange,
}: MetricChartInputs): MetricChartSpec[] {
  const chartFromLabel = timeRange.from_timestamp
    ? formatChartTimestampLabel(timeRange.from_timestamp)
    : 'Range start';

  const chartToLabel = timeRange.to_timestamp
    ? formatChartTimestampLabel(timeRange.to_timestamp)
    : 'Range end';

  const fallbackChart = (value: number | null): ChartDatum[] => {
    if (value === null) {
      return [];
    }
    // No timestamps: these two points are the window's edges holding the
    // current value flat, not observations. Leaving them null keeps the card
    // out of the synced cursor, which is the honest outcome — there is no
    // history here to line up with a sibling's.
    return [
      { label: chartFromLabel, value, timestamp: null },
      { label: chartToLabel, value, timestamp: null },
    ];
  };

  // The real time series when present, else the flat current-value
  // placeholder, which the card renders identically.
  const seriesOrFallback = (
    points: ChartDatum[],
    currentValue: number | null,
  ): ChartDatum[] => (points.length > 0 ? points : fallbackChart(currentValue));

  // Sky's figures where it reports them: these are the flat line a card falls
  // back to, which must land on the same number the card's value shows.
  const exposureValue = parseNumericValue(
    preferReference(
      riskCapital?.reference_prime_exposure_usd,
      riskCapital?.prime_exposure_usd,
    ),
  );

  const requiredRiskCapitalValue = parseNumericValue(
    preferReference(
      riskCapital?.reference_prime_required_risk_capital_usd,
      riskCapital?.prime_required_risk_capital_usd,
    ),
  );

  const totalRiskCapitalValue = parseNumericValue(
    preferReference(
      riskCapital?.reference_total_risk_capital_usd,
      riskCapital?.total_risk_capital_usd,
    ),
  );

  // The same read the debt card's headline makes: a reference view holds its
  // snapshot in `referenceDebt`, so reading only the indexed snapshot left
  // the fallback null there — and with the series also empty, the whole
  // chart vanished under a headline that had a figure.
  const primeDebtValue = wadToUnits(
    showsReferenceNow ? referenceDebt?.debt_wad : primeDebtSnapshot?.debt_wad,
  );

  const encumbranceValue = parseNumericValue(
    preferReference(
      riskCapital?.reference_prime_encumbrance_ratio,
      riskCapital?.prime_encumbrance_ratio,
    ),
  );

  // The line wears the band the current ratio sits in, so a healthy chart
  // is not painted breach-red.
  const encumbranceStroke: ChartColorToken = {
    healthy: 'chart.series.positive' as const,
    'at-risk': 'chart.series.quaternary' as const,
    low: 'identity.8' as const,
    high: 'chart.series.critical' as const,
  }[encumbranceSeverity(encumbranceValue)];

  // Legacy's is the preferred model, so its series is the one drawn. Whole
  // series: a line traced from both would trace neither. Verify's is not
  // drawn beside it — a reader who wants that switches the view's provenance,
  // which keeps every card the same shape whichever provenance is on screen.
  const preferSkySeries = (
    stl: ChartDatum[],
    sky: ChartDatum[],
  ): ChartDatum[] => (sky.length > 0 ? sky : stl);

  const exposure = preferSkySeries(
    series.exposureSeries,
    toChartSeries(series.exposureBuckets, (bucket) =>
      parseNumericValue(bucket.reference_exposure_usd),
    ),
  );

  const totalCapital = preferSkySeries(
    series.totalCapitalSeries,
    toChartSeries(series.totalCapitalBuckets, (bucket) =>
      parseNumericValue(bucket.reference_total_capital_usd),
    ),
  );

  const primeDebt = preferSkySeries(
    series.primeDebtSeries,
    toChartSeries(series.debtBuckets, (bucket) =>
      wadToUnits(bucket.reference_debt_wad),
    ),
  );

  // One ordinal series token per card, named rather than written out as a
  // `var()` read: the token type is what catches a typo (and a repeat of the
  // collision where two of these cards named the same token unnoticed).
  const charts: MetricChartSpec[] = [
    {
      // Balance reconstructed from signed USD net flows, anchored at the
      // current total. The one card with no flat current-value fallback — an
      // absent history is an empty state — so it is also the one that has to
      // carry its read's failure, or an outage reads as a quiet window.
      key: 'allocation-activity-volume',
      data: series.allocationBalanceSeries,
      errorMessage: series.activityErrorMessage,
      stroke: 'chart.series.primary',
      formatValue: formatCompactUsd,
    },
    {
      // Exposure trend from priced receipt-token balances over time; falls
      // back to the flat current value when no history is available.
      key: 'risk-capital',
      data: seriesOrFallback(exposure, exposureValue),
      stroke: 'chart.series.secondary',
      formatValue: formatCompactUsd,
    },
    {
      key: 'total-capital',
      data: seriesOrFallback(totalCapital, totalRiskCapitalValue),
      stroke: 'chart.series.quaternary',
      formatValue: formatCompactUsd,
      // The requirement the caption states, drawn as one reference line —
      // no endpoint serves the requirement over time.
      thresholds:
        requiredRiskCapitalValue === null
          ? undefined
          : [
              {
                value: requiredRiskCapitalValue,
                // Named only. The figure is on the axis the line sits
                // against, in the caption above, and in the cursor tooltip
                // at full precision — repeating a rounded copy on the plot
                // read as a fourth, slightly different number.
                label: 'Required',
                // Reported at the cursor too: the total is read directly
                // against this line, so the two figures belong side by side.
                showInTooltip: true,
                // Muted, matching the encumbrance card's own early-warning
                // line. A coloured limit competed with the series for the
                // eye and read as a second quantity rather than a bound.
                stroke: 'var(--colors-text-muted)',
              },
            ],
    },
    {
      key: 'prime-debt-exposure',
      data: seriesOrFallback(primeDebt, primeDebtValue),
      stroke: 'chart.series.quinary',
      formatValue: (value: number) => `${formatCompactNumber(value)} DAI`,
    },
    {
      key: 'prime-collateral',
      data: seriesOrFallback(
        series.collateralSeries,
        series.primeCollateralValue,
      ),
      stroke: 'chart.series.tertiary',
      formatValue: formatCompactUsd,
    },
    {
      key: 'encumbrance-ratio',
      data: seriesOrFallback(series.encumbranceSeries, encumbranceValue),
      stroke: encumbranceStroke,
      formatValue: formatRatioPercent,
      // Ascending, and all three bands the severity scale reads: the 80%
      // edge is STL's own early warning rather than an Atlas level, so it is
      // drawn in the muted hue the other two are deliberately not.
      thresholds: [
        {
          value: ENCUMBRANCE_AT_RISK_THRESHOLD,
          label: formatRatioPercent(ENCUMBRANCE_AT_RISK_THRESHOLD, 0),
          stroke: 'var(--colors-text-muted)',
        },
        {
          value: ENCUMBRANCE_LOW_SEVERITY_THRESHOLD,
          label: formatRatioPercent(ENCUMBRANCE_LOW_SEVERITY_THRESHOLD, 0),
          stroke: 'var(--colors-text-warning)',
        },
        {
          value: ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD,
          label: formatRatioPercent(ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD, 0),
          stroke: 'var(--colors-text-critical)',
        },
      ],
    },
  ];

  return charts.filter(
    (chart) => chart.data.length > 0 || chart.errorMessage != null,
  );
}
