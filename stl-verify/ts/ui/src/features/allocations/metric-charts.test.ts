import type { TimeRange } from '@archon-research/design-system';
import { describe, expect, it } from 'vitest';

import type { PrimeRiskCapital } from '../../shared/types/allocation';
import { buildMetricCharts } from './metric-charts';
import type { MetricChartInputs } from './metric-charts';
import type { ChartDatum } from './metricCards';
import type { PrimeChartSeries } from './usePrimeChartSeries';

const point = (label: string, value: number): ChartDatum => ({
  label,
  value,
  timestamp: Date.parse('2026-08-01T00:00:00Z'),
});

const emptySeries = (): PrimeChartSeries => ({
  allocationBalanceSeries: [],
  primeDebtSeries: [],
  totalCapitalSeries: [],
  collateralSeries: [],
  encumbranceSeries: [],
  exposureSeries: [],
  debtBuckets: [],
  totalCapitalBuckets: [],
  exposureBuckets: [],
  primeCollateralObservedAt: null,
  capitalObservedAt: null,
  primeCollateralValue: null,
  isLoading: false,
  errorMessage: null,
  activityErrorMessage: null,
});

const TIME_RANGE: TimeRange = {
  from_timestamp: '2026-08-01T00:00:00Z',
  to_timestamp: '2026-08-08T00:00:00Z',
} as TimeRange;

const inputs = (
  overrides: Partial<MetricChartInputs> = {},
): MetricChartInputs => ({
  series: emptySeries(),
  riskCapital: null,
  referenceDebt: null,
  primeDebtSnapshot: null,
  showsReferenceNow: false,
  timeRange: TIME_RANGE,
  ...overrides,
});

const keysOf = (specs: ReturnType<typeof buildMetricCharts>) =>
  specs.map((spec) => spec.key);

const chartFor = (specs: ReturnType<typeof buildMetricCharts>, key: string) =>
  specs.find((spec) => spec.key === key);

describe('buildMetricCharts drop rule', () => {
  it('returns nothing when every card is empty and no current value stands in', () => {
    expect(buildMetricCharts(inputs())).toEqual([]);
  });

  it('keeps only the card whose series carries points', () => {
    const specs = buildMetricCharts(
      inputs({
        series: {
          ...emptySeries(),
          allocationBalanceSeries: [point('day 1', 10)],
        },
      }),
    );

    expect(keysOf(specs)).toEqual(['allocation-activity-volume']);
  });

  it('never places a card the activity feed cannot draw, even with a current value', () => {
    // `allocation-activity-volume` is the one card with no flat fallback: an
    // absent history is an empty state, not a straight line at today's total.
    const specs = buildMetricCharts(
      inputs({
        riskCapital: { prime_exposure_usd: '100' } as PrimeRiskCapital,
      }),
    );

    expect(keysOf(specs)).not.toContain('allocation-activity-volume');
  });

  it('keeps the activity card when its read failed, rather than dropping it into the same empty state', () => {
    const specs = buildMetricCharts(
      inputs({
        series: {
          ...emptySeries(),
          activityErrorMessage: 'activity is unavailable',
        },
      }),
    );

    expect(keysOf(specs)).toEqual(['allocation-activity-volume']);
    const activity = chartFor(specs, 'allocation-activity-volume');
    expect(activity?.errorMessage).toBe('activity is unavailable');
    // Still no flat line: the card reports the failure, it does not invent a
    // series to report it with.
    expect(activity?.data).toEqual([]);
  });

  it('leaves every other card free of an error it did not have', () => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: { prime_exposure_usd: '100' } as PrimeRiskCapital,
        series: {
          ...emptySeries(),
          activityErrorMessage: 'activity is unavailable',
        },
      }),
    );

    expect(chartFor(specs, 'risk-capital')?.errorMessage ?? null).toBeNull();
  });
});

describe('buildMetricCharts current-value fallback', () => {
  it('stands a card up from its current value as two flat, undatable points', () => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: { prime_exposure_usd: '250.5' } as PrimeRiskCapital,
      }),
    );

    expect(chartFor(specs, 'risk-capital')?.data).toEqual([
      { label: expect.any(String), value: 250.5, timestamp: null },
      { label: expect.any(String), value: 250.5, timestamp: null },
    ]);
  });

  it('labels the flat points as range edges when the window is open-ended', () => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: { prime_exposure_usd: '1' } as PrimeRiskCapital,
        timeRange: {} as TimeRange,
      }),
    );

    expect(
      chartFor(specs, 'risk-capital')?.data.map((datum) => datum.label),
    ).toEqual(['Range start', 'Range end']);
  });
});

describe('buildMetricCharts provenance precedence', () => {
  it("draws Sky's series when it has points, not STL's", () => {
    const specs = buildMetricCharts(
      inputs({
        series: {
          ...emptySeries(),
          exposureSeries: [point('stl', 1)],
          exposureBuckets: [
            {
              bucket_start: '2026-08-01T00:00:00Z',
              reference_exposure_usd: '99',
            },
          ] as PrimeChartSeries['exposureBuckets'],
        },
      }),
    );

    expect(chartFor(specs, 'risk-capital')?.data.at(0)?.value).toBe(99);
  });

  it("falls back to STL's series when Sky reports no bucket values", () => {
    const specs = buildMetricCharts(
      inputs({
        series: {
          ...emptySeries(),
          exposureSeries: [point('stl', 1)],
          exposureBuckets: [
            {
              bucket_start: '2026-08-01T00:00:00Z',
              reference_exposure_usd: null,
            },
          ] as PrimeChartSeries['exposureBuckets'],
        },
      }),
    );

    expect(chartFor(specs, 'risk-capital')?.data.at(0)?.value).toBe(1);
  });

  it('reads the debt headline from the reference snapshot under a reference view', () => {
    const specs = buildMetricCharts(
      inputs({
        showsReferenceNow: true,
        referenceDebt: {
          bucket_start: '2026-08-01T00:00:00Z',
          debt_wad: '2000000000000000000',
        } as MetricChartInputs['referenceDebt'],
        primeDebtSnapshot: {
          debt_wad: '9000000000000000000',
        } as MetricChartInputs['primeDebtSnapshot'],
      }),
    );

    expect(chartFor(specs, 'prime-debt-exposure')?.data.at(0)?.value).toBe(2);
  });

  it('reads the debt headline from the indexed snapshot otherwise', () => {
    const specs = buildMetricCharts(
      inputs({
        showsReferenceNow: false,
        referenceDebt: {
          bucket_start: '2026-08-01T00:00:00Z',
          debt_wad: '2000000000000000000',
        } as MetricChartInputs['referenceDebt'],
        primeDebtSnapshot: {
          debt_wad: '9000000000000000000',
        } as MetricChartInputs['primeDebtSnapshot'],
      }),
    );

    expect(chartFor(specs, 'prime-debt-exposure')?.data.at(0)?.value).toBe(9);
  });
});

describe('buildMetricCharts encumbrance band', () => {
  it.each([
    [0.1, 'chart.series.positive'],
    [0.85, 'chart.series.quaternary'],
    [1.01, 'identity.8'],
    [1.5, 'chart.series.critical'],
  ])('strokes the ratio %o with the band it sits in', (ratio, stroke) => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: {
          prime_encumbrance_ratio: String(ratio),
        } as PrimeRiskCapital,
      }),
    );

    expect(chartFor(specs, 'encumbrance-ratio')?.stroke).toBe(stroke);
  });

  it('draws all three bands as ascending threshold lines', () => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: { prime_encumbrance_ratio: '0.5' } as PrimeRiskCapital,
      }),
    );
    const values = chartFor(specs, 'encumbrance-ratio')?.thresholds?.map(
      (threshold) => threshold.value,
    );

    expect(values).toEqual([...(values ?? [])].sort((a, b) => a - b));
    expect(values).toHaveLength(3);
  });
});

describe('buildMetricCharts required-capital reference line', () => {
  it('omits the line when no requirement is reported', () => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: { total_risk_capital_usd: '10' } as PrimeRiskCapital,
      }),
    );

    expect(chartFor(specs, 'total-capital')?.thresholds).toBeUndefined();
  });

  it('draws the requirement once, named rather than numbered on the plot', () => {
    const specs = buildMetricCharts(
      inputs({
        riskCapital: {
          total_risk_capital_usd: '10',
          prime_required_risk_capital_usd: '4',
        } as PrimeRiskCapital,
      }),
    );

    expect(chartFor(specs, 'total-capital')?.thresholds).toEqual([
      expect.objectContaining({ value: 4, label: 'Required' }),
    ]);
  });
});
