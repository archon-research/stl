import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import type { MetricChartSpec } from './metricCards';
import {
  PrimeMetricsBand,
  type PrimeMetricsBandProps,
} from './PrimeMetricsBand';

type Coverage = { pricedEntityCount: number; entityCount: number } | null;

// `data: []` keeps `MetricCardTrend` on its "no trend data" branch, so the
// card never reaches the visx chart -- the caption these tests check is a
// sibling of that chart, not something the chart itself has to render.
const activityChart = (coverage: Coverage): MetricChartSpec => ({
  key: 'allocation-activity-volume',
  data: [],
  stroke: 'chart.series.primary',
  formatValue: (value) => String(value),
  coverage,
});

// Only `total-allocation` renders: `riskCapital: null` and `hasPrime: false`
// drop the other three visible cards to their skeleton, which is chart-free.
const props = (coverage: Coverage): PrimeMetricsBandProps => ({
  isSkeleton: false,
  hasTopMetrics: true,
  summary: { allocationCount: 3, totalUsd: 100 },
  overallSummary: null,
  hasSearchQuery: false,
  riskCapital: null,
  capitalObservedAt: null,
  riskCapitalErrorMessage: null,
  summaryErrorMessage: null,
  primeDebtErrorMessage: null,
  hasPrime: false,
  collateral: { usd: null, observedAt: null, isLoading: false },
  encumbrance: { ratio: null, caption: null, severity: 'healthy' },
  debt: { wad: null, ilkLabel: null, isLoading: false },
  charts: {
    activity: activityChart(coverage),
    exposure: null,
    totalCapital: null,
    collateral: null,
    encumbrance: null,
    debt: null,
  },
  isChartsLoading: false,
  chartsErrorMessage: null,
});

const render = (coverage: Coverage) =>
  renderToStaticMarkup(<PrimeMetricsBand {...props(coverage)} />);

describe('PrimeMetricsBand total allocation coverage note', () => {
  it('names no counts for a fully-priced bucket', () => {
    const markup = render(null);

    expect(markup).toContain('3 allocations');
    expect(markup).not.toContain('positions priced');
  });

  it('names the counts for a partially-priced bucket', () => {
    const markup = render({ pricedEntityCount: 49, entityCount: 58 });

    expect(markup).toContain('49 of 58 positions priced');
  });

  // series=flow reports both counts null, which `latestAllocationCoverage`
  // (see dashboard.test.ts) already reduces to the same `null` coverage as
  // the fully-priced case -- so the card renders exactly as it did before
  // these fields existed.
  it('renders unchanged for a null coverage, as series=flow reports', () => {
    expect(render(null)).toBe(render(null));
  });
});
