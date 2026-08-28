import { useMemo } from 'react';

import { usePrimeChartData } from '../../hooks/usePrimeChartData';
import {
  formatChartTimestampLabel,
  parseNumericValue,
  toChartSeries,
  wadToUnits,
} from '../../shared/lib/dashboard';
import { useProvenanceView } from '../../shared/lib/provenance';
import type {
  ExposureBucket,
  PrimeDebtBucket,
  TimeSeriesResolution,
  TotalCapitalBucket,
} from '../../shared/types/allocation';
import type { RangePreset, TimeRange } from '../../shared/ui';
import type { ChartDatum } from './metricCards';

export type PrimeChartSeries = {
  allocationBalanceSeries: ChartDatum[];
  primeDebtSeries: ChartDatum[];
  totalCapitalSeries: ChartDatum[];
  collateralSeries: ChartDatum[];
  encumbranceSeries: ChartDatum[];
  exposureSeries: ChartDatum[];
  // The raw buckets travel too: each card falls back to the *other*
  // provenance's column of the same bucket when its own series is empty.
  debtBuckets: PrimeDebtBucket[];
  totalCapitalBuckets: TotalCapitalBucket[];
  exposureBuckets: ExposureBucket[];
  primeCollateralObservedAt: string | null;
  capitalObservedAt: string | null;
  primeCollateralValue: number | null;
  isLoading: boolean;
  errorMessage: string | null;
};

// Picks the chart's downsampling resolution for a range. This is deliberately
// NOT the server's window-to-resolution policy (`time_series.minimum_resolution`),
// which is only a *floor* — the finest resolution the backend will allow for a
// window. This instead picks a *display* resolution that (1) is always at least
// as coarse as that floor (so the request never 422s) and (2) keeps the bucket
// count under the 500 per-prime page cap. Letting the server default would pick
// its floor and silently truncate long ranges (365d at the PT6H floor is ~1460
// buckets, well over 500). Each value below must stay >= the server floor for
// its window; if the server's policy tightens, these must be revisited.
function getResolutionForRange(
  preset: RangePreset,
  range: TimeRange,
): TimeSeriesResolution {
  const presetMap: Record<
    Exclude<RangePreset, 'custom'>,
    TimeSeriesResolution
  > = {
    '1h': 'PT1M',
    '6h': 'PT5M',
    '24h': 'PT15M',
    '7d': 'PT1H',
    '30d': 'PT6H',
    '90d': 'P1D',
    '180d': 'P1D',
    '365d': 'P1D',
  };

  if (preset !== 'custom') {
    return presetMap[preset];
  }

  const fromMs = range.from_timestamp
    ? new Date(range.from_timestamp).getTime()
    : Number.NaN;
  const toMs = range.to_timestamp
    ? new Date(range.to_timestamp).getTime()
    : Number.NaN;

  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) {
    return 'PT15M';
  }

  const durationMs = toMs - fromMs;

  if (durationMs <= 2 * 60 * 60 * 1000) {
    return 'PT1M';
  }
  if (durationMs <= 12 * 60 * 60 * 1000) {
    return 'PT5M';
  }
  if (durationMs <= 48 * 60 * 60 * 1000) {
    return 'PT15M';
  }
  if (durationMs <= 14 * 24 * 60 * 60 * 1000) {
    return 'PT1H';
  }
  if (durationMs <= 60 * 24 * 60 * 60 * 1000) {
    return 'PT6H';
  }
  return 'P1D';
}

/**
 * The trend series behind the metric cards, for one prime over one window.
 *
 * `primeTotalAllocationUsd` is the anchor the balance series is reconstructed
 * from, so it belongs to the caller that owns the rows, not to this hook.
 */
export function usePrimeChartSeries(
  primaryProxyAddress: string | null,
  rangePreset: RangePreset,
  timeRange: TimeRange,
  primeTotalAllocationUsd: number,
): PrimeChartSeries {
  const { showsReference: showsReferenceNow } = useProvenanceView();

  const chartResolution = useMemo(
    () => getResolutionForRange(rangePreset, timeRange),
    [rangePreset, timeRange],
  );

  const {
    debtBuckets,
    activityBuckets,
    totalCapitalBuckets,
    exposureBuckets,
    isLoading,
    errorMessage,
  } = usePrimeChartData(
    // Any one of the prime's proxies: the activity and exposure endpoints
    // resolve it prime-wide server-side. Total-capital and debt read
    // prime-scoped rows, so one address answers for the whole prime there too.
    primaryProxyAddress,
    {
      fromTimestamp: timeRange.from_timestamp,
      toTimestamp: timeRange.to_timestamp,
      resolution: chartResolution,
    },
  );

  // Reconstruct the total-allocation balance over time: anchor at the current
  // whole-prime total and walk backwards, undoing each bucket's signed USD net
  // flow. The newest bucket therefore lands exactly on the current total.
  // Flow-based, so it captures deposits/withdrawals but not price moves;
  // clamped at 0 since a negative balance is meaningless.
  //
  // This is only valid when the window ends at "now" so the newest bucket truly
  // is the current total. Presets always end now; a custom range is a fixed
  // window whose end drifts into the past, so anchoring its newest (past) bucket
  // at the current total would misstate every point. Suppress it for custom
  // ranges until a range-end anchor is available.
  const allocationBalanceSeries = useMemo<ChartDatum[]>(() => {
    if (rangePreset === 'custom' || activityBuckets.length === 0) {
      return [];
    }

    // Walked newest-first because each point is the one after it less its own
    // net flow, then flipped back into the ascending order the charts assume.
    const newestFirst: ChartDatum[] = [];
    let balance = primeTotalAllocationUsd;
    for (const bucket of [...activityBuckets].reverse()) {
      newestFirst.push({
        label: formatChartTimestampLabel(bucket.bucket_start),
        value: Math.max(balance, 0),
        timestamp: Date.parse(bucket.bucket_start),
      });
      balance -= parseNumericValue(bucket.net_flow_usd) ?? 0;
    }
    return newestFirst.reverse();
  }, [activityBuckets, primeTotalAllocationUsd, rangePreset]);

  const primeDebtSeries = useMemo<ChartDatum[]>(
    () => toChartSeries(debtBuckets, (bucket) => wadToUnits(bucket.debt_wad)),
    [debtBuckets],
  );

  // Total capital is the on-chain SubProxy treasury balance over time.
  const totalCapitalSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.total_capital_usd),
      ),
    [totalCapitalBuckets],
  );

  // Both ride the total-capital buckets: assets_usd and encumbrance_ratio come
  // from the same two upstream feeds, so a separate request could pair figures
  // observed at different instants. Reference mode only — self mode reports
  // them null, which filters to an empty series and a flat fallback card.
  const collateralSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.assets_usd),
      ),
    [totalCapitalBuckets],
  );

  const encumbranceSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.encumbrance_ratio),
      ),
    [totalCapitalBuckets],
  );

  // Priced receipt-token exposure over time; drives the Exposure card trend
  // (falls back to the flat current value below when no history is available).
  const exposureSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(exposureBuckets, (bucket) =>
        parseNumericValue(bucket.exposure_usd),
      ),
    [exposureBuckets],
  );

  // When the reference collateral figure was observed, which is not the bucket
  // serving it: the feed is daily and the value is carried forward, so without
  // showing this a figure up to a day old is indistinguishable from a fresh one.
  const primeCollateralObservedAt = showsReferenceNow
    ? (totalCapitalBuckets
        .filter((bucket) => bucket.assets_observed_at != null)
        .at(-1)?.assets_observed_at ?? null)
    : null;

  // The monitor's three figures share one stamp because they share one row. It
  // matters for the same reason the collateral one does, and more so since the
  // prior seeding reaches up to 90 days back.
  const capitalObservedAt = showsReferenceNow
    ? (totalCapitalBuckets
        .filter((bucket) => bucket.capital_observed_at != null)
        .at(-1)?.capital_observed_at ?? null)
    : null;

  // Reference mode publishes a real total-assets figure. Self mode has no
  // equivalent — STL does not index PSM3 and prices no Curve LP position — so
  // it shows what STL actually holds records for, captioned as such.
  // Buckets are oldest-first, so the newest observation is the last point.
  const primeCollateralValue = showsReferenceNow
    ? (collateralSeries.at(-1)?.value ?? null)
    : primeTotalAllocationUsd;

  return {
    allocationBalanceSeries,
    primeDebtSeries,
    totalCapitalSeries,
    collateralSeries,
    encumbranceSeries,
    exposureSeries,
    debtBuckets,
    totalCapitalBuckets,
    exposureBuckets,
    primeCollateralObservedAt,
    capitalObservedAt,
    primeCollateralValue,
    isLoading,
    errorMessage,
  };
}
