import type { TimeRange as ChartTimeRange } from '@archon-research/charting/xychart';
import type { TimeRange } from '@archon-research/design-system';

/**
 * `@archon-research/charting`'s `TimeRange` is `{ start, end }` epoch-ms; the
 * app's own `TimeRange` (from `@archon-research/design-system`) is ISO
 * strings. Ordered defensively rather than trusted: a reversed pair would
 * otherwise vanish silently downstream, at `normalizeRangeSelection`'s
 * `to > from` check on the URL schema.
 */
export function chartRangeToTimeRange(range: ChartTimeRange): TimeRange {
  const [startMs, endMs] =
    range.start <= range.end
      ? [range.start, range.end]
      : [range.end, range.start];

  return {
    from_timestamp: new Date(startMs).toISOString(),
    to_timestamp: new Date(endMs).toISOString(),
  };
}
