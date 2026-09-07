import { presetToRange, type TimeRange } from '@archon-research/design-system';

/**
 * The app's default window, a local override of the design system's own '30d'.
 *
 * The time-series and risk-capital queries scan the columnstore in full because
 * `allocation_position` has no compress_segmentby, and a 30d window multiplies
 * that cost across every chunk. Capped at 24h until the columnstore segmentby
 * migration lands and re-compression finishes; a reader can still pick a longer
 * range explicitly. Owners: see VEC-N/A (perf).
 *
 * Its own module rather than the `shared/ui` barrel: the root route reads the
 * default, so through the barrel it pinned every component the barrel names
 * into the entry chunk.
 */
export const DEFAULT_RANGE_PRESET = '24h' as const;

/**
 * The design system's `defaultTimeRange`, resolved at the override above. Kept
 * shaped like the kit's helper so callers stay unaware that it differs.
 */
export function defaultTimeRange(): TimeRange {
  return presetToRange(DEFAULT_RANGE_PRESET);
}
