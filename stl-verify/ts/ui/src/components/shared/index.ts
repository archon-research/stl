export { ChainLogo } from './ChainLogo';
export { PageShell } from './PageShell';
export { PercentageSlider } from './PercentageSlider';
export { ProtocolLogo } from './ProtocolLogo';
export {
  isRangePreset,
  presetToRange,
  RangePicker,
  type RangePreset,
  type TimeRange,
} from '@archon-research/design-system';
// Temporary local override of the design-system default (which is '30d').
// The time-series + risk-capital queries currently scan the columnstore in full
// because allocation_position has no compress_segmentby — a 30d window
// multiplies that cost across every chunk. Cap the default at 24h until the
// columnstore segmentby migration lands and re-compression finishes; users can
// still pick a longer range explicitly. Owners: see VEC-N/A (perf).
import {
  defaultTimeRange as _designSystemDefaultTimeRange,
  presetToRange as _designSystemPresetToRange,
  type TimeRange as _DesignSystemTimeRange,
} from '@archon-research/design-system';

export const DEFAULT_RANGE_PRESET = '24h' as const;

export function defaultTimeRange(): _DesignSystemTimeRange {
  // Keep the shape identical to the design-system helper so callers stay
  // unaware of the override; only the bounds change.
  return _designSystemPresetToRange(DEFAULT_RANGE_PRESET);
}
// `_designSystemDefaultTimeRange` is retained as an explicit reference so the
// override stays grep-traceable if/when the design-system bumps its default.
void _designSystemDefaultTimeRange;

export { StatusBadge } from './StatusBadge';
export { SummaryMetric } from './SummaryMetric';
export { TokenAddress } from './TokenAddress';
export { TokenLogo } from './TokenLogo';
export { AppTooltip } from './Tooltip';
