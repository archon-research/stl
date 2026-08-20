import { StatTile } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type SummaryMetricProps = {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  className?: string;
};

// `statTile`'s value slot is not a flex container and neither it nor `sub` sets
// wrapping. Both are needed here: values carry an inline <TokenLogo> beside
// their text, and USD amounts get long enough to overflow a 4-up grid column.
// Upstream gap in the recipe, tracked as ORB-352.
const valueClassName = css({
  display: 'flex',
  alignItems: 'center',
  gap: '2',
  flexWrap: 'wrap',
  minWidth: 0,
  overflowWrap: 'anywhere',
  wordBreak: 'break-word',
});

const detailClassName = css({
  overflowWrap: 'anywhere',
  wordBreak: 'break-word',
});

// Thin adapter over StatTile that keeps this component's own prop names, so the
// metric call sites across the allocations views stay untouched. Note that
// `className` now *composes* with the tile frame instead of replacing it, which
// is what StatTile does; callers that pass a full card style still win, because
// Panda orders the utilities layer after the recipes layer.
export function SummaryMetric({
  label,
  value,
  detail,
  className,
}: SummaryMetricProps) {
  return (
    <StatTile
      className={className}
      labelCase="upper"
      label={label}
      value={<span className={valueClassName}>{value}</span>}
      sub={
        // Falsy, not nullish: `''` and `0` must render nothing, or the tile gains
        // an empty `sub` slot and the extra grid gap that comes with it.
        !detail ? undefined : <span className={detailClassName}>{detail}</span>
      }
    />
  );
}
