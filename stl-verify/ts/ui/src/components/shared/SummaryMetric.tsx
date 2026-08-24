import { InfoPopover, StatTile } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type SummaryMetricProps = {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  className?: string;
  /** Opens a click-through explanation of the metric beside the label. */
  info?: ReactNode;
};

// The `statTile` value slot is a wrap-friendly inline-flex row now, so `value`
// goes in unwrapped: a logo and its figure are two items of that row and take
// its gap, which a span of our own would collapse back to inline text.
// `sub` is the same row, and that is what the wrapper below is for — a block
// child of a flex row is sized by its content, so the detail's chart column
// needs `flex` to keep filling the tile.
const detailClassName = css({
  flex: '1',
  minWidth: 0,
  overflowWrap: 'anywhere',
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
  info,
}: SummaryMetricProps) {
  return (
    <StatTile
      className={className}
      labelCase="upper"
      label={
        info === undefined ? (
          label
        ) : (
          <span
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              gap: '1',
            })}
          >
            {label}
            <InfoPopover label={`About ${label}`} placement="top">
              {info}
            </InfoPopover>
          </span>
        )
      }
      value={value}
      sub={
        // Falsy, not nullish: `''` and `0` must render nothing, or the tile gains
        // an empty `sub` slot and the extra grid gap that comes with it.
        !detail ? undefined : <span className={detailClassName}>{detail}</span>
      }
    />
  );
}
