import {
  RangePicker,
  type RangePreset,
  type TimeRange,
} from '@archon-research/design-system';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  formatDateTime,
  formatFreshnessLabel,
} from '../../shared/lib/dashboard';
import { filterLabelClassName } from './activityFilterStyles';

type ActivityPageHeaderProps = {
  isPageMode: boolean;
  showAllPrimes: boolean;
  latestActivityAt: string | null;
  rangePreset: RangePreset;
  range: TimeRange;
  onRangeChange: (preset: RangePreset, range: TimeRange) => void;
};

export function ActivityPageHeader({
  isPageMode,
  showAllPrimes,
  latestActivityAt,
  rangePreset,
  range,
  onRangeChange,
}: ActivityPageHeaderProps) {
  return (
    <div
      className={flex({
        align: 'flex-start',
        justify: 'space-between',
        gap: { base: '3', md: '4' },
        wrap: 'wrap',
      })}
    >
      <div
        className={css({
          display: 'grid',
          gap: '1',
          minWidth: { base: '0', md: '72' },
          flexGrow: '1',
          flexShrink: '1',
          flexBasis: '80',
        })}
      >
        <h1
          className={css({
            m: '0',
            fontSize: { base: '3xl', md: '4xl' },
            lineHeight: 'tight',
            color: 'text.strong',
          })}
        >
          Activities
        </h1>
        {showAllPrimes ? (
          <span className={css({ fontSize: 'sm', color: 'text.muted' })}>
            Across all primes
          </span>
        ) : null}

        {!isPageMode ? (
          <div className={css({ display: 'grid', gap: '1' })}>
            <span className={filterLabelClassName}>Time range</span>
            <RangePicker
              preset={rangePreset}
              range={range}
              onChange={onRangeChange}
            />
          </div>
        ) : null}
      </div>
      {latestActivityAt ? (
        <div
          className={css({
            display: 'flex',
            flexDirection: 'column',
            alignItems: { base: 'flex-start', md: 'flex-end' },
            gap: '0.5',
          })}
        >
          <span
            className={css({
              fontSize: 'sm',
              fontWeight: 'semibold',
              color: 'text.strong',
            })}
          >
            Latest activity {formatFreshnessLabel(latestActivityAt)}
          </span>
          <span className={css({ fontSize: 'xs', color: 'text.muted' })}>
            {formatDateTime(latestActivityAt)}
          </span>
        </div>
      ) : null}
    </div>
  );
}
