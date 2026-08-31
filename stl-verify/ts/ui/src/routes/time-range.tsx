import { useSearch } from '@tanstack/react-router';
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  type ReactNode,
} from 'react';

import { useUpdateSearch } from '../shared/hooks/useUpdateSearch';
// DEFAULT_RANGE_PRESET comes from the local shared barrel so the temporary 24h
// override in shared/ui/index.ts applies here too; see that file.
import {
  DEFAULT_RANGE_PRESET,
  presetToRange,
  type RangePreset,
  type TimeRange,
} from '../shared/ui';

export type TimeRangeSelection = {
  rangePreset: RangePreset;
  timeRange: TimeRange;
  onRangeChange: (preset: RangePreset, range: TimeRange) => void;
};

/**
 * The window every view and every series query reads.
 *
 * A context rather than a hook each consumer calls for itself, because
 * `presetToRange` reads the clock: two callers resolving "24h" a millisecond
 * apart would produce two windows, and a window is a query key. One instance is
 * what makes the series cacheable and keeps the top bar, the charts and the
 * activity feed describing the same span.
 */
const TimeRangeContext = createContext<TimeRangeSelection | null>(null);

export function TimeRangeProvider({ children }: { children: ReactNode }) {
  const search = useSearch({ from: '__root__' });
  const updateSearch = useUpdateSearch();

  // A usable from/to pair in the URL is the custom selection itself; `range`
  // only ever names a preset (see the root search schema).
  const customTimeRange = useMemo<TimeRange | null>(
    () =>
      search.from && search.to
        ? { from_timestamp: search.from, to_timestamp: search.to }
        : null,
    [search.from, search.to],
  );

  const searchRangePreset = search.range ?? DEFAULT_RANGE_PRESET;
  const rangePreset: RangePreset = customTimeRange
    ? 'custom'
    : searchRangePreset;

  // Deliberately not re-derived per render: `presetToRange` reads the clock, and
  // a moving bound is a moving cache key. The window a preset resolves to is
  // therefore fixed until the preset changes, which is what makes the series
  // queries cacheable at all.
  const timeRange = useMemo<TimeRange>(
    () => customTimeRange ?? presetToRange(searchRangePreset),
    [customTimeRange, searchRangePreset],
  );

  const onRangeChange = useCallback(
    (preset: RangePreset, range: TimeRange) => {
      const customRange = preset === 'custom' ? range : null;
      updateSearch({
        // The default preset stays out of the URL to keep it clean, and a custom
        // range is carried by from/to alone.
        range:
          preset === 'custom' || preset === DEFAULT_RANGE_PRESET
            ? undefined
            : preset,
        from: customRange?.from_timestamp,
        to: customRange?.to_timestamp,
      });
    },
    [updateSearch],
  );

  const value = useMemo<TimeRangeSelection>(
    () => ({ rangePreset, timeRange, onRangeChange }),
    [onRangeChange, rangePreset, timeRange],
  );

  return (
    <TimeRangeContext.Provider value={value}>
      {children}
    </TimeRangeContext.Provider>
  );
}

export function useTimeRange(): TimeRangeSelection {
  const selection = useContext(TimeRangeContext);

  if (selection === null) {
    throw new Error('useTimeRange must be used inside a TimeRangeProvider');
  }

  return selection;
}
