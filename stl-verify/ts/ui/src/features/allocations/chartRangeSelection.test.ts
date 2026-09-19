import { describe, expect, it } from 'vitest';

import { chartRangeToTimeRange } from './chartRangeSelection';

describe('chartRangeToTimeRange', () => {
  it('converts an already-ordered ms range to matching ISO bounds', () => {
    const start = 1_700_000_000_000;
    const end = 1_700_003_600_000;

    expect(chartRangeToTimeRange({ start, end })).toEqual({
      from_timestamp: new Date(start).toISOString(),
      to_timestamp: new Date(end).toISOString(),
    });
  });

  it('swaps a reversed range so from stays before to', () => {
    const earlier = 1_700_000_000_000;
    const later = 1_700_003_600_000;

    expect(chartRangeToTimeRange({ start: later, end: earlier })).toEqual({
      from_timestamp: new Date(earlier).toISOString(),
      to_timestamp: new Date(later).toISOString(),
    });
  });

  it('preserves millisecond precision in the ISO output', () => {
    const result = chartRangeToTimeRange({
      start: 1_700_000_000_123,
      end: 1_700_000_000_456,
    });

    expect(result.from_timestamp).toBe('2023-11-14T22:13:20.123Z');
    expect(result.to_timestamp).toBe('2023-11-14T22:13:20.456Z');
  });

  it('produces equal bounds for a zero-width range rather than throwing', () => {
    const instant = 1_700_000_000_000;

    const result = chartRangeToTimeRange({ start: instant, end: instant });

    expect(result.from_timestamp).toBe(result.to_timestamp);
  });
});
