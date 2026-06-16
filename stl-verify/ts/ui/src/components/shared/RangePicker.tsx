import { type ChangeEvent, useCallback } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

export type RangePreset = '1h' | '6h' | '24h' | '7d' | '30d' | 'custom';

export type TimeRange = {
  from_timestamp: string | undefined;
  to_timestamp: string | undefined;
};

export type RangePickerProps = {
  preset: RangePreset;
  range: TimeRange;
  onChange: (preset: RangePreset, range: TimeRange) => void;
};

const PRESETS: { label: string; value: Exclude<RangePreset, 'custom'> }[] = [
  { label: '1h', value: '1h' },
  { label: '6h', value: '6h' },
  { label: '24h', value: '24h' },
  { label: '7d', value: '7d' },
  { label: '30d', value: '30d' },
];

function presetToRange(preset: Exclude<RangePreset, 'custom'>): TimeRange {
  const now = new Date();
  const offsetMs: Record<typeof preset, number> = {
    '1h': 60 * 60 * 1000,
    '6h': 6 * 60 * 60 * 1000,
    '24h': 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
    '30d': 30 * 24 * 60 * 60 * 1000,
  };
  return {
    from_timestamp: new Date(now.getTime() - offsetMs[preset]).toISOString(),
    to_timestamp: now.toISOString(),
  };
}

function toDateTimeLocalValue(iso: string | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  const local = new Date(d.getTime() - d.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function fromDateTimeLocalValue(value: string): string | undefined {
  return value ? new Date(value).toISOString() : undefined;
}

const presetButtonClassName = (active: boolean) =>
  css({
    h: '8',
    px: '2.5',
    borderRadius: 'md',
    borderWidth: '1px',
    borderStyle: 'solid',
    borderColor: active ? 'interactive.accent' : 'border.subtle',
    bg: active ? 'interactive.selected' : 'surface.default',
    color: active ? 'text.interactive' : 'text.default',
    fontSize: 'xs',
    fontWeight: active ? 'semibold' : 'normal',
    cursor: 'pointer',
    whiteSpace: 'nowrap',
    transitionProperty: 'color, background-color, border-color',
    transitionDuration: 'fast',
    _hover: {
      borderColor: 'interactive.accent',
      color: 'text.interactive',
    },
  });

const dateInputClassName = css({
  h: '8',
  borderRadius: 'md',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  bg: 'surface.default',
  color: 'text.default',
  px: '2',
  fontSize: 'xs',
  minWidth: 0,
  width: 'full',
});

export function RangePicker({ preset, range, onChange }: RangePickerProps) {
  const handlePreset = useCallback(
    (p: Exclude<RangePreset, 'custom'>) => {
      onChange(p, presetToRange(p));
    },
    [onChange],
  );

  const handleCustom = useCallback(() => {
    // Switch to custom without changing the timestamps (carry over current
    // range so the user doesn't lose their last selection).
    onChange('custom', range);
  }, [onChange, range]);

  const handleFromChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      onChange('custom', {
        ...range,
        from_timestamp: fromDateTimeLocalValue(e.target.value),
      });
    },
    [onChange, range],
  );

  const handleToChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      onChange('custom', {
        ...range,
        to_timestamp: fromDateTimeLocalValue(e.target.value),
      });
    },
    [onChange, range],
  );

  return (
    <div className={flex({ align: 'center', gap: '1.5', wrap: 'wrap' })}>
      {PRESETS.map((p) => (
        <button
          key={p.value}
          type="button"
          aria-pressed={preset === p.value}
          onClick={() => handlePreset(p.value)}
          className={presetButtonClassName(preset === p.value)}
        >
          {p.label}
        </button>
      ))}

      <button
        type="button"
        aria-pressed={preset === 'custom'}
        onClick={handleCustom}
        className={presetButtonClassName(preset === 'custom')}
      >
        Custom
      </button>

      {preset === 'custom' && (
        <div className={flex({ align: 'center', gap: '1.5', wrap: 'wrap' })}>
          <input
            aria-label="From timestamp"
            type="datetime-local"
            value={toDateTimeLocalValue(range.from_timestamp)}
            onChange={handleFromChange}
            className={dateInputClassName}
          />
          <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>
            to
          </span>
          <input
            aria-label="To timestamp"
            type="datetime-local"
            value={toDateTimeLocalValue(range.to_timestamp)}
            onChange={handleToChange}
            className={dateInputClassName}
          />
        </div>
      )}
    </div>
  );
}
