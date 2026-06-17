import { StyledSelect } from '@archon-research/design-system';
import {
  type ChangeEvent,
  type MouseEvent,
  useCallback,
  useMemo,
  useState,
} from 'react';

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

const PRESETS: { label: string; value: RangePreset }[] = [
  { label: '1h', value: '1h' },
  { label: '6h', value: '6h' },
  { label: '24h', value: '24h' },
  { label: '7d', value: '7d' },
  { label: '30d', value: '30d' },
  { label: 'Custom', value: 'custom' },
];

// Value for the option that displays the currently-applied custom range (e.g.
// "Custom (1 day)"). Distinct from 'custom', which re-opens the picker so the
// user can choose a different range.
const CUSTOM_ACTIVE_VALUE = 'custom-active';

export function presetToRange(
  preset: Exclude<RangePreset, 'custom'>,
): TimeRange {
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

export const DEFAULT_RANGE_PRESET: RangePreset = '24h';

export function defaultTimeRange(): TimeRange {
  return presetToRange('24h');
}

// Human-readable span of a range, e.g. "1 day", "1 day 6 hours", "30 minutes".
// Minutes are only shown for sub-day ranges to keep it concise.
function humanizeRangeDuration(
  from: string | undefined,
  to: string | undefined,
): string | null {
  if (!from || !to) {
    return null;
  }

  const ms = new Date(to).getTime() - new Date(from).getTime();
  if (!Number.isFinite(ms) || ms <= 0) {
    return null;
  }

  const totalMinutes = Math.round(ms / 60_000);
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;

  const unit = (value: number, name: string) =>
    `${value} ${name}${value === 1 ? '' : 's'}`;

  const parts: string[] = [];
  if (days > 0) {
    parts.push(unit(days, 'day'));
  }
  if (hours > 0) {
    parts.push(unit(hours, 'hour'));
  }
  if (minutes > 0 && days === 0) {
    parts.push(unit(minutes, 'minute'));
  }

  return parts.length > 0 ? parts.join(' ') : 'Less than a minute';
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

const customModalBackdropClassName = css({
  position: 'fixed',
  inset: 0,
  bg: 'rgba(0, 0, 0, 0.55)',
  zIndex: 70,
  display: 'grid',
  placeItems: 'center',
  p: '4',
});

const customModalClassName = css({
  width: 'min(32rem, 100%)',
  borderRadius: 'lg',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.default',
  bg: 'surface.default',
  boxShadow: '0 18px 48px rgba(0, 0, 0, 0.35)',
  display: 'grid',
  gap: '4',
  p: '4',
});

const modalActionButtonClassName = (variant: 'ghost' | 'solid') =>
  css({
    h: '9',
    px: '3.5',
    borderRadius: 'md',
    borderWidth: '1px',
    borderStyle: 'solid',
    borderColor: variant === 'solid' ? 'interactive.accent' : 'border.default',
    bg: variant === 'solid' ? 'interactive.default' : 'surface.default',
    color: variant === 'solid' ? 'text.inverted' : 'text.default',
    cursor: 'pointer',
    fontSize: 'sm',
    fontWeight: 'semibold',
  });

export function RangePicker({ preset, range, onChange }: RangePickerProps) {
  const [isCustomModalOpen, setIsCustomModalOpen] = useState(false);
  const [draftRange, setDraftRange] = useState<TimeRange>(range);

  const selectedValue = useMemo(
    () => (preset === 'custom' ? CUSTOM_ACTIVE_VALUE : preset),
    [preset],
  );

  const closeCustomModal = useCallback(() => {
    setIsCustomModalOpen(false);
  }, []);

  const handleFromChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
    setDraftRange((previous) => ({
      ...previous,
      from_timestamp: fromDateTimeLocalValue(e.target.value),
    }));
  }, []);

  const handleToChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
    setDraftRange((previous) => ({
      ...previous,
      to_timestamp: fromDateTimeLocalValue(e.target.value),
    }));
  }, []);

  const handlePresetChange = useCallback(
    (event: ChangeEvent<HTMLSelectElement>) => {
      const value = event.target.value;
      // Re-open the picker so a new custom range can be chosen.
      if (value === 'custom') {
        setDraftRange(range);
        setIsCustomModalOpen(true);
        return;
      }
      // Selecting the already-applied custom range is a no-op.
      if (value === CUSTOM_ACTIVE_VALUE) {
        return;
      }

      const nextPreset = value as Exclude<RangePreset, 'custom'>;
      onChange(nextPreset, presetToRange(nextPreset));
    },
    [onChange, range],
  );

  const handleConfirmCustomRange = useCallback(() => {
    onChange('custom', draftRange);
    closeCustomModal();
  }, [closeCustomModal, draftRange, onChange]);

  const handleCancelCustomRange = useCallback(() => {
    setDraftRange(range);
    closeCustomModal();
  }, [closeCustomModal, range]);

  const disableConfirm =
    !draftRange.from_timestamp ||
    !draftRange.to_timestamp ||
    new Date(draftRange.from_timestamp).getTime() >=
      new Date(draftRange.to_timestamp).getTime();

  const isCustomSelected = preset === 'custom';

  const customDuration = isCustomSelected
    ? humanizeRangeDuration(range.from_timestamp, range.to_timestamp)
    : null;

  const modalTitleId = 'range-picker-custom-modal-title';

  const modalDescriptionId = 'range-picker-custom-modal-description';

  const handleModalBackdropClick = useCallback(
    (event: MouseEvent<HTMLDivElement>) => {
      if (event.target === event.currentTarget) {
        handleCancelCustomRange();
      }
    },
    [handleCancelCustomRange],
  );

  return (
    <div className={css({ display: 'grid', gap: '1' })}>
      <StyledSelect
        aria-label="Select activity range"
        value={selectedValue}
        onChange={handlePresetChange}
      >
        {PRESETS.map((presetOption) => (
          <option key={presetOption.value} value={presetOption.value}>
            {presetOption.label}
          </option>
        ))}
        {isCustomSelected ? (
          <option value={CUSTOM_ACTIVE_VALUE}>
            {customDuration ? `Custom (${customDuration})` : 'Custom'}
          </option>
        ) : null}
      </StyledSelect>

      {isCustomModalOpen ? (
        <div
          className={customModalBackdropClassName}
          role="dialog"
          aria-modal="true"
          aria-labelledby={modalTitleId}
          aria-describedby={modalDescriptionId}
          onClick={handleModalBackdropClick}
        >
          <div className={customModalClassName}>
            <div className={css({ display: 'grid', gap: '1' })}>
              <h3
                id={modalTitleId}
                className={css({ m: 0, fontSize: 'md', color: 'text.strong' })}
              >
                Select custom range
              </h3>
              <p
                id={modalDescriptionId}
                className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}
              >
                Choose exact start and end timestamps, then confirm.
              </p>
            </div>

            <div className={flex({ align: 'center', gap: '2', wrap: 'wrap' })}>
              <div className={css({ minW: '12rem', flex: '1 1 12rem' })}>
                <label className={css({ display: 'grid', gap: '1' })}>
                  <span
                    className={css({ fontSize: 'xs', color: 'text.muted' })}
                  >
                    From
                  </span>
                  <input
                    aria-label="From timestamp"
                    type="datetime-local"
                    value={toDateTimeLocalValue(draftRange.from_timestamp)}
                    onChange={handleFromChange}
                    className={dateInputClassName}
                  />
                </label>
              </div>

              <div className={css({ minW: '12rem', flex: '1 1 12rem' })}>
                <label className={css({ display: 'grid', gap: '1' })}>
                  <span
                    className={css({ fontSize: 'xs', color: 'text.muted' })}
                  >
                    To
                  </span>
                  <input
                    aria-label="To timestamp"
                    type="datetime-local"
                    value={toDateTimeLocalValue(draftRange.to_timestamp)}
                    onChange={handleToChange}
                    className={dateInputClassName}
                  />
                </label>
              </div>
            </div>

            <div
              className={flex({
                align: 'center',
                justify: 'end',
                gap: '2',
                wrap: 'wrap',
              })}
            >
              <button
                type="button"
                onClick={handleCancelCustomRange}
                className={modalActionButtonClassName('ghost')}
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmCustomRange}
                disabled={disableConfirm}
                className={`${modalActionButtonClassName('solid')} ${css({
                  opacity: disableConfirm ? 0.45 : 1,
                  cursor: disableConfirm ? 'not-allowed' : 'pointer',
                })}`}
              >
                Confirm
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
