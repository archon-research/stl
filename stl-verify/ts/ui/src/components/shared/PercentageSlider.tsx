import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { formatRatioPercent } from '../../lib/dashboard';

type PercentageSliderProps = {
  id: string;
  label: string;
  value: number;
  onChange: (value: number) => void;
  min?: number;
  max?: number;
  step?: number;
  tickLabels?: number[];
  disabled?: boolean;
};

function getTickValues(min: number, max: number, step: number): number[] {
  const values: number[] = [];
  for (let value = min; value <= max; value += step) {
    values.push(Number(value.toFixed(2)));
  }
  return values;
}

function getOffset(value: number, min: number, max: number): string {
  return `${((value - min) / (max - min)) * 100}%`;
}

function getEdgeAlignedTransform(
  value: number,
  firstValue: number,
  lastValue: number,
): string {
  if (value === firstValue) {
    return 'translateX(0)';
  }

  if (value === lastValue) {
    return 'translateX(-100%)';
  }

  return 'translateX(-50%)';
}

export function PercentageSlider({
  id,
  label,
  value,
  onChange,
  min = 0,
  max = 1,
  step = 0.05,
  tickLabels = [0, 0.25, 0.5, 0.75, 1],
  disabled = false,
}: PercentageSliderProps) {
  const tickValues = getTickValues(min, max, step);
  const displayValue = formatRatioPercent(value, 0);

  return (
    <label htmlFor={id} className={css({ display: 'grid', gap: '2' })}>
      <span
        className={css({
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
        })}
      >
        {label}
      </span>
      <div
        className={flex({
          align: 'flex-start',
          justify: 'space-between',
          gap: '3',
          wrap: 'wrap',
        })}
      >
        <div
          className={css({
            flex: '1',
            minWidth: '16rem',
            display: 'grid',
            gap: '2',
          })}
        >
          <input
            id={id}
            type="range"
            min={min}
            max={max}
            step={step}
            value={value}
            onChange={(event) => onChange(Number(event.target.value))}
            disabled={disabled}
            className={css({
              width: '100%',
              accentColor: 'interactive.accent',
              cursor: disabled ? 'not-allowed' : 'pointer',
              opacity: disabled ? 0.5 : 1,
            })}
          />
          <div
            aria-hidden="true"
            className={css({
              position: 'relative',
              height: '10',
            })}
          >
            {tickValues.map((tickValue, index) => (
              <span
                key={tickValue}
                style={{
                  left: getOffset(tickValue, min, max),
                  transform: getEdgeAlignedTransform(
                    tickValue,
                    tickValues[0],
                    tickValues[tickValues.length - 1],
                  ),
                }}
                className={css({
                  position: 'absolute',
                  top: 0,
                  width: '1px',
                  height: index % 2 === 1 ? '2.5' : '1.5',
                  bg: 'border.default',
                  opacity: tickValue === value ? 1 : 0.7,
                })}
              />
            ))}
          </div>
          <div
            aria-hidden="true"
            className={css({
              position: 'relative',
              height: '6',
            })}
          >
            {tickLabels.map((tickLabel) => (
              <span
                key={tickLabel}
                style={{
                  left: getOffset(tickLabel, min, max),
                  transform: getEdgeAlignedTransform(
                    tickLabel,
                    tickLabels[0],
                    tickLabels[tickLabels.length - 1],
                  ),
                }}
                className={css({
                  position: 'absolute',
                  top: 0,
                  fontSize: 'xs',
                  color: 'text.muted',
                  whiteSpace: 'nowrap',
                })}
              >
                {formatRatioPercent(tickLabel, 0)}
              </span>
            ))}
          </div>
        </div>
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {displayValue}
        </span>
      </div>
    </label>
  );
}
