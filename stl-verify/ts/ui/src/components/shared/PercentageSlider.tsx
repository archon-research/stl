import { Slider } from '@archon-research/design-system';

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

type SliderValueChangeDetails = { value: number[] };

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
  const displayValue = formatRatioPercent(value, 0);

  return (
    <Slider.Root
      id={id}
      min={min}
      max={max}
      step={step}
      value={[value]}
      disabled={disabled}
      onValueChange={(details: SliderValueChangeDetails) =>
        onChange(details.value[0] ?? min)
      }
      className={css({ display: 'grid', gap: '2' })}
    >
      <Slider.Label
        className={css({
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
        })}
      >
        {label}
      </Slider.Label>
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
          <Slider.Control
            className={css({
              width: '100%',
              position: 'relative',
              py: '1',
            })}
          >
            <Slider.Track
              className={css({
                height: '1.5',
                borderRadius: 'full',
                bg: 'surface.subtle',
              })}
            >
              <Slider.Range
                className={css({
                  height: 'full',
                  borderRadius: 'full',
                  bg: 'interactive.accent',
                })}
              />
            </Slider.Track>
            <Slider.Thumb
              index={0}
              className={css({
                width: '4',
                height: '4',
                borderRadius: 'full',
                bg: 'surface.default',
                borderWidth: '2px',
                borderStyle: 'solid',
                borderColor: 'interactive.accent',
                boxShadow: '0 1px 4px rgba(0, 0, 0, 0.2)',
              })}
            />
          </Slider.Control>
          <Slider.HiddenInput />
          <Slider.MarkerGroup
            className={css({
              position: 'relative',
              height: '6',
            })}
          >
            {tickLabels.map((tickLabel) => (
              <Slider.Marker
                key={tickLabel}
                value={tickLabel}
                className={css({
                  fontSize: 'xs',
                  color: 'text.muted',
                  whiteSpace: 'nowrap',
                })}
              >
                {formatRatioPercent(tickLabel, 0)}
              </Slider.Marker>
            ))}
          </Slider.MarkerGroup>
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
    </Slider.Root>
  );
}
