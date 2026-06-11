import { StyledSelect, ToggleGroup } from '@archon-research/design-system';
import type { ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';
import { segmentedControl } from '#styled-system/recipes';

import type { FilterOption } from '../../lib/dashboard';

type TopBarProps = {
  hasSelectedPrime: boolean;
  onViewChange: (view: 'allocation' | 'activities') => void;
  networkOptions: FilterOption[];
  onNetworkChange: (value: string | null) => void;
  onProtocolChange: (value: string | null) => void;
  protocolOptions: FilterOption[];
  selectedNetwork: string | null;
  selectedProtocol: string | null;
  selectedView: 'allocation' | 'activities';
  showAllPrimes: boolean;
  onShowAllPrimesChange: (value: boolean) => void;
};

const segmentedControlStyles = segmentedControl();
const toggleGroupClassName = `${segmentedControlStyles.group} ${css({ p: '0.25', gap: '0.5' })}`;
const toggleClassName = `${segmentedControlStyles.item} ${css({ minHeight: '8', px: '2.5', fontSize: 'sm' })}`;

function FilterField({
  ariaLabel,
  disabled,
  label,
  onChange,
  options,
  placeholder,
  showLabel,
  value,
}: {
  ariaLabel: string;
  disabled: boolean;
  label: string;
  onChange: (value: string | null) => void;
  options: FilterOption[];
  placeholder: string;
  showLabel: boolean;
  value: string | null;
}) {
  return (
    <label
      className={css({
        display: 'grid',
        gap: '1',
      })}
    >
      {showLabel ? (
        <span
          className={css({
            fontSize: 'xs',
            textTransform: 'uppercase',
            letterSpacing: '0.1em',
            color: 'text.muted',
          })}
        >
          {label}
        </span>
      ) : null}
      <StyledSelect
        aria-label={ariaLabel}
        value={value ?? ''}
        onChange={(event: ChangeEvent<HTMLSelectElement>) =>
          onChange(event.target.value || null)
        }
        disabled={disabled}
      >
        <option value="">{placeholder}</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {`${option.label} (${option.count})`}
          </option>
        ))}
      </StyledSelect>
    </label>
  );
}

export function TopBar({
  hasSelectedPrime,
  onViewChange,
  networkOptions,
  onNetworkChange,
  onProtocolChange,
  protocolOptions,
  selectedNetwork,
  selectedProtocol,
  selectedView,
  showAllPrimes,
  onShowAllPrimesChange,
}: TopBarProps) {
  const showLabels = selectedView === 'activities';

  return (
    <div
      className={css({
        display: 'grid',
        gap: '3',
        alignItems: 'end',
      })}
    >
      <div className={flex({ justify: 'flex-start' })}
      >
        <ToggleGroup.Root
          value={[selectedView]}
          onValueChange={(details: { value: string[] }) => {
            const nextValue = details.value[0];

            if (nextValue === 'allocation' || nextValue === 'activities') {
              onViewChange(nextValue);
            }
          }}
          aria-label="Core navigation"
          className={toggleGroupClassName}
        >
          <ToggleGroup.Item value="allocation" className={toggleClassName}>
            Allocations
          </ToggleGroup.Item>
          <ToggleGroup.Item value="activities" className={toggleClassName}>
            Activities
          </ToggleGroup.Item>
        </ToggleGroup.Root>
      </div>

      <div
        className={css({
          display: 'grid',
          gridTemplateColumns: {
            base: '1fr',
            md: 'repeat(3, minmax(0, 1fr))',
          },
          gap: '3',
          alignItems: 'end',
        })}
      >
        <FilterField
          ariaLabel="Filter by network"
          disabled={networkOptions.length === 0}
          label="Network"
          onChange={onNetworkChange}
          options={networkOptions}
          placeholder="All networks"
          showLabel={showLabels}
          value={selectedNetwork}
        />
        <FilterField
          ariaLabel="Filter by protocol"
          disabled={
            (!hasSelectedPrime && selectedView === 'allocation') ||
            protocolOptions.length === 0
          }
          label="Protocol"
          onChange={onProtocolChange}
          options={protocolOptions}
          placeholder="All protocols"
          showLabel={showLabels}
          value={selectedProtocol}
        />
        <label
          className={css({
            display: 'grid',
            gap: '1',
            alignContent: 'end',
            minHeight: '100%',
          })}
        >
          {showLabels ? (
            <span
              className={css({
                fontSize: 'xs',
                textTransform: 'uppercase',
                letterSpacing: '0.1em',
                color: 'text.muted',
              })}
            >
              Prime scope
            </span>
          ) : null}
          <span
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              gap: '2',
              fontSize: 'sm',
              color: selectedView === 'allocation' ? 'text.muted' : 'text.default',
            })}
          >
            <input
              type="checkbox"
              aria-label="Show all primes"
              checked={showAllPrimes}
              disabled={selectedView === 'allocation'}
              onChange={(event: ChangeEvent<HTMLInputElement>) =>
                onShowAllPrimesChange(event.target.checked)
              }
            />
            Show all primes
          </span>
        </label>
      </div>
    </div>
  );
}
