import { StyledSelect } from '@archon-research/design-system';
import type { ChangeEvent } from 'react';

import { css } from '#styled-system/css';

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
};

function FilterField({
  ariaLabel,
  disabled,
  onChange,
  options,
  placeholder,
  value,
}: {
  ariaLabel: string;
  disabled: boolean;
  onChange: (value: string | null) => void;
  options: FilterOption[];
  placeholder: string;
  value: string | null;
}) {
  return (
    <div
      className={css({
        minWidth: '0',
        width: '100%',
      })}
    >
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
    </div>
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
}: TopBarProps) {
  const isAllocationView = selectedView === 'allocation';

  return (
    <div
      className={css({
        display: 'grid',
        gridTemplateColumns: {
          base: '1fr',
          sm: isAllocationView
            ? 'minmax(12rem, max-content) repeat(2, minmax(0, 1fr))'
            : 'minmax(12rem, max-content)',
        },
        gap: { base: '2.5', md: '3' },
        alignItems: 'end',
      })}
    >
      <div
        className={css({
          minWidth: { base: '0', sm: '12rem' },
          width: '100%',
        })}
      >
        <StyledSelect
          aria-label="Switch dashboard view"
          value={selectedView}
          onChange={(event: ChangeEvent<HTMLSelectElement>) => {
            const next = event.target.value;

            if (next === 'allocation' || next === 'activities') {
              onViewChange(next);
            }
          }}
        >
          <option value="allocation">Allocations</option>
          <option value="activities">Activities</option>
        </StyledSelect>
      </div>

      {isAllocationView ? (
        <>
          <FilterField
            ariaLabel="Filter allocations by network"
            disabled={!hasSelectedPrime || networkOptions.length === 0}
            onChange={onNetworkChange}
            options={networkOptions}
            placeholder="All networks"
            value={selectedNetwork}
          />
          <FilterField
            ariaLabel="Filter allocations by protocol"
            disabled={!hasSelectedPrime || protocolOptions.length === 0}
            onChange={onProtocolChange}
            options={protocolOptions}
            placeholder="All protocols"
            value={selectedProtocol}
          />
        </>
      ) : null}
    </div>
  );
}
