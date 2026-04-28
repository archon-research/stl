import { StyledSelect } from '@archon-research/design-system';
import { useEffect, useState, type ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { FilterOption } from '../../lib/dashboard';

type TopBarProps = {
  hasSelectedPrime: boolean;
  networkOptions: FilterOption[];
  onNetworkChange: (value: string | null) => void;
  onProtocolChange: (value: string | null) => void;
  onSearchChange: (value: string) => void;
  protocolOptions: FilterOption[];
  searchValue: string;
  selectedNetwork: string | null;
  selectedProtocol: string | null;
};

function SearchField({
  disabled,
  onChange,
  value,
}: {
  disabled: boolean;
  onChange: (value: string) => void;
  value: string;
}) {
  const [localValue, setLocalValue] = useState(value);

  useEffect(() => {
    setLocalValue(value);
  }, [value]);

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      onChange(localValue);
    }, 300);

    return () => window.clearTimeout(timeoutId);
  }, [localValue, onChange]);

  return (
    <div className={css({ minWidth: '16rem', flex: '1 1 18rem' })}>
      <input
        aria-label="Search allocations"
        type="search"
        value={localValue}
        onChange={(event) => setLocalValue(event.target.value)}
        placeholder="Search assets, protocols, chains"
        disabled={disabled}
        className={css({
          width: '100%',
          minWidth: 0,
          borderRadius: 'md',
          borderWidth: '1px',
          borderStyle: 'solid',
          borderColor: 'border.subtle',
          bg: 'surface.default',
          px: '3.5',
          py: '2.5',
          fontSize: 'sm',
          color: 'text.strong',
          outline: 'none',
          transitionProperty: 'border-color, box-shadow',
          transitionDuration: 'fast',
          _placeholder: { color: 'text.muted' },
          _focusVisible: {
            borderColor: 'interactive.accent',
            boxShadow: '0 0 0 3px rgba(59, 130, 246, 0.16)',
          },
          _disabled: {
            cursor: 'not-allowed',
            opacity: 0.6,
            bg: 'surface.subtle',
          },
        })}
      />
    </div>
  );
}

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
    <div className={css({ minWidth: '13rem' })}>
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
  networkOptions,
  onNetworkChange,
  onProtocolChange,
  onSearchChange,
  protocolOptions,
  searchValue,
  selectedNetwork,
  selectedProtocol,
}: TopBarProps) {
  return (
    <div className={flex({ align: 'end', gap: '3', wrap: 'wrap' })}>
      <SearchField
        disabled={!hasSelectedPrime}
        onChange={onSearchChange}
        value={searchValue}
      />
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
    </div>
  );
}
