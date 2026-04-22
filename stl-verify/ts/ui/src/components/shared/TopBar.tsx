import type { ChangeEvent } from 'react';

import { StyledSelect } from '@archon-research/design-system';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { FilterOption } from '../../lib/dashboard';

type TopBarProps = {
  hasSelectedStar: boolean;
  networkOptions: FilterOption[];
  onNetworkChange: (value: string | null) => void;
  onProtocolChange: (value: string | null) => void;
  protocolOptions: FilterOption[];
  selectedNetwork: string | null;
  selectedProtocol: string | null;
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
  hasSelectedStar,
  networkOptions,
  onNetworkChange,
  onProtocolChange,
  protocolOptions,
  selectedNetwork,
  selectedProtocol,
}: TopBarProps) {
  return (
    <div className={flex({ align: 'end', gap: '3', wrap: 'wrap' })}>
      <FilterField
        ariaLabel="Filter allocations by network"
        disabled={!hasSelectedStar || networkOptions.length === 0}
        onChange={onNetworkChange}
        options={networkOptions}
        placeholder="All networks"
        value={selectedNetwork}
      />
      <FilterField
        ariaLabel="Filter allocations by protocol"
        disabled={!hasSelectedStar || protocolOptions.length === 0}
        onChange={onProtocolChange}
        options={protocolOptions}
        placeholder="All protocols"
        value={selectedProtocol}
      />
    </div>
  );
}
