import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { FilterOption } from '../../lib/dashboard';
import type { Star } from '../../types/allocation';
import { StyledSelect } from './StyledSelect';

type TopBarProps = {
  allocationCount: number;
  filteredAllocationCount: number;
  networkOptions: FilterOption[];
  onNetworkChange: (value: string | null) => void;
  onProtocolChange: (value: string | null) => void;
  protocolOptions: FilterOption[];
  selectedNetwork: string | null;
  selectedProtocol: string | null;
  selectedStar: Star | null;
  starCount: number;
};

function formatAddress(address: string): string {
  if (address.length <= 18) {
    return address;
  }

  return `${address.slice(0, 10)}…${address.slice(-6)}`;
}

function StatusPill({ label }: { label: string }) {
  return (
    <span
      className={css({
        display: 'inline-flex',
        alignItems: 'center',
        px: '2.5',
        py: '1.5',
        borderRadius: 'sm',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: 'border.subtle',
        bg: 'surface.subtle',
        fontSize: 'xs',
        color: 'text.muted',
      })}
    >
      {label}
    </span>
  );
}

function FilterField({
  disabled,
  label,
  onChange,
  options,
  placeholder,
  value,
}: {
  disabled: boolean;
  label: string;
  onChange: (value: string | null) => void;
  options: FilterOption[];
  placeholder: string;
  value: string | null;
}) {
  return (
    <label className={css({ display: 'grid', gap: '1', minWidth: '13rem' })}>
      <span
        className={css({
          fontSize: 'xs',
          textTransform: 'uppercase',
          letterSpacing: '0.14em',
          color: 'text.muted',
        })}
      >
        {label}
      </span>
      <StyledSelect
        value={value ?? ''}
        onChange={(event) => onChange(event.target.value || null)}
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
  allocationCount,
  filteredAllocationCount,
  networkOptions,
  onNetworkChange,
  onProtocolChange,
  protocolOptions,
  selectedNetwork,
  selectedProtocol,
  selectedStar,
  starCount,
}: TopBarProps) {
  return (
    <div className={css({ display: 'grid', gap: '4' })}>
      <div
        className={flex({
          align: 'center',
          justify: 'space-between',
          gap: '4',
          wrap: 'wrap',
        })}
      >
        <div className={css({ display: 'grid', gap: '1' })}>
          <p
            className={css({
              m: 0,
              fontSize: 'xs',
              textTransform: 'uppercase',
              letterSpacing: '0.16em',
              color: 'text.muted',
            })}
          >
            Selected star
          </p>
          <div className={flex({ align: 'baseline', gap: '3', wrap: 'wrap' })}>
            <h1
              className={css({
                m: 0,
                fontSize: 'lg',
                lineHeight: 'tight',
                color: 'text.strong',
              })}
            >
              {selectedStar ? selectedStar.name : 'Choose a star'}
            </h1>
            {selectedStar ? (
              <span className={css({ fontSize: 'xs', color: 'text.muted' })}>
                {formatAddress(selectedStar.address)}
              </span>
            ) : null}
          </div>
        </div>

        <div className={flex({ align: 'center', gap: '2', wrap: 'wrap' })}>
          <StatusPill label={`${starCount} stars`} />
          <StatusPill
            label={`${filteredAllocationCount}/${allocationCount} positions`}
          />
        </div>
      </div>

      <div className={flex({ align: 'end', gap: '3', wrap: 'wrap' })}>
        <FilterField
          disabled={!selectedStar || networkOptions.length === 0}
          label="Network"
          onChange={onNetworkChange}
          options={networkOptions}
          placeholder="All networks"
          value={selectedNetwork}
        />
        <FilterField
          disabled={!selectedStar || protocolOptions.length === 0}
          label="Protocol"
          onChange={onProtocolChange}
          options={protocolOptions}
          placeholder="All protocols"
          value={selectedProtocol}
        />
      </div>
    </div>
  );
}
