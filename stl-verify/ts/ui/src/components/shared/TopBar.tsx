import {
  PageShell as DesignSystemPageShell,
  RangePicker,
  type RangePreset,
  StyledSelect,
  Tabs,
  type TimeRange,
} from '@archon-research/design-system';
import type { ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { FilterOption } from '../../lib/dashboard';
import { SettingsMenu, useDataSourceSection } from './SettingsMenu';

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
  // Range picker (rendered whenever all three props are provided)
  rangePreset?: RangePreset;
  timeRange?: TimeRange;
  onRangeChange?: (preset: RangePreset, range: TimeRange) => void;
};

const tabsListClassName = css({
  display: 'inline-flex',
  gap: '7',
});

// Primary navigation, so prominence comes from the weight/colour step between
// resting and selected plus the underline — not from an oversized type step,
// which at `lg`/`bold` shouted over the page title beneath it. The muted
// neutral indicator keeps the accent budget for data. The large list gap
// guarantees adjacent underlines never touch.
const tabTriggerClassName = css({
  appearance: 'none',
  bg: 'transparent',
  border: 'none',
  cursor: 'pointer',
  px: '0.5',
  pb: '2',
  fontSize: 'md',
  fontWeight: 'medium',
  color: 'text.muted',
  borderBottomWidth: '2px',
  borderBottomStyle: 'solid',
  borderBottomColor: 'transparent',
  transitionProperty: 'color, border-color',
  transitionDuration: 'fast',
  whiteSpace: 'nowrap',
  _hover: { color: 'text.default' },
  '&[data-selected]': {
    color: 'text.strong',
    fontWeight: 'semibold',
    borderBottomColor: 'text.strong',
  },
});

const rangeFieldClassName = css({
  width: { base: '100%', sm: '14rem' },
  flexShrink: 0,
});

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
        width: { base: '100%', sm: '11rem' },
        flexShrink: 0,
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
            {option.count > 0
              ? `${option.label} (${option.count})`
              : option.label}
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
  rangePreset,
  timeRange,
  onRangeChange,
}: TopBarProps) {
  const dataSource = useDataSourceSection();
  const showRangePicker =
    rangePreset !== undefined &&
    timeRange !== undefined &&
    onRangeChange !== undefined;

  return (
    // Wrapped in the content shell so the tabs line up with the card beneath
    // rather than sitting flush to the viewport. The shell in this slot already
    // starts inset from the content one, so only the remainder is added here —
    // repeating the content Panel's full padding overshoots it.
    <DesignSystemPageShell maxWidth="none">
      <div
        className={css({
          width: '100%',
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'flex-end',
          justifyContent: 'space-between',
          gap: '4',
          // Asymmetric because the inset is: this slot already starts right of
          // the content shell, so the left needs only the remainder, while the
          // right needs the content Panel's padding in full.
          paddingLeft: '1',
          paddingRight: '5',
        })}
      >
        <Tabs.Root
          value={selectedView}
          onValueChange={(details: { value: string }) => {
            if (
              details.value === 'allocation' ||
              details.value === 'activities'
            ) {
              onViewChange(details.value);
            }
          }}
          aria-label="Core navigation"
          className={css({ flexShrink: 0 })}
        >
          <Tabs.List className={tabsListClassName}>
            <Tabs.Trigger value="allocation" className={tabTriggerClassName}>
              Allocations
            </Tabs.Trigger>
            <Tabs.Trigger value="activities" className={tabTriggerClassName}>
              Activities
            </Tabs.Trigger>
          </Tabs.List>
        </Tabs.Root>

        <div
          className={flex({
            gap: '3',
            align: 'end',
            wrap: 'wrap',
            justify: 'flex-end',
          })}
        >
          <FilterField
            ariaLabel="Filter by network"
            disabled={networkOptions.length === 0}
            onChange={onNetworkChange}
            options={networkOptions}
            placeholder="All networks"
            value={selectedNetwork}
          />
          <FilterField
            ariaLabel="Filter by protocol"
            disabled={
              (!hasSelectedPrime && selectedView === 'allocation') ||
              protocolOptions.length === 0
            }
            onChange={onProtocolChange}
            options={protocolOptions}
            placeholder="All protocols"
            value={selectedProtocol}
          />
          {showRangePicker ? (
            <div className={rangeFieldClassName}>
              <RangePicker
                preset={rangePreset}
                range={timeRange}
                onChange={onRangeChange}
              />
            </div>
          ) : null}
          <SettingsMenu sections={[dataSource]} />
        </div>
      </div>
    </DesignSystemPageShell>
  );
}
