import {
  PageShell as DesignSystemPageShell,
  RangePicker,
  type RangePreset,
  StyledSelect,
  Tabs,
  type TimeRange,
} from '@archon-research/design-system';
import { PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import type { ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { FilterOption } from '../lib/dashboard';
import type { Provenance } from '../types/allocation';
import { SIDEBAR_TOGGLE_ID } from './CollapsibleSidebarLayout';
import { SettingsMenu, useDataSourceSection } from './SettingsMenu';

type TopBarProps = {
  hasSelectedPrime: boolean;
  /** Whether the prime list is currently collapsed away. */
  isSidebarCollapsed: boolean;
  onToggleSidebar: () => void;
  onViewChange: (view: 'allocation' | 'activities') => void;
  // The pointer or focus reaching a tab, which is the cue to fetch that view's
  // chunk. Optional: a caller with nothing to warm simply omits it.
  onViewIntent?: (view: 'allocation' | 'activities') => void;
  networkOptions: FilterOption[];
  /** Whether the empty option list is a failed read rather than a short one. */
  networkOptionsFailed: boolean;
  onNetworkChange: (value: string | null) => void;
  onProtocolChange: (value: string | null) => void;
  protocolOptions: FilterOption[];
  protocolOptionsFailed: boolean;
  selectedNetwork: string | null;
  selectedProtocol: string | null;
  selectedView: 'allocation' | 'activities';
  // Range picker (rendered whenever all three props are provided)
  rangePreset?: RangePreset;
  timeRange?: TimeRange;
  onRangeChange?: (preset: RangePreset, range: TimeRange) => void;
  /** Provenances the selected prime can be served from. */
  availableProvenances?: readonly Provenance[];
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
  transitionProperty: 'colors',
  transitionDuration: 'fast',
  whiteSpace: 'nowrap',
  _hover: { color: 'text.default' },
  '&[data-selected]': {
    color: 'text.strong',
    fontWeight: 'semibold',
    borderBottomColor: 'text.strong',
  },
});

// Square icon button matching the settings trigger on the far right of this
// bar, so the two ends of the top bar read as the same control family. Height
// is the `9` step (2.25rem), which lines the button up with the tab block.
const sidebarToggleClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  appearance: 'none',
  height: '9',
  width: '9',
  p: '0',
  flexShrink: 0,
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  borderRadius: 'md',
  background: 'surface.default',
  color: 'text.muted',
  cursor: 'pointer',
  transitionProperty: 'colors',
  transitionDuration: 'fast',
  _hover: { color: 'text.strong', borderColor: 'border.default' },
  _focusVisible: {
    outlineWidth: '2px',
    outlineStyle: 'solid',
    outlineColor: 'interactive.accent',
    outlineOffset: '0.5',
  },
});

const rangeFieldClassName = css({
  width: { base: 'full', sm: '56' },
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
        width: { base: 'full', sm: '44' },
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
  isSidebarCollapsed,
  onToggleSidebar,
  onViewChange,
  onViewIntent,
  networkOptions,
  networkOptionsFailed,
  onNetworkChange,
  onProtocolChange,
  protocolOptions,
  protocolOptionsFailed,
  selectedNetwork,
  selectedProtocol,
  selectedView,
  rangePreset,
  timeRange,
  onRangeChange,
  availableProvenances,
}: TopBarProps) {
  const dataSource = useDataSourceSection(availableProvenances);
  const showRangePicker =
    rangePreset !== undefined &&
    timeRange !== undefined &&
    onRangeChange !== undefined;

  return (
    // Wrapped in the content shell so the tabs line up with the card beneath
    // rather than sitting flush to the viewport.
    <DesignSystemPageShell maxWidth="none">
      <div
        className={css({
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'flex-end',
          justifyContent: 'space-between',
          gap: '4',
          // Pulled back out to the content card's own bounds. The layout's top
          // bar slot carries horizontal padding the content column does not, so
          // this shell starts inset from that one and the row has to give it
          // back — padding could only push it further in.
          //
          // Raw pixels, and no `width: 100%`: these cancel a measured layout
          // inset rather than express a spacing choice, and a pinned width made
          // the negative margins slide the row left instead of widening it,
          // leaving the right edge 21px short of the card. The two differ
          // because the content column and this slot resolve to different
          // widths; measured against the card at 1680px, both edges land within
          // a pixel.
          marginLeft: '-4',
          // A device-pixel correction rather than a spacing choice.
          marginRight: '[-5px]',
        })}
      >
        <div className={flex({ align: 'center', gap: '4', flexShrink: 0 })}>
          {/* Leads the navigation because it governs what sits to its left:
              the prime list. The icon states the direction of travel — closing
              chevron while open, opening chevron while collapsed. */}
          <button
            type="button"
            id={SIDEBAR_TOGGLE_ID}
            onClick={onToggleSidebar}
            aria-expanded={!isSidebarCollapsed}
            aria-label={
              isSidebarCollapsed ? 'Expand prime list' : 'Collapse prime list'
            }
            title={
              isSidebarCollapsed ? 'Expand prime list' : 'Collapse prime list'
            }
            className={sidebarToggleClassName}
          >
            {isSidebarCollapsed ? (
              <PanelLeftOpen size={18} aria-hidden="true" />
            ) : (
              <PanelLeftClose size={18} aria-hidden="true" />
            )}
          </button>

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
              <Tabs.Trigger
                value="allocation"
                className={tabTriggerClassName}
                onMouseEnter={() => onViewIntent?.('allocation')}
                onFocus={() => onViewIntent?.('allocation')}
              >
                Allocations
              </Tabs.Trigger>
              <Tabs.Trigger
                value="activities"
                className={tabTriggerClassName}
                onMouseEnter={() => onViewIntent?.('activities')}
                onFocus={() => onViewIntent?.('activities')}
              >
                Activities
              </Tabs.Trigger>
            </Tabs.List>
          </Tabs.Root>
        </div>

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
            // A registry that failed leaves the same empty, disabled field an
            // unanswered one does; the placeholder is the only slot that can
            // say which of the two the reader is looking at. Kept short — the
            // field is 122px of text and a native select clips without an
            // ellipsis.
            placeholder={
              networkOptionsFailed ? 'Network list failed' : 'All networks'
            }
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
            placeholder={
              protocolOptionsFailed ? 'Protocol list failed' : 'All protocols'
            }
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
