import { StyledSelect } from '@archon-research/design-system';
import type { ChangeEvent } from 'react';

import { css } from '#styled-system/css';

import {
  filterFieldClassName,
  filterLabelClassName,
} from './activityFilterStyles';

const ACTION_FILTER_OPTIONS = [
  { label: 'All actions', value: '' },
  { label: 'In', value: 'in' },
  { label: 'Out', value: 'out' },
  { label: 'Sweep', value: 'sweep' },
];

function normalizeFilterValue(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

type ActivityFilterBarProps = {
  actionFilter?: string;
  onActionFilterChange?: (value: string | null) => void;
  tokenFilter: string | null;
  onTokenFilterChange?: (value: string | null) => void;
  tokenOptions: string[];
  tokenOptionsFailed: boolean;
  hasActiveFilters: boolean;
  onClearFilters: () => void;
};

export function ActivityFilterBar({
  actionFilter,
  onActionFilterChange,
  tokenFilter,
  onTokenFilterChange,
  tokenOptions,
  tokenOptionsFailed,
  hasActiveFilters,
  onClearFilters,
}: ActivityFilterBarProps) {
  return (
    <div className={css({ display: 'grid', gap: '3' })}>
      <div
        className={css({
          display: 'grid',
          gridTemplateColumns: {
            base: '1fr',
            sm: 'repeat(2, minmax(0, 1fr))',
            lg: 'repeat(4, minmax(0, 1fr))',
          },
          gap: '3',
          alignItems: 'end',
        })}
      >
        <label className={filterFieldClassName}>
          <span className={filterLabelClassName}>Action</span>
          <StyledSelect
            aria-label="Filter activity by action"
            value={actionFilter ?? ''}
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              onActionFilterChange?.(event.target.value || null)
            }
          >
            {ACTION_FILTER_OPTIONS.map((option) => (
              <option key={option.value || 'all'} value={option.value}>
                {option.label}
              </option>
            ))}
          </StyledSelect>
        </label>
        {tokenOptions.length > 0 || tokenOptionsFailed ? (
          <label className={filterFieldClassName}>
            <span className={filterLabelClassName}>Token</span>
            <StyledSelect
              aria-label="Filter activity by token symbol"
              disabled={tokenOptions.length === 0}
              value={tokenFilter ?? ''}
              onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                onTokenFilterChange?.(
                  normalizeFilterValue(event.target.value) ?? null,
                )
              }
            >
              {/* The same slot the network and protocol chips say it in: an
                  empty, disabled field cannot say which of the two it is. */}
              <option value="">
                {tokenOptionsFailed ? 'Token list failed' : 'All tokens'}
              </option>
              {tokenOptions.map((symbol) => (
                <option key={symbol} value={symbol}>
                  {symbol}
                </option>
              ))}
            </StyledSelect>
          </label>
        ) : null}
      </div>
      {hasActiveFilters ? (
        <div
          className={css({
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: '3',
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          <span>Server filters active</span>
          <button
            type="button"
            onClick={onClearFilters}
            className={css({
              h: '8',
              borderRadius: 'md',
              borderWidth: '1px',
              borderStyle: 'solid',
              borderColor: 'border.subtle',
              bg: 'surface.default',
              color: 'text.default',
              px: '3',
              fontSize: 'xs',
              cursor: 'pointer',
              _hover: { bg: 'surface.hover' },
            })}
          >
            Clear filters
          </button>
        </div>
      ) : null}
    </div>
  );
}
