import { SearchInput, StyledSelect } from '@archon-research/design-system';
import type { ChangeEvent, Dispatch, SetStateAction } from 'react';

import { css } from '#styled-system/css';

import { getCategoryLabel } from '../../shared/lib/dashboard';
import { ALLOCATION_CATEGORIES } from '../../shared/lib/search-params';
import type { AllocationCategory, Prime } from '../../shared/types/allocation';

type AllocationGridFilterBarProps = {
  categoryFilter: AllocationCategory | '';
  handleCategoryChange: (value: string) => void;
  selectedPrime: Prime | null;
  localSearchValue: string;
  setLocalSearchValue: Dispatch<SetStateAction<string>>;
};

export function AllocationGridFilterBar({
  categoryFilter,
  handleCategoryChange,
  selectedPrime,
  localSearchValue,
  setLocalSearchValue,
}: AllocationGridFilterBarProps) {
  return (
    <div
      className={css({
        display: 'grid',
        gridTemplateColumns: {
          base: '1fr',
          lg: 'auto minmax(28rem, 36rem)',
        },
        gap: { base: '3', md: '4', lg: '5' },
        alignItems: 'end',
      })}
    >
      <span
        className={css({
          display: 'inline-flex',
          width: 'fit',
          alignItems: 'center',
          borderRadius: 'full',
          bg: 'bg.neutral',
          px: '3',
          py: '1',
          fontSize: 'xs',
          fontWeight: 'semibold',
          letterSpacing: 'widest',
          textTransform: 'uppercase',
          color: 'text.muted',
        })}
      >
        Allocations
      </span>
      {/* Same shape as the top bar's network/protocol filters — a
          `StyledSelect` in an 11rem cell whose placeholder option is the
          cleared state — so the three read as one filter family even though
          this one is scoped to the grid rather than the page. */}
      <div
        className={css({
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'end',
          gap: '3',
          minWidth: '0',
          width: 'full',
          justifySelf: { lg: 'end' },
        })}
      >
        <div
          className={css({
            width: { base: 'full', sm: '44' },
            flexShrink: 0,
          })}
        >
          <StyledSelect
            aria-label="Filter by category"
            value={categoryFilter}
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              handleCategoryChange(event.target.value)
            }
            disabled={!selectedPrime}
          >
            <option value="">All categories</option>
            {ALLOCATION_CATEGORIES.map((category) => (
              <option key={category} value={category}>
                {getCategoryLabel(category)}
              </option>
            ))}
          </StyledSelect>
        </div>
        <div
          className={css({
            flexGrow: '1',
            flexShrink: '1',
            flexBasis: '64',
            minWidth: '0',
          })}
        >
          <SearchInput
            aria-label="Search allocations"
            disabled={!selectedPrime}
            onValueChange={setLocalSearchValue}
            placeholder="Search assets, protocols, chains"
            value={localSearchValue}
          />
        </div>
      </div>
    </div>
  );
}
