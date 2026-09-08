import { toSearchOption } from '@archon-research/router-kit';
import { useNavigate, useSearch } from '@tanstack/react-router';
import {
  useEffect,
  useMemo,
  useState,
  type Dispatch,
  type SetStateAction,
} from 'react';

import { ALLOCATION_CATEGORIES } from '../../shared/lib/search-params';
import type {
  Allocation,
  AllocationCategory,
} from '../../shared/types/allocation';
import type { UseAllocationGridArgs } from './useAllocationGrid';

export type UseAllocationGridFiltersArgs = Pick<
  UseAllocationGridArgs,
  'searchValue' | 'onSearchChange' | 'filteredAllocations'
>;

export type UseAllocationGridFiltersResult = {
  localSearchValue: string;
  setLocalSearchValue: Dispatch<SetStateAction<string>>;
  categoryFilter: AllocationCategory | '';
  handleCategoryChange: (value: string) => void;
  visibleAllocations: Allocation[];
  hasSearchQuery: boolean;
};

// Composes with — never replaces — the search box and the top bar's
// network/protocol filters: those are already applied upstream in
// `filteredAllocations`, and this narrows what survives them.
function filterAllocationsByCategory(
  filteredAllocations: Allocation[],
  categoryFilter: AllocationCategory | '',
): Allocation[] {
  return categoryFilter === ''
    ? filteredAllocations
    : filteredAllocations.filter(
        (allocation) => allocation.category === categoryFilter,
      );
}

function buildCategoryChangeHandler(
  navigate: ReturnType<typeof useNavigate>,
): (value: string) => void {
  return (value: string) => {
    void navigate({
      to: '.',
      search: (previous) => ({
        ...previous,
        category: toSearchOption(value, ALLOCATION_CATEGORIES),
      }),
      replace: true,
    });
  };
}

function scheduleSearchDebounce(
  value: string,
  onSearchChange: (value: string) => void,
): () => void {
  const timeoutId = window.setTimeout(() => {
    onSearchChange(value);
  }, 300);

  return () => window.clearTimeout(timeoutId);
}

/**
 * The grid's own filters: the search box's debounced local value, and the
 * URL-backed category chip.
 */
export function useAllocationGridFilters({
  searchValue,
  onSearchChange,
  filteredAllocations,
}: UseAllocationGridFiltersArgs): UseAllocationGridFiltersResult {
  const [localSearchValue, setLocalSearchValue] = useState(searchValue);

  // The category filter lives in the URL rather than in local state so it is
  // shareable alongside the other grid filters, and so the shell's per-prime
  // reset clears it with `network`/`protocol`.
  const allocationSearch = useSearch({ from: '/allocation' });
  const navigate = useNavigate();
  const categoryFilter: AllocationCategory | '' =
    allocationSearch?.category ?? '';

  const handleCategoryChange = buildCategoryChangeHandler(navigate);

  const visibleAllocations = useMemo(
    () => filterAllocationsByCategory(filteredAllocations, categoryFilter),
    [categoryFilter, filteredAllocations],
  );

  useEffect(() => {
    setLocalSearchValue(searchValue);
  }, [searchValue]);

  useEffect(() => {
    if (localSearchValue === searchValue) {
      return;
    }

    return scheduleSearchDebounce(localSearchValue, onSearchChange);
  }, [localSearchValue, onSearchChange, searchValue]);

  const hasSearchQuery = searchValue.trim().length > 0;

  return {
    localSearchValue,
    setLocalSearchValue,
    categoryFilter,
    handleCategoryChange,
    visibleAllocations,
    hasSearchQuery,
  };
}
