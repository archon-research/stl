import { type SortingState } from '@tanstack/react-table';
import { useCallback, useMemo } from 'react';

import { useUrlParam } from '../lib/url-params';
import type { UseUrlSyncedTableReturn } from './types';
import {
  deserializeSorting,
  serializeSorting,
  validateSortingState,
} from './utils';

/**
 * Hook to sync TanStack table state (sorting, global search) with URL query params.
 * Enables shareable/bookmarkable table states.
 *
 * @param sortParamKey - URL param name for sorting (e.g. 'sort')
 * @param searchParamKey - URL param name for search (e.g. 'q')
 * @returns Object with current sorting/filter state and setters
 */
export function useUrlSyncedTableState(
  sortParamKey: string = 'sort',
  searchParamKey: string = 'q',
): UseUrlSyncedTableReturn {
  const [sortParam, setSortParam] = useUrlParam(sortParamKey);
  const [searchParam, setSearchParam] = useUrlParam(searchParamKey);

  const sorting = useMemo(() => {
    return validateSortingState(deserializeSorting(sortParam));
  }, [sortParam]);

  const globalFilter = searchParam ?? '';

  const handleSetSorting = useCallback(
    (newSorting: SortingState | ((old: SortingState) => SortingState)) => {
      const resolvedSorting =
        typeof newSorting === 'function' ? newSorting(sorting) : newSorting;
      const serializedSorting = serializeSorting(resolvedSorting);
      setSortParam(serializedSorting || null);
    },
    [sorting, setSortParam],
  );

  const handleSetGlobalFilter = useCallback(
    (filter: string) => {
      setSearchParam(filter || null);
    },
    [setSearchParam],
  );

  return {
    sorting,
    globalFilter,
    setSorting: handleSetSorting,
    setGlobalFilter: handleSetGlobalFilter,
  };
}
