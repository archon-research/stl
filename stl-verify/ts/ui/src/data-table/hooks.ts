import { useUrlSyncedTableStateAdapter } from '@archon-research/design-system';
import type { UseUrlSyncedTableReturn } from '@archon-research/design-system';
import { useMemo } from 'react';

import { useUrlParam } from '../lib/url-params';

/**
 * Hook to sync TanStack table state (sorting, global search) with URL query params.
 * Enables shareable/bookmarkable table states.
 *
 * The sort param is validated for shape only (upstream `validateSortingState`),
 * never against a table's column set, and `sort`/`q` are one namespace shared by
 * every table (see `PARAMS`). So a stale bookmark or a sibling table's column id
 * renders unsorted while the URL keeps advertising the sort. Stripping the param
 * here is not the fix: whichever table mounted first would clobber the other's
 * sort. Splitting the keys per table is, and that needs both tables' owners.
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

  // The adapter identity is load-bearing: `useUrlSyncedTableStateAdapter`
  // memoises the setters it returns on this object, so a fresh literal per
  // render makes `setGlobalFilter` a new reference every time. That reference is
  // a dependency of AllocationGrid's 300ms search debounce, which would then be
  // torn down and re-armed on every unrelated re-render (a resolving fetch, a
  // URL param change) and never commit the search to the URL under a burst.
  const adapter = useMemo(
    () => ({ sortParam, setSortParam, searchParam, setSearchParam }),
    [sortParam, setSortParam, searchParam, setSearchParam],
  );

  return useUrlSyncedTableStateAdapter(adapter);
}
