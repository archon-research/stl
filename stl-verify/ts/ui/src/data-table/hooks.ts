import { useUrlSyncedTableStateAdapter } from '@archon-research/design-system';
import type { UseUrlSyncedTableReturn } from '@archon-research/design-system';

import { useUrlParam } from '../lib/url-params';

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

  return useUrlSyncedTableStateAdapter({
    sortParam,
    setSortParam,
    searchParam,
    setSearchParam,
  });
}
