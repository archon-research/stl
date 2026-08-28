import { useUrlSyncedTableStateAdapter } from '@archon-research/design-system';
import type { UseUrlSyncedTableReturn } from '@archon-research/design-system';
import { createUrlSyncedTableAdapter } from '@archon-research/router-kit';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { useMemo } from 'react';

/**
 * Syncs TanStack table state (sorting, global search) with the allocation
 * route's `sort`/`q` search params, so a table state is shareable.
 *
 * The read is scoped to the `/allocation` route schema, so a table on another
 * route gets its own keys rather than sharing one namespace with this one. That
 * scoping is not enforcement: no route sets `search.strict`, so an unvalidated
 * `sort`/`q` still travels in the URL until the entry-time cleanup drops it.
 * `sort` is validated for shape only (upstream `validateSortingState`), never
 * against a column set, so a stale bookmark naming a dropped column renders
 * unsorted while the URL keeps advertising it.
 */
export function useUrlSyncedTableState(): UseUrlSyncedTableReturn {
  // Not strict: the allocation route is what calls this, but the same
  // non-throwing read is what every other consumer of this search makes, and a
  // throw here would take the whole view down rather than one table's state.
  const search = useSearch({ from: '/allocation', shouldThrow: false });
  const navigate = useNavigate();

  // Keyed on the two params, not on the whole search object the factory's own
  // example memoises on: that identity changes with any param, and a fresh
  // adapter re-arms AllocationGrid's 300ms search debounce on every one.
  const adapter = useMemo(
    () =>
      createUrlSyncedTableAdapter({
        search: { sort: search?.sort, q: search?.q },
        sortKey: 'sort',
        searchKey: 'q',
        navigate,
      }),
    [search?.q, search?.sort, navigate],
  );

  return useUrlSyncedTableStateAdapter(adapter);
}
