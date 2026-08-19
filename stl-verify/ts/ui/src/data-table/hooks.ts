import { useUrlSyncedTableStateAdapter } from '@archon-research/design-system';
import type { UseUrlSyncedTableReturn } from '@archon-research/design-system';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { useCallback, useMemo } from 'react';

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
  // Not strict: the drawer hosting this table stays mounted on the activities
  // route, where the allocation search does not exist.
  const search = useSearch({ from: '/allocation', shouldThrow: false });
  const navigate = useNavigate();

  const setSortParam = useCallback(
    (value: string | null) => {
      void navigate({
        to: '.',
        search: (previous) => ({ ...previous, sort: value ?? undefined }),
        replace: true,
      });
    },
    [navigate],
  );

  const setSearchParam = useCallback(
    (value: string | null) => {
      void navigate({
        to: '.',
        search: (previous) => ({ ...previous, q: value ?? undefined }),
        replace: true,
      });
    },
    [navigate],
  );

  // The adapter identity is load-bearing: `useUrlSyncedTableStateAdapter`
  // memoises the setters it returns on this object, so a fresh literal per
  // render makes `setGlobalFilter` a new reference every time. That reference is
  // a dependency of AllocationGrid's 300ms search debounce, which would then be
  // torn down and re-armed on every unrelated re-render (a resolving fetch, a
  // URL param change) and never commit the search to the URL under a burst.
  const adapter = useMemo(
    () => ({
      sortParam: search?.sort ?? null,
      setSortParam,
      searchParam: search?.q ?? null,
      setSearchParam,
    }),
    [search?.q, search?.sort, setSearchParam, setSortParam],
  );

  return useUrlSyncedTableStateAdapter(adapter);
}
