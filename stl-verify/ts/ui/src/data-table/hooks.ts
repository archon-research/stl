import * as React from 'react';

import {
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type ColumnFiltersState,
  type OnChangeFn,
  type SortingState,
  type Table,
} from '@tanstack/react-table';
import { useCallback, useMemo } from 'react';

import { deserializeSorting, serializeSorting, validateSortingState } from './utils';
import type { DataTableConfig, UseUrlSyncedTableReturn } from './types';
import { useUrlParam } from '../lib/url-params';

/**
 * Hook to create a TanStack React Table instance with all standard features.
 * Handles sorting, column filtering, and global search.
 * @param data - Array of row data
 * @param columns - Column definitions
 * @param config - Table configuration
 * @returns TanStack Table instance
 */
export function useDataTable<T>(
  data: T[],
  columns: ColumnDef<T>[],
  config: DataTableConfig = {},
): Table<T> {
  const [internalSorting, setInternalSorting] = React.useState<SortingState>(
    config.defaultSorting ?? [],
  );
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>(
    [],
  );
  const [internalGlobalFilter, setInternalGlobalFilter] = React.useState('');

  const sorting = config.sorting ?? internalSorting;
  const globalFilter = config.globalFilter ?? internalGlobalFilter;

  const handleSortingChange: OnChangeFn<SortingState> =
    config.onSortingChange ?? setInternalSorting;
  const handleGlobalFilterChange =
    config.onGlobalFilterChange ?? setInternalGlobalFilter;

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      columnFilters,
      globalFilter,
    },
    onSortingChange: handleSortingChange,
    onColumnFiltersChange: setColumnFilters,
    onGlobalFilterChange: handleGlobalFilterChange,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: 'includesString',
    enableSorting: config.enableSorting,
    enableGlobalFilter: config.enableSearch,
  });

  return table;
}

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
      setSortParam(serializeSorting(resolvedSorting));
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
