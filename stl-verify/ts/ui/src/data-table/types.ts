import type {
  ColumnDef,
  OnChangeFn,
  SortingState,
} from '@tanstack/react-table';

/**
 * Shared table configuration for consistent behavior across consumers.
 * Supports optional global search and column sorting.
 */
export interface DataTableConfig {
  /** Enable global text search across row data */
  enableSearch?: boolean;
  /** Enable column sorting */
  enableSorting?: boolean;
  /** Controlled sorting state */
  sorting?: SortingState;
  /** Controlled global search value */
  globalFilter?: string;
  /** Controlled sorting change handler */
  onSortingChange?: OnChangeFn<SortingState>;
  /** Controlled global search change handler */
  onGlobalFilterChange?: (filter: string) => void;
  /** Default sorting state on mount */
  defaultSorting?: SortingState;
  /** Global search debounce time in ms */
  searchDebounceMs?: number;
}

/**
 * Normalized search result for a row. Used internally to cache
 * computed search strings so filtering doesn't recompute on every keystroke.
 */
export interface SearchableRow {
  /** Original row data */
  data: unknown;
  /** Normalized search string (lowercase, no symbols) for fast contains matching */
  searchString: string;
}

/**
 * Table state shape for URL syncing and consumer composition.
 * Consumers can subscribe to these changes and persist to query params.
 */
export interface DataTableState {
  sorting: SortingState;
  globalFilter: string;
}

/**
 * Contract for a column definition factory that preserves type safety
 * and TanStack Column requirements. Consumers use this to define
 * typed columns without repeating shared patterns.
 */
export type TypedColumnDef<T> = ColumnDef<T> & {
  /** Optional metadata for search/filter hints */
  searchable?: boolean;
  sortable?: boolean;
};

/**
 * Hook return type for URL-synced table state.
 * Decouples consumer state management from URL persistence.
 */
export interface UseUrlSyncedTableReturn {
  sorting: SortingState;
  globalFilter: string;
  setSorting: OnChangeFn<SortingState>;
  setGlobalFilter: (filter: string) => void;
}
