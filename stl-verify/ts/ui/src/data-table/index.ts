/**
 * Data Table Module
 *
 * This module provides a shared, reusable foundation for building sortable,
 * searchable tables using TanStack React Table. It's designed to be consumed
 * by feature-specific table implementations (allocations, risk breakdown, etc.).
 *
 * Core exports:
 * - DataTable component for consistent table rendering
 * - Types for table state and configuration
 * - Utilities for string normalization, search matching, and URL serialization
 * - Hooks for table instantiation and URL-synced state management
 * - Panda CSS recipes for consistent table presentation
 *
 * Usage pattern:
 * 1. Import hooks and types from this module
 * 2. Define your domain-specific column definitions using TanStack patterns
 * 3. Compose sorting/search state management via useUrlSyncedTableState
 * 4. Render using DataTable component or build custom with recipes
 */

export { DataTable } from './DataTable';
export * from './types';
export * from './utils';
export * from './hooks';
export * from './recipes';
