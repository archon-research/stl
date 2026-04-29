/**
 * Recipes for shared table presentation.
 * These Panda CSS classNames can be reused by any table consumer to maintain
 * consistent styling while keeping domain columns local.
 */

/**
 * Styles for table wrapper. Handles overflow, borders, and layout.
 */
export const tableRecipes = {
  wrapper: () => ({
    overflowX: 'auto' as const,
    borderRadius: 'md',
    borderStyle: 'solid' as const,
    borderWidth: '1px',
    borderColor: 'border.subtle',
  }),
  table: () => ({
    width: '100%',
    minWidth: '48rem',
    borderCollapse: 'collapse' as const,
    bg: 'surface.default',
  }),
};

/**
 * Styles for table header (thead)
 */
export const headerRecipes = {
  headerRow: () => ({
    bg: 'surface.subtle',
  }),
  headerCell: () => ({
    px: '4',
    py: '3',
    textAlign: 'left' as const,
    fontSize: 'xs',
    fontWeight: 'semibold' as const,
    letterSpacing: '0.08em',
    textTransform: 'uppercase' as const,
    color: 'text.muted',
    cursor: 'pointer' as const, // Sortable headers should be clickable
    userSelect: 'none' as const,
    '&:hover': {
      bg: 'surface.default',
    },
  }),
  sortIndicator: () => ({
    ml: '1',
    display: 'inline-block',
    fontSize: '0.75em',
    opacity: 0.6,
  }),
};

/**
 * Styles for table body (tbody)
 */
export const bodyRecipes = {
  bodyRow: (isSelected: boolean = false) => ({
    cursor: 'pointer' as const,
    bg: isSelected ? 'interactive.selected' : 'surface.default',
    transitionDuration: 'fast' as const,
    transitionProperty: 'background-color',
    '&:hover': { bg: 'interactive.hover' },
    '&:focus-visible': { bg: 'interactive.hover' },
  }),
  bodyCell: () => ({
    borderBottomWidth: '1px',
    borderBottomStyle: 'solid' as const,
    borderBottomColor: 'border.subtle',
    px: '4',
    py: '3.5',
  }),
  cellValue: () => ({
    m: 0,
    fontSize: 'sm',
    color: 'text.strong',
  }),
};

/**
 * Styles for empty/loading states
 */
export const stateRecipes = {
  emptyState: () => ({
    borderRadius: 'md',
    borderStyle: 'solid' as const,
    borderWidth: '1px',
    borderColor: 'border.subtle',
    bg: 'surface.subtle',
    p: '4',
  }),
  emptyStateText: () => ({
    m: 0,
    fontSize: 'sm',
    color: 'text.muted',
  }),
  loadingRow: () => ({
    borderBottomWidth: '1px',
    borderBottomStyle: 'solid' as const,
    borderBottomColor: 'border.subtle',
  }),
  skeletonCell: () => ({
    height: '7',
    borderRadius: 'sm',
    bg: 'surface.subtle',
    opacity: 0.85,
  }),
};
