import { css } from '#styled-system/css';

// House header style for every app DataTable, louder than the 11px (`2xs`) muted
// micro-label the recipe gives a `density="compact"` table. Sortable headers need
// no separate rule: the recipe's headerButton slot inherits font, color,
// text-transform and letter-spacing from the cell.
//
// One shared definition rather than a copy per table: three tables want the same
// header voice, and Panda emits the same arbitrary-selector class for identical
// source text anyway, so the copies were pure drift risk.
export const tableHeaderTypographyClassName = css({
  '& thead th': {
    fontSize: 'sm',
    fontWeight: 'semibold',
    // `shorter` is no token, so this shipped as an invalid
    // `line-height: shorter` the browser drops; `tight` is 1.25.
    lineHeight: 'tight',
    // Between the 'wide' (0.025em) and 'wider' (0.05em) steps.
    letterSpacing: '[0.02em]',
    textTransform: 'uppercase',
    color: 'text.default',
  },
});
