import { ArrowDownRight, ArrowRightLeft, ArrowUpLeft } from 'lucide-react';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

export function getActionIcon(
  actionType: string | null | undefined,
): ReactNode {
  switch (actionType?.toLowerCase()) {
    case 'in':
      return <ArrowDownRight className={css({ width: '4', height: '4' })} />;
    case 'out':
      return <ArrowUpLeft className={css({ width: '4', height: '4' })} />;
    case 'sweep':
      return <ArrowRightLeft className={css({ width: '4', height: '4' })} />;
    default:
      return null;
  }
}

// Panda extracts styles from the *source text* of a `css()` call, so a caller
// doing `css({ color: actionColor })` on a value returned from here extracts
// nothing and emits no rule — Panda cannot resolve a runtime identifier. These
// four literal `css()` calls are therefore load-bearing: they are the only
// reason `.c_text\.interactive` (the `sweep` colour) exists in the stylesheet
// at all. Previously `sweep` rows rendered uncoloured, and the other three
// branches worked only by coincidence, from unrelated literal
// `color: 'text.warning'`-style usages in other files — deleting any one of
// those would have silently dropped an action colour here.
const ACTION_COLOR_CLASS = {
  in: css({ color: 'text.success' }),
  out: css({ color: 'text.warning' }),
  sweep: css({ color: 'text.interactive' }),
} as const;

const DEFAULT_ACTION_COLOR_CLASS = css({ color: 'text.default' });

export function getActionColorClass(
  actionType: string | null | undefined,
): string {
  const key = actionType?.toLowerCase();
  return key !== undefined && key in ACTION_COLOR_CLASS
    ? ACTION_COLOR_CLASS[key as keyof typeof ACTION_COLOR_CLASS]
    : DEFAULT_ACTION_COLOR_CLASS;
}
