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

// Panda extracts styles from the *source text* of a `css()` call, so handing a
// caller a token path to put in `css({ color: value })` emits no rule at all.
// These literal calls are load-bearing: they are the only reason
// `.c_text\.interactive` (the `sweep` colour) exists in the stylesheet.
const ACTION_COLOR_CLASS = {
  in: css({ color: 'text.success' }),
  out: css({ color: 'text.warning' }),
  sweep: css({ color: 'text.interactive' }),
} as const;

const DEFAULT_ACTION_COLOR_CLASS = css({ color: 'text.default' });

// Own-property check, not `in`: `action_type` is unvalidated API text, and
// `'constructor' in ACTION_COLOR_CLASS` is true, which would return a function
// and leave the element with no colour class at all.
function isActionType(key: string): key is keyof typeof ACTION_COLOR_CLASS {
  return Object.hasOwn(ACTION_COLOR_CLASS, key);
}

export function getActionColorClass(
  actionType: string | null | undefined,
): string {
  const key = actionType?.toLowerCase();
  return key !== undefined && isActionType(key)
    ? ACTION_COLOR_CLASS[key]
    : DEFAULT_ACTION_COLOR_CLASS;
}
