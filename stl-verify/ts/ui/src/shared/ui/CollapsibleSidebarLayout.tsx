import { SidebarLayout } from '@archon-research/design-system';
import { type ReactNode, useEffect, useRef } from 'react';

import { css } from '#styled-system/css';

/**
 * DOM id of the top bar's collapse toggle. This layout swaps its whole subtree
 * when the sidebar collapses, so it needs a stable handle on the control that
 * triggered the swap in order to hand keyboard focus back to it.
 */
export const SIDEBAR_TOGGLE_ID = 'prime-sidebar-toggle';

type CollapsibleSidebarLayoutProps = {
  isSidebarCollapsed: boolean;
  main: ReactNode;
  sidebar: ReactNode;
  topBar: ReactNode;
};

// The design system's `SidebarLayout` ships no collapse prop: its sidebar is an
// Ark Splitter panel whose min/max widths are baked into the panel config, so no
// combination of props (or CSS short of `!important` over Ark's inline
// flex-basis) drives it to zero width. Collapsing therefore drops the split
// entirely and renders the main column on its own, which is what actually
// reclaims the horizontal space — a zero-width panel would still be capped by
// the main panel's inline `max-width`, leaving a gap.
//
// `SidebarLayout` owns the resized sidebar width and persists it to
// localStorage, so re-expanding restores the width the user dragged to.
//
// The collapsed branch mirrors the `sidebarLayout` slot recipe's
// root/main/topBar/mainColumn/content slots — same semantic tokens, same
// metrics, same nesting — so the top bar and content frame are identical in
// both states and only the sidebar track disappears.
const rootClassName = css({
  width: 'full',
  height: 'screen',
  minWidth: '0',
  overflow: 'hidden',
});

const mainClassName = css({
  display: 'flex',
  flexDirection: 'column',
  minWidth: '0',
  minHeight: '0',
  height: 'full',
  overflow: 'hidden',
  bg: 'surface.default',
});

const topBarClassName = css({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'flex-end',
  px: '4',
  py: '3',
  minHeight: '16',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
  bg: 'surface.default',
});

const mainColumnClassName = css({
  display: 'flex',
  flexDirection: 'column',
  minWidth: '0',
  minHeight: '0',
  height: 'full',
  flex: '1',
  overflow: 'hidden',
  bg: 'surface.default',
});

const contentClassName = css({
  minWidth: '0',
  minHeight: '0',
  flex: '1',
  overflow: 'auto',
  bg: 'surface.default',
});

export function CollapsibleSidebarLayout({
  isSidebarCollapsed,
  main,
  sidebar,
  topBar,
}: CollapsibleSidebarLayoutProps) {
  const renderedCollapsed = useRef(isSidebarCollapsed);

  // Swapping branches unmounts the subtree that holds the toggle, so the button
  // the user just pressed disappears and focus drops to <body> — a keyboard user
  // would have to tab in from the top again. Hand focus back to the re-rendered
  // toggle, but only when it was genuinely lost: if anything is still focused,
  // the collapse came from elsewhere and stealing focus would be worse.
  useEffect(() => {
    if (renderedCollapsed.current === isSidebarCollapsed) {
      return;
    }

    renderedCollapsed.current = isSidebarCollapsed;

    const active = document.activeElement;
    if (active !== null && active !== document.body) {
      return;
    }

    document.getElementById(SIDEBAR_TOGGLE_ID)?.focus();
  }, [isSidebarCollapsed]);

  if (isSidebarCollapsed) {
    return (
      <div className={rootClassName}>
        <div className={mainClassName}>
          <header className={topBarClassName}>{topBar}</header>
          <div className={mainColumnClassName}>
            <div className={contentClassName}>{main}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <SidebarLayout
      collapseBelow={768}
      sidebar={sidebar}
      topBar={topBar}
      main={main}
    />
  );
}
