import { Badge, ThemeToggle } from '@archon-research/design-system';
import { Link, Outlet } from '@tanstack/react-router';
import { PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import { useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { CollapsibleSidebarLayout } from '../../shared/ui/CollapsibleSidebarLayout.tsx';
import { SIDEBAR_TOGGLE_ID } from '../../shared/ui/CollapsibleSidebarLayout.tsx';
import { RESOURCES } from '../schema/registry.ts';

/**
 * The shell.
 *
 * Reuses the app's own `CollapsibleSidebarLayout` rather than a second
 * implementation: it already owns the resizable split, persists the dragged
 * width, and hands focus back to the toggle across the subtree swap that
 * collapsing performs. A curation-specific copy would be the same code with a
 * different bug surface.
 *
 * The navigation is generated from the registry, so a new resource appears in it
 * without anyone editing this file.
 */
export function CurationLayout() {
  const [isSidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <div className={shellClassName}>
      <div data-sidebar-layout>
        <CollapsibleSidebarLayout
          isSidebarCollapsed={isSidebarCollapsed}
          sidebar={<CurationSidebar />}
          topBar={
            <div className={topBarClassName}>
              <button
                type="button"
                id={SIDEBAR_TOGGLE_ID}
                onClick={() => setSidebarCollapsed((collapsed) => !collapsed)}
                aria-expanded={!isSidebarCollapsed}
                aria-label={
                  isSidebarCollapsed
                    ? 'Expand navigation'
                    : 'Collapse navigation'
                }
                title={
                  isSidebarCollapsed
                    ? 'Expand navigation'
                    : 'Collapse navigation'
                }
                className={toggleClassName}
              >
                {isSidebarCollapsed ? (
                  <PanelLeftOpen size={18} aria-hidden="true" />
                ) : (
                  <PanelLeftClose size={18} aria-hidden="true" />
                )}
              </button>

              <span className={brandClassName}>Combined master</span>
              <Badge>curation spike</Badge>
            </div>
          }
          main={<Outlet />}
        />
      </div>
    </div>
  );
}

function CurationSidebar() {
  return (
    <nav className={sidebarClassName} aria-label="Curation">
      <div className={navScrollClassName}>
        <span className={navHeadingClassName}>Curate</span>
        {RESOURCES.map((resource) => (
          <Link
            key={resource.key}
            to="/$resourceKey"
            params={{ resourceKey: resource.key }}
            className={navLinkClassName}
            activeProps={{ 'data-selected': 'true' }}
          >
            {resource.label}
          </Link>
        ))}

        <span className={navHeadingClassName}>Workflows</span>
        <Link
          to="/workflow/classify"
          className={navLinkClassName}
          activeProps={{ 'data-selected': 'true' }}
        >
          Classify a security
        </Link>
        <Link
          to="/workflow/import-prices"
          className={navLinkClassName}
          activeProps={{ 'data-selected': 'true' }}
        >
          Import prices
        </Link>
        <Link
          to="/worklist"
          className={navLinkClassName}
          activeProps={{ 'data-selected': 'true' }}
        >
          Validity worklist
        </Link>
      </div>

      {/* The footer is where the stl UI puts its persistent controls, and these
          two are the same kind of thing: neither is navigation, and neither
          belongs in a top bar that now carries only the collapse affordance. */}
      <div className={sidebarFooterClassName}>
        <span className={offlineClassName}>
          <span className={offlineDotClassName} aria-hidden="true" />
          offline · msw
        </span>
        <ThemeToggle />
      </div>
    </nav>
  );
}

const shellClassName = css({
  height: 'screen',
  overflow: 'hidden',
  bg: 'surface.canvas',
  color: 'text.default',
});

const topBarClassName = flex({
  align: 'center',
  gap: '3',
  width: 'full',
  // The design system's top-bar slot right-aligns its children; this row reads
  // left to right from the control that governs the column beside it.
  justify: 'flex-start',
});

const brandClassName = css({
  fontSize: 'md',
  fontWeight: 'semibold',
  color: 'text.strong',
  letterSpacing: 'tight',
});

const toggleClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  appearance: 'none',
  height: '9',
  width: '9',
  p: '0',
  flexShrink: 0,
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  borderRadius: 'md',
  background: 'surface.default',
  color: 'text.muted',
  cursor: 'pointer',
  transitionProperty: 'colors',
  transitionDuration: 'fast',
  _hover: { color: 'text.strong', borderColor: 'border.default' },
  _focusVisible: {
    outlineWidth: '2px',
    outlineStyle: 'solid',
    outlineColor: 'interactive.accent',
    outlineOffset: '[2px]',
  },
});

const sidebarClassName = css({
  display: 'flex',
  flexDirection: 'column',
  height: 'full',
  minHeight: '0',
  bg: 'surface.default',
});

const navScrollClassName = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '0.5',
  flex: '1',
  minHeight: '0',
  overflowY: 'auto',
  px: '3',
  pt: '3',
  pb: '4',
});

const navHeadingClassName = css({
  fontSize: '2xs',
  fontWeight: 'semibold',
  textTransform: 'uppercase',
  letterSpacing: 'wider',
  color: 'text.muted',
  px: '3',
  mt: '4',
  mb: '1.5',
  _first: { mt: '1' },
});

/**
 * The selected state carries weight as well as fill.
 *
 * Fill alone was the whole signal before, and at this density a subtle tint is
 * easy to lose — so selection also takes the strong text colour, a medium
 * weight, and an accent rule down the leading edge. The rule is what survives
 * both themes; the tint is what reads at a glance in either.
 */
const navLinkClassName = css({
  position: 'relative',
  display: 'block',
  // Indented past the section heading's own gutter, so the items read as
  // belonging to the heading above them rather than as a flat list beside it.
  paddingInlineStart: '5',
  paddingInlineEnd: '3',
  py: '2',
  borderRadius: 'md',
  fontSize: 'sm',
  lineHeight: 'snug',
  color: 'text.muted',
  textDecoration: 'none',
  transitionProperty: 'colors',
  transitionDuration: 'fast',
  _hover: { bg: 'interactive.hover', color: 'text.default' },
  _focusVisible: {
    outlineWidth: '2px',
    outlineStyle: 'solid',
    outlineColor: 'interactive.accent',
    outlineOffset: '[-2px]',
  },
  '&[data-selected]': {
    bg: 'interactive.selected',
    color: 'text.strong',
    fontWeight: 'medium',
  },
  '&[data-selected]::before': {
    content: '""',
    position: 'absolute',
    insetBlock: '1.5',
    insetInlineStart: '0',
    width: '[2px]',
    borderRadius: 'full',
    bg: 'interactive.accent',
  },
});

const sidebarFooterClassName = flex({
  align: 'center',
  justify: 'space-between',
  gap: '3',
  flexWrap: 'wrap',
  px: '4',
  py: '3',
  borderTopWidth: '1px',
  borderTopStyle: 'solid',
  borderColor: 'border.subtle',
  bg: 'surface.default',
});

const offlineClassName = flex({
  align: 'center',
  gap: '2',
  fontFamily: 'mono',
  fontSize: '2xs',
  letterSpacing: 'wide',
  color: 'text.muted',
  whiteSpace: 'nowrap',
});

const offlineDotClassName = css({
  width: '1.5',
  height: '1.5',
  borderRadius: 'full',
  bg: 'text.success',
  flexShrink: 0,
});
