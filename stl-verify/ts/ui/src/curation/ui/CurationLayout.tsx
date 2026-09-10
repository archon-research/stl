import { Badge, ThemeToggle } from '@archon-research/design-system';
import { Link, Outlet } from '@tanstack/react-router';

import { css } from '#styled-system/css';

import { RESOURCES } from '../schema/registry.ts';

/**
 * The shell.
 *
 * The navigation is generated from the registry, so a new resource appears in it
 * without anyone editing this file — which is the point of the registry, and the
 * first thing that would rot if the nav were a hand-written list.
 */
export function CurationLayout() {
  return (
    <div className={shell}>
      <header className={header}>
        <div className={brand}>
          <span className={title}>Combined master</span>
          <Badge>curation spike</Badge>
        </div>
        <div
          className={css({ display: 'flex', gap: '3', alignItems: 'center' })}
        >
          <span className={offline}>offline · msw</span>
          <ThemeToggle />
        </div>
      </header>

      <div className={body}>
        <nav className={sidebar}>
          <span className={navHeading}>Curate</span>
          {RESOURCES.map((resource) => (
            <Link
              key={resource.key}
              to="/$resourceKey"
              params={{ resourceKey: resource.key }}
              className={navLink}
              activeProps={{ className: `${navLink} ${navLinkActive}` }}
            >
              {resource.label}
            </Link>
          ))}

          <span className={navHeading}>Workflows</span>
          <Link
            to="/workflow/classify"
            className={navLink}
            activeProps={{ className: `${navLink} ${navLinkActive}` }}
          >
            Classify a security
          </Link>
          <Link
            to="/worklist"
            className={navLink}
            activeProps={{ className: `${navLink} ${navLinkActive}` }}
          >
            Validity worklist
          </Link>
        </nav>

        <main className={main}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}

const shell = css({
  display: 'flex',
  flexDirection: 'column',
  minHeight: 'screen',
  bg: 'bg.canvas',
  color: 'text.default',
});

const header = css({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  paddingInline: '6',
  paddingBlock: '4',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
});

const brand = css({ display: 'flex', alignItems: 'center', gap: '3' });

const title = css({ fontSize: 'lg', fontWeight: 'semibold' });

const offline = css({
  fontSize: 'xs',
  color: 'text.muted',
  fontFamily: 'mono',
});

const body = css({
  display: 'grid',
  gridTemplateColumns: { base: '1fr', md: '16rem 1fr' },
  flex: '1',
  minHeight: '0',
});

const sidebar = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '1',
  padding: '4',
  borderRightWidth: '1px',
  borderRightStyle: 'solid',
  borderColor: 'border.subtle',
});

const navHeading = css({
  fontSize: '2xs',
  textTransform: 'uppercase',
  letterSpacing: 'wide',
  color: 'text.muted',
  marginTop: '4',
  marginBottom: '1',
});

const navLink = css({
  paddingInline: '3',
  paddingBlock: '2',
  borderRadius: 'sm',
  fontSize: 'sm',
  color: 'text.muted',
  textDecoration: 'none',
  _hover: { bg: 'surface.subtle', color: 'text.default' },
});

const navLinkActive = css({
  bg: 'surface.subtle',
  color: 'text.default',
  fontWeight: 'medium',
});

const main = css({ padding: '6', minWidth: '0' });
