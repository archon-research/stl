import { Link } from '@tanstack/react-router';
import { ChevronRight } from 'lucide-react';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

/**
 * The one page frame every screen uses.
 *
 * It exists because the screens had drifted: the list pages ran to the full
 * width of the content column while the detail and form pages capped
 * themselves at their own chosen max-widths, so moving between a table and one
 * of its rows shifted the whole page under the reader. One frame means one
 * measure, one gutter, and one header treatment — and a new screen inherits
 * them instead of choosing again.
 *
 * Breadcrumbs lead rather than trail. A back-link at the foot of a long detail
 * page is only reachable after scrolling past everything the reader came for,
 * and it says where they would go rather than where they are.
 */

type Crumb = {
  label: string;
  /** Omit on the final crumb — the page you are already on is not a link. */
  to?: string;
  params?: Record<string, string>;
};

export type PageFrameProps = {
  crumbs: readonly Crumb[];
  title: ReactNode;
  /** Sits under the title; one line on what the resource is for. */
  description?: ReactNode;
  /** Trailing controls on the title row — the primary action lives here. */
  actions?: ReactNode;
  /** Status chips and the like, beside the title. */
  meta?: ReactNode;
  children: ReactNode;
};

export function PageFrame({
  crumbs,
  title,
  description,
  actions,
  meta,
  children,
}: PageFrameProps) {
  return (
    <div className={pageClassName}>
      <Breadcrumbs crumbs={crumbs} />

      <div className={headerRowClassName}>
        <div className={titleBlockClassName}>
          <div className={titleLineClassName}>
            <h1 className={titleClassName}>{title}</h1>
            {meta}
          </div>
          {description !== undefined && (
            <p className={descriptionClassName}>{description}</p>
          )}
        </div>
        {actions !== undefined && (
          <div className={actionsClassName}>{actions}</div>
        )}
      </div>

      {children}
    </div>
  );
}

function Breadcrumbs({ crumbs }: { crumbs: readonly Crumb[] }) {
  return (
    <nav aria-label="Breadcrumb" className={breadcrumbNavClassName}>
      <ol className={breadcrumbListClassName}>
        {crumbs.map((crumb, index) => {
          const isLast = index === crumbs.length - 1;

          return (
            <li key={`${crumb.label}:${index}`} className={crumbItemClassName}>
              {crumb.to === undefined || isLast ? (
                <span
                  className={crumbCurrentClassName}
                  {...(isLast && { 'aria-current': 'page' })}
                >
                  {crumb.label}
                </span>
              ) : (
                <Link
                  to={crumb.to}
                  {...(crumb.params !== undefined && { params: crumb.params })}
                  className={crumbLinkClassName}
                >
                  {crumb.label}
                </Link>
              )}
              {!isLast && (
                <ChevronRight
                  size={13}
                  aria-hidden="true"
                  className={crumbSeparatorClassName}
                />
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}

/**
 * A bordered surface for a block of page content.
 *
 * The detail page used the design system's `Panel` while the list pages used a
 * bare bordered div, which is why the two read as different products. Both go
 * through this now.
 */
export function PageSection({
  title,
  description,
  children,
  bleed,
}: {
  title?: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  /** Content that supplies its own padding — a table, typically. */
  bleed?: boolean;
}) {
  return (
    <section className={sectionClassName}>
      {title !== undefined && (
        <header className={sectionHeaderClassName}>
          <h2 className={sectionTitleClassName}>{title}</h2>
          {description !== undefined && (
            <p className={sectionDescriptionClassName}>{description}</p>
          )}
        </header>
      )}
      <div className={bleed === true ? undefined : sectionBodyClassName}>
        {children}
      </div>
    </section>
  );
}

/**
 * The measure every screen shares.
 *
 * Capped but left-aligned, not centred. Centring a 7xl column inside a content
 * area that is wider than it opens a dead gutter between the sidebar and the
 * page, which reads as a broken layout rather than as a measure — and it moves
 * the whole page sideways when the sidebar is collapsed.
 */
const pageClassName = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '5',
  width: 'full',
  maxWidth: '7xl',
  paddingInline: { base: '5', md: '8' },
  paddingBlock: { base: '5', md: '7' },
  minWidth: '0',
});

const breadcrumbNavClassName = css({ minWidth: '0' });

const breadcrumbListClassName = flex({
  align: 'center',
  gap: '1',
  flexWrap: 'wrap',
  listStyle: 'none',
  margin: '0',
  padding: '0',
});

const crumbItemClassName = flex({ align: 'center', gap: '1' });

const crumbLinkClassName = css({
  fontSize: 'xs',
  color: 'text.muted',
  textDecoration: 'none',
  borderRadius: 'sm',
  _hover: { color: 'text.default', textDecoration: 'underline' },
  _focusVisible: {
    outlineWidth: '2px',
    outlineStyle: 'solid',
    outlineColor: 'interactive.accent',
    outlineOffset: '[2px]',
  },
});

const crumbCurrentClassName = css({
  fontSize: 'xs',
  color: 'text.default',
  fontWeight: 'medium',
});

const crumbSeparatorClassName = css({ color: 'border.strong', flexShrink: 0 });

const headerRowClassName = flex({
  align: 'flex-start',
  justify: 'space-between',
  gap: '4',
  flexWrap: 'wrap',
});

const titleBlockClassName = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '1',
  minWidth: '0',
});

const titleLineClassName = flex({
  align: 'center',
  gap: '3',
  flexWrap: 'wrap',
});

const titleClassName = css({
  fontSize: 'xl',
  fontWeight: 'semibold',
  letterSpacing: 'tight',
  color: 'text.strong',
  margin: '0',
  textWrap: 'balance',
});

const descriptionClassName = css({
  fontSize: 'sm',
  color: 'text.muted',
  margin: '0',
  maxWidth: '4xl',
});

const actionsClassName = flex({ align: 'center', gap: '2', flexShrink: 0 });

const sectionClassName = css({
  display: 'flex',
  flexDirection: 'column',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  borderRadius: 'lg',
  bg: 'surface.default',
  overflow: 'hidden',
  minWidth: '0',
});

const sectionHeaderClassName = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '0.5',
  px: '4',
  py: '3',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
  bg: 'surface.subtle',
});

const sectionTitleClassName = css({
  fontSize: '2xs',
  fontWeight: 'semibold',
  textTransform: 'uppercase',
  letterSpacing: 'wider',
  color: 'text.muted',
  margin: '0',
});

const sectionDescriptionClassName = css({
  fontSize: 'xs',
  color: 'text.muted',
  margin: '0',
});

const sectionBodyClassName = css({ p: '4', minWidth: '0' });
