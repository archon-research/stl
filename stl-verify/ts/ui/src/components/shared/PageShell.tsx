import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type PageShellProps = {
  children: ReactNode;
};

// Shared page framing for the top-level views (allocations, activities): a
// muted full-height backdrop wrapping a single elevated, bordered content card.
// Keeping this in one place ensures both views share identical padding,
// radius, border and elevation.
export function PageShell({ children }: PageShellProps) {
  return (
    <div
      className={css({
        minHeight: '100%',
        bg: 'surface.subtle',
        px: { base: '4', md: '5' },
        py: { base: '4', md: '5' },
      })}
    >
      <section
        className={css({
          borderRadius: 'lg',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.default',
          bg: 'surface.default',
          p: { base: '4', md: '5' },
          boxShadow: '2xl',
        })}
      >
        {children}
      </section>
    </div>
  );
}
