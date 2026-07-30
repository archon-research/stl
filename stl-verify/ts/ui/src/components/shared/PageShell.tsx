import {
  PageShell as DesignSystemPageShell,
  Panel,
} from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type PageShellProps = {
  children: ReactNode;
};

// Shared page framing for the top-level views (allocations, activities): a
// full-height backdrop wrapping a single elevated, bordered content card.
// Keeping this in one place ensures both views share identical padding,
// radius, border and elevation.
//
// The backdrop stays local because the design system ships no page-background
// primitive. Horizontal padding is dropped in favour of the upstream PageShell
// recipe's own, but `maxWidth="none"` defeats that recipe's ~1160px centring
// cap, which would otherwise narrow the allocations grid — we want its padding,
// not its width limit. The `panel` recipe sets no shadow, so the card's
// elevation is applied here, from the dark-aware `elevation` token.
export function PageShell({ children }: PageShellProps) {
  return (
    <div
      className={css({
        minHeight: '100%',
        bg: 'surface.canvas',
        py: { base: '4', md: '5' },
      })}
    >
      <DesignSystemPageShell maxWidth="none">
        <Panel
          surface="raised"
          className={css({ boxShadow: 'elevation', p: { base: '4', md: '5' } })}
        >
          {children}
        </Panel>
      </DesignSystemPageShell>
    </div>
  );
}
