import {
  SurfaceMessageBody,
  SurfaceMessageRoot,
} from '@archon-research/design-system';

import { css } from '#styled-system/css';

import { getChainLabel } from '../lib/dashboard';

type TabNotePanelProps = {
  message: string;
};

// Root plus body rather than the `SurfaceMessage` compound: that one takes a
// required `title`, and this panel is a bare note.

/**
 * Why a tab has nothing to show for a position on a chain STL has no id for.
 *
 * The empty states around it name a cause -- no matching filters, a direct
 * holding -- that is false here, and reads as the position being inert.
 */
export function unindexedChainMessage(
  network: string | null | undefined,
  subject: string,
): string {
  const chain =
    network === null || network === undefined || network.length === 0
      ? 'this chain'
      : getChainLabel(null, undefined, network);
  return `STL does not index ${chain} yet, so ${subject} is unavailable for this position.`;
}

export function TabNotePanel({ message }: TabNotePanelProps) {
  return (
    <SurfaceMessageRoot>
      {/* The `body` slot's `mt` exists to clear a preceding title; this panel
          has none, so the leading gap is removed. */}
      <SurfaceMessageBody className={css({ mt: 0 })}>
        {message}
      </SurfaceMessageBody>
    </SurfaceMessageRoot>
  );
}
