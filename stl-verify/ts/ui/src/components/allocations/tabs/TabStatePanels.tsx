import {
  SurfaceMessageBody,
  SurfaceMessageRoot,
} from '@archon-research/design-system';

import { css } from '#styled-system/css';

type TabNotePanelProps = {
  message: string;
};

// Root plus body rather than the `SurfaceMessage` compound: that one takes a
// required `title`, and this panel is a bare note.
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
