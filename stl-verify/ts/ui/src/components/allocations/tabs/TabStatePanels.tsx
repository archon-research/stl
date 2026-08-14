import { css, cx } from '#styled-system/css';
import { surfaceMessage } from '#styled-system/recipes';

type TabNotePanelProps = {
  message: string;
};

// Applying the recipe rather than the shipped `SurfaceMessage` component is
// deliberate — that component styles itself with inline `style` objects, which
// win over anything a consumer `className` declares. Tracked as ORB-352.
const surfaceMessageStyles = surfaceMessage();

export function TabNotePanel({ message }: TabNotePanelProps) {
  return (
    <div className={surfaceMessageStyles.root}>
      {/* The `body` slot's `mt` exists to clear a preceding title; this panel
          has none, so the leading gap is removed. */}
      <p className={cx(surfaceMessageStyles.body, css({ mt: 0 }))}>{message}</p>
    </div>
  );
}
