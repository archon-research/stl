import { css, cx } from '#styled-system/css';
import { surfaceMessage } from '#styled-system/recipes';

type TabNotePanelProps = {
  message: string;
};

type TabErrorPanelProps = {
  title: string;
  message: string;
};

// The upstream `surfaceMessage` recipe already describes this surface exactly:
// a recessed `surface.subtle` fill inside a `border.subtle` frame at radius
// `md`, with a semibold `text.strong` title over a `text.muted` body. Applying
// the recipe rather than the shipped `SurfaceMessage` component is deliberate —
// that component styles itself with inline `style` objects, so a consumer
// `className` cannot reach it.
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

export function TabErrorPanel({ title, message }: TabErrorPanelProps) {
  return (
    <div
      // One step stronger than the recipe's hairline, so a failed tab reads as
      // louder than an empty one.
      className={cx(
        surfaceMessageStyles.root,
        css({ borderColor: 'border.default' }),
      )}
    >
      <p className={surfaceMessageStyles.title}>{title}</p>
      <p className={surfaceMessageStyles.body}>{message}</p>
    </div>
  );
}
