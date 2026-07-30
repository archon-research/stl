import { css, cx } from '#styled-system/css';
import { surfaceMessage } from '#styled-system/recipes';

type TabNotePanelProps = {
  message: string;
};

// Applied as a recipe rather than through the shipped `SurfaceMessage`
// component; see `shared/ErrorPanel` for why.
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
