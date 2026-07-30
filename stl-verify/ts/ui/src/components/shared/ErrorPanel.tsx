import { css, cx } from '#styled-system/css';
import { surfaceMessage } from '#styled-system/recipes';

type ErrorPanelProps = {
  title: string;
  message: string;
  errorMessage?: string;
};

// The upstream `surfaceMessage` recipe already describes this surface exactly:
// a recessed `surface.subtle` fill inside a `border.subtle` frame at radius
// `md`, with a semibold `text.strong` title over a `text.muted` body. Applying
// the recipe rather than the shipped `SurfaceMessage` component is deliberate —
// that component styles itself with inline `style` objects, so a consumer
// `className` cannot reach it.
const surfaceMessageStyles = surfaceMessage();

// Use this, not the design system's `ErrorState`, for a failure inside a panel,
// a drawer or the prime rail. `ErrorState` is a page-level state: it hardcodes
// `maxWidth: 840` + `marginInline: auto` in an inline style and takes no
// className, so inline it centres itself in its container and reads as a
// floating box — inside a ~250px rail it cannot fit at all. Reported upstream.
export function ErrorPanel({ title, message, errorMessage }: ErrorPanelProps) {
  return (
    <div
      // One step stronger than the recipe's `border.subtle`, so a failed panel
      // reads as louder than an empty one.
      className={cx(
        surfaceMessageStyles.root,
        css({ borderColor: 'border.default' }),
      )}
    >
      <p className={surfaceMessageStyles.title}>{title}</p>
      <p className={surfaceMessageStyles.body}>{message}</p>
      {errorMessage ? (
        <p
          // Wraps rather than scrolls: these strings carry a URL and a status
          // code, and the narrowest host is the prime rail.
          className={cx(
            surfaceMessageStyles.body,
            css({
              fontFamily: 'mono',
              fontSize: 'xs',
              overflowWrap: 'anywhere',
            }),
          )}
        >
          {errorMessage}
        </p>
      ) : null}
    </div>
  );
}
