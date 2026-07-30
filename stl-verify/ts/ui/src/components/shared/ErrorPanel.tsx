import { css, cx } from '#styled-system/css';
import { surfaceMessage } from '#styled-system/recipes';

type ErrorPanelProps = {
  title: string;
  message: string;
  errorMessage?: string;
};

// Applying the recipe rather than the shipped `SurfaceMessage` component is
// deliberate — that component styles itself with inline `style` objects, so a
// consumer `className` cannot reach it.
const surfaceMessageStyles = surfaceMessage({ tone: 'critical' });

// Use this, not the design system's `ErrorState`, for a failure inside a panel,
// a drawer or the prime rail. `ErrorState` is a page-level state: a centred
// 44px icon over an 18px title, and an `errorMessage` block fixed to
// `whiteSpace: nowrap` by an inline style no consumer prop can reach — so in a
// ~250px rail a URL-and-status string becomes a horizontal scroll strip. The
// `className`/`style` escape hatches reach only its root.
export function ErrorPanel({ title, message, errorMessage }: ErrorPanelProps) {
  return (
    <div className={surfaceMessageStyles.root}>
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
