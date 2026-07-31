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

// Use this, not the design system's `ErrorState`, wherever a failure has to
// read as an error: `ErrorState` has no `tone`, so its neutral surface and
// `text.strong` title are set by inline styles a consumer class cannot
// override, and the red border/title/tint that marks a failure everywhere else
// in this app would be lost.
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
