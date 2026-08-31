import { X } from 'lucide-react';
import {
  useEffect,
  useState,
  type CSSProperties,
  type MouseEvent,
  type ReactNode,
} from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

type RiskDetailDrawerProps = {
  children: ReactNode;
  detail?: ReactNode;
  isOpen: boolean;
  onClose: () => void;
  subtitle?: ReactNode;
  title?: ReactNode;
};

type DragState = {
  startPosition: number;
  startSize: number;
};

const DEFAULT_DRAWER_WIDTH = 704;
const MIN_DRAWER_WIDTH = 480;
const DRAWER_STORAGE_KEY = 'risk-detail-drawer-width';

// The drawer hosts the full backing-collateral table, which is wider than any
// fixed pixel ceiling that also looks sensible on a laptop, so the cap is
// relative to the viewport. Leaving 8% keeps the grid edge visible behind it so
// the drawer still reads as an overlay rather than a page.
//
// Enforced in CSS as well as here: the stored width is a pixel preference, and
// only the CSS `min()` keeps the cap true when the viewport is resized after
// mount -- clamping in JS alone would leave a stale, too-wide value behind.
const MAX_DRAWER_VIEWPORT_FRACTION = 0.92;
const MAX_DRAWER_WIDTH_CSS = `${MAX_DRAWER_VIEWPORT_FRACTION * 100}vw`;

function isBrowser(): boolean {
  return typeof window !== 'undefined';
}

function maxDrawerWidth(): number {
  if (!isBrowser()) {
    return DEFAULT_DRAWER_WIDTH;
  }

  return Math.max(
    MIN_DRAWER_WIDTH,
    Math.round(window.innerWidth * MAX_DRAWER_VIEWPORT_FRACTION),
  );
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function readStoredWidth(): number {
  if (!isBrowser()) {
    return DEFAULT_DRAWER_WIDTH;
  }

  const stored = window.localStorage.getItem(DRAWER_STORAGE_KEY);
  if (!stored) {
    return DEFAULT_DRAWER_WIDTH;
  }

  const parsed = Number(stored);
  if (Number.isNaN(parsed)) {
    return DEFAULT_DRAWER_WIDTH;
  }

  return clamp(parsed, MIN_DRAWER_WIDTH, maxDrawerWidth());
}

export function RiskDetailDrawer({
  children,
  detail,
  isOpen,
  onClose,
  subtitle,
  title = 'Risk details',
}: RiskDetailDrawerProps) {
  const [drawerWidth, setDrawerWidth] = useState(readStoredWidth);
  const [dragState, setDragState] = useState<DragState | null>(null);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', handleKeyDown);

    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  useEffect(() => {
    if (!isBrowser() || dragState === null) {
      return;
    }

    // Where the drag has got to, held for the length of the gesture. The two
    // listeners below are registered once per drag, so a closure over
    // `drawerWidth` would freeze at the width the drag started from -- which is
    // what the ref this replaces existed to work around, at the cost of a write
    // during render that the compiler is free to skip.
    let draggedWidth = dragState.startSize;

    const handleMouseMove = (event: globalThis.MouseEvent) => {
      const delta = dragState.startPosition - event.clientX;
      draggedWidth = clamp(
        dragState.startSize + delta,
        MIN_DRAWER_WIDTH,
        maxDrawerWidth(),
      );
      setDrawerWidth(draggedWidth);
    };

    const handleMouseUp = () => {
      window.localStorage.setItem(DRAWER_STORAGE_KEY, String(draggedWidth));
      setDragState(null);
    };

    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'col-resize';

    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp, { once: true });

    return () => {
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [dragState]);

  const drawerStyle: CSSProperties = {
    width: `min(${drawerWidth}px, ${MAX_DRAWER_WIDTH_CSS})`,
  };

  const handleResizeStart = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setDragState({
      startPosition: event.clientX,
      // Seed the drag from the width actually painted, not from state. After the
      // viewport narrows, state still holds the wider stored preference, and a
      // drag anchored there could never reach the new cap -- the handle would be
      // inert until the window widened again. Clamping here rather than on
      // resize keeps the preference intact for when it does.
      startSize: clamp(drawerWidth, MIN_DRAWER_WIDTH, maxDrawerWidth()),
    });
  };

  const ariaTitle = typeof title === 'string' ? title : 'Risk details';

  return (
    // Drawer stays mounted when closed (tabs gate fetches on `isOpen`). `inert` (not
    // `aria-hidden`) removes from both tab order and accessibility tree. ~95% browser
    // support; older browsers skip it (drawer stays keyboard-reachable when closed).
    <div
      inert={!isOpen}
      className={css({
        pointerEvents: isOpen ? 'auto' : 'none',
      })}
    >
      <button
        type="button"
        aria-label="Close risk detail drawer"
        onClick={onClose}
        className={css({
          position: 'fixed',
          inset: 0,
          bg: 'overlay.backdrop',
          border: 'none',
          p: 0,
          opacity: isOpen ? 1 : 0,
          visibility: isOpen ? 'visible' : 'hidden',
          transitionDuration: 'normal',
          transitionProperty: 'opacity',
          zIndex: 30,
        })}
      />

      <aside
        aria-label={ariaTitle}
        style={drawerStyle}
        className={css({
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          bg: 'surface.default',
          boxShadow: '2xl',
          transform: isOpen ? 'translateX(0)' : 'translateX(100%)',
          transitionDuration: 'normal',
          transitionProperty: 'transform',
          zIndex: 40,
          display: 'flex',
          flexDirection: 'column',
        })}
      >
        <button
          type="button"
          aria-label="Resize risk detail drawer"
          onMouseDown={handleResizeStart}
          className={css({
            position: 'absolute',
            top: 0,
            left: 0,
            bottom: 0,
            width: '2',
            border: 'none',
            bg: 'transparent',
            p: 0,
            cursor: 'col-resize',
            zIndex: 2,
          })}
        >
          <div
            aria-hidden="true"
            className={css({
              position: 'absolute',
              top: 0,
              bottom: 0,
              left: 0,
              width: '1px',
              bg: 'border.subtle',
              opacity: 0.7,
            })}
          />
        </button>

        <div
          className={css({
            pl: { base: '5', md: '6' },
            pr: { base: '4', md: '5' },
            py: '4',
            borderBottomWidth: '1px',
            borderBottomStyle: 'solid',
            borderBottomColor: 'border.subtle',
          })}
        >
          <div
            className={flex({
              align: 'flex-start',
              justify: 'space-between',
              gap: '3',
            })}
          >
            <div>
              <p
                className={css({
                  m: 0,
                  fontSize: 'xs',
                  textTransform: 'uppercase',
                  letterSpacing: '0.1em',
                  color: 'text.muted',
                })}
              >
                Allocation detail
              </p>
              <h2
                className={css({
                  m: 0,
                  mt: '1',
                  fontSize: 'lg',
                  lineHeight: 'tight',
                  color: 'text.strong',
                })}
              >
                {title}
              </h2>
              {subtitle ? (
                <div
                  className={css({
                    m: 0,
                    mt: '1',
                    fontSize: 'sm',
                    color: 'text.default',
                  })}
                >
                  {subtitle}
                </div>
              ) : null}
              {detail ? (
                <p
                  className={css({
                    m: 0,
                    mt: '0.5',
                    fontSize: 'xs',
                    color: 'text.muted',
                  })}
                >
                  {detail}
                </p>
              ) : null}
            </div>

            <button
              type="button"
              aria-label="Close"
              onClick={onClose}
              className={css({
                flexShrink: 0,
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '9',
                height: '9',
                borderRadius: 'md',
                borderWidth: '1px',
                borderStyle: 'solid',
                borderColor: 'border.subtle',
                bg: 'surface.default',
                color: 'text.muted',
                cursor: 'pointer',
                transitionProperty: 'background-color, color, border-color',
                transitionDuration: 'fast',
                _hover: {
                  bg: 'surface.hover',
                  color: 'text.strong',
                },
              })}
            >
              <X className={css({ width: '4', height: '4' })} />
            </button>
          </div>
        </div>

        <div
          className={css({
            flex: 1,
            minHeight: 0,
            overflowY: 'auto',
          })}
        >
          {children}
        </div>
      </aside>
    </div>
  );
}
