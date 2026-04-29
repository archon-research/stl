import {
  useEffect,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent,
  type ReactNode,
} from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

type RiskDetailDrawerProps = {
  children: ReactNode;
  detail?: string;
  isOpen: boolean;
  onClose: () => void;
  subtitle?: string;
  title?: string;
};

type DragState = {
  startPosition: number;
  startSize: number;
};

const DEFAULT_DRAWER_WIDTH = 704;
const MIN_DRAWER_WIDTH = 480;
const MAX_DRAWER_WIDTH = 1100;
const DRAWER_STORAGE_KEY = 'risk-detail-drawer-width';

function isBrowser(): boolean {
  return typeof window !== 'undefined';
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

  return clamp(parsed, MIN_DRAWER_WIDTH, MAX_DRAWER_WIDTH);
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
  const drawerWidthRef = useRef(drawerWidth);

  drawerWidthRef.current = drawerWidth;

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

    const handleMouseMove = (event: globalThis.MouseEvent) => {
      const delta = dragState.startPosition - event.clientX;
      const nextWidth = clamp(
        dragState.startSize + delta,
        MIN_DRAWER_WIDTH,
        MAX_DRAWER_WIDTH,
      );
      setDrawerWidth(nextWidth);
    };

    const handleMouseUp = () => {
      window.localStorage.setItem(DRAWER_STORAGE_KEY, String(drawerWidthRef.current));
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
    width: `min(${drawerWidth}px, 100vw)`,
  };

  const handleResizeStart = (event: MouseEvent<HTMLDivElement>) => {
    event.preventDefault();
    setDragState({
      startPosition: event.clientX,
      startSize: drawerWidth,
    });
  };

  return (
    <div
      aria-hidden={!isOpen}
      className={css({
        pointerEvents: isOpen ? 'auto' : 'none',
      })}
    >
      <div
        onClick={onClose}
        className={css({
          position: 'fixed',
          inset: 0,
          bg: 'rgba(15, 23, 42, 0.28)',
          opacity: isOpen ? 1 : 0,
          visibility: isOpen ? 'visible' : 'hidden',
          transitionDuration: 'normal',
          transitionProperty: 'opacity',
          zIndex: 30,
        })}
      />

      <aside
        aria-label={title}
        style={drawerStyle}
        className={css({
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          bg: 'surface.default',
          boxShadow: '-24px 0 80px rgba(15, 23, 42, 0.16)',
          transform: isOpen ? 'translateX(0)' : 'translateX(100%)',
          transitionDuration: 'normal',
          transitionProperty: 'transform',
          zIndex: 40,
          display: 'flex',
          flexDirection: 'column',
        })}
      >
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize risk detail drawer"
          onMouseDown={handleResizeStart}
          className={css({
            position: 'absolute',
            top: 0,
            left: 0,
            bottom: 0,
            width: '2',
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
        </div>

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
          <div className={flex({ align: 'center', justify: 'space-between', gap: '3' })}>
            <div>
              <p
                className={css({
                  m: 0,
                  fontSize: 'xs',
                  textTransform: 'uppercase',
                  letterSpacing: '0.14em',
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
                <p
                  className={css({
                    m: 0,
                    mt: '1',
                    fontSize: 'sm',
                    color: 'text.default',
                  })}
                >
                  {subtitle}
                </p>
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
              onClick={onClose}
              className={css({
                borderRadius: 'sm',
                borderWidth: '1px',
                borderStyle: 'solid',
                borderColor: 'border.subtle',
                bg: 'surface.default',
                px: '3',
                py: '1.5',
                fontSize: 'sm',
                color: 'text.strong',
                cursor: 'pointer',
              })}
            >
              Close
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