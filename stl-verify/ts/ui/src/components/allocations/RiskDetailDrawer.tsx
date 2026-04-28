import { useEffect, type ReactNode } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

type RiskDetailDrawerProps = {
  children: ReactNode;
  isOpen: boolean;
  onClose: () => void;
  title?: string;
};

export function RiskDetailDrawer({
  children,
  isOpen,
  onClose,
  title = 'Risk details',
}: RiskDetailDrawerProps) {
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
          transitionDuration: 'normal',
          transitionProperty: 'opacity',
          zIndex: 30,
        })}
      />

      <aside
        aria-label={title}
        className={css({
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          width: { base: '100vw', md: 'min(44rem, 82vw)' },
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
          className={css({
            px: { base: '4', md: '5' },
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