import { AlertTriangle } from 'lucide-react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

type ErrorStateProps = {
  title: string;
  description: string;
  errorMessage?: string;
  onRetry?: () => void;
};

export function ErrorState({
  title,
  description,
  errorMessage,
  onRetry,
}: ErrorStateProps) {
  return (
    <div
      className={css({
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.default',
        bg: 'surface.subtle',
        p: { base: '5', md: '6' },
        maxWidth: '40rem',
        mx: 'auto',
      })}
    >
      <div className={css({ display: 'grid', gap: '3' })}>
        <div
          className={css({
            display: 'inline-flex',
            width: 'fit-content',
            alignItems: 'center',
            justifyContent: 'center',
            borderRadius: 'full',
            bg: { _dark: 'red.950', base: 'red.50' },
            p: '2',
          })}
        >
          <AlertTriangle
            className={css({
              width: '5',
              height: '5',
              color: { _dark: 'red.400', base: 'red.600' },
            })}
          />
        </div>
        <div>
          <h3
            className={css({
              m: 0,
              fontSize: 'lg',
              fontWeight: 'semibold',
              color: 'text.strong',
            })}
          >
            {title}
          </h3>
          <p
            className={css({
              m: 0,
              mt: '2',
              fontSize: 'sm',
              color: 'text.muted',
              lineHeight: '1.6',
            })}
          >
            {description}
          </p>
        </div>
        {errorMessage ? (
          <div
            className={css({
              borderRadius: 'md',
              bg: { _dark: 'red.950/50', base: 'red.50' },
              p: '3',
            })}
          >
            <p
              className={css({
                m: 0,
                fontSize: 'xs',
                fontFamily: 'mono',
                color: { _dark: 'red.300', base: 'red.800' },
                wordBreak: 'break-word',
              })}
            >
              {errorMessage}
            </p>
          </div>
        ) : null}
        {onRetry ? (
          <div className={flex({ justify: 'flex-start', mt: '2' })}>
            <button
              type="button"
              onClick={onRetry}
              className={css({
                borderRadius: 'md',
                bg: 'interactive.accent',
                px: '4',
                py: '2',
                fontSize: 'sm',
                fontWeight: 'semibold',
                color: 'white',
                cursor: 'pointer',
                border: 'none',
                _hover: { opacity: 0.9 },
              })}
            >
              Try again
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}
