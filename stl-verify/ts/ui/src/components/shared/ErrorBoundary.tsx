import React, { Component, type ReactNode } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { logging } from '../../lib/logging';

type ErrorBoundaryProps = {
  children: ReactNode;
  fallback?: (error: Error, resetError: () => void) => ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
  error: Error | null;
};

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    logging.error('React error boundary caught rendering error', {
      error,
      componentStack: errorInfo.componentStack,
      errorBoundary: true,
    });
  }

  resetError = (): void => {
    this.setState({ hasError: false, error: null });
  };

  render(): ReactNode {
    if (this.state.hasError && this.state.error) {
      if (this.props.fallback) {
        return this.props.fallback(this.state.error, this.resetError);
      }

      return (
        <div
          className={flex({
            align: 'center',
            justify: 'center',
            minHeight: '100vh',
            bg: 'surface.subtle',
            p: '6',
          })}
        >
          <div
            className={css({
              maxWidth: '32rem',
              borderRadius: 'lg',
              borderStyle: 'solid',
              borderWidth: '1px',
              borderColor: 'border.subtle',
              bg: 'surface.default',
              p: '6',
              boxShadow: '0 24px 80px rgba(15, 23, 42, 0.08)',
            })}
          >
            <h1
              className={css({
                m: 0,
                fontSize: '2xl',
                fontWeight: 'semibold',
                color: 'text.strong',
              })}
            >
              Something went wrong
            </h1>
            <p
              className={css({
                m: 0,
                mt: '3',
                fontSize: 'sm',
                color: 'text.muted',
                lineHeight: '1.6',
              })}
            >
              The application encountered an unexpected error. This has been
              logged and will be investigated.
            </p>
            {this.state.error.message ? (
              <div
                className={css({
                  mt: '4',
                  borderRadius: 'md',
                  bg: { _dark: 'red.950', base: 'red.50' },
                  p: '3',
                })}
              >
                <p
                  className={css({
                    m: 0,
                    fontSize: 'xs',
                    fontFamily: 'mono',
                    color: { _dark: 'red.200', base: 'red.800' },
                    wordBreak: 'break-word',
                  })}
                >
                  {this.state.error.message}
                </p>
              </div>
            ) : null}
            <div
              className={flex({
                gap: '3',
                mt: '6',
              })}
            >
              <button
                type="button"
                onClick={() => window.location.reload()}
                className={css({
                  flex: 1,
                  borderRadius: 'md',
                  bg: 'interactive.accent',
                  px: '4',
                  py: '2.5',
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'white',
                  cursor: 'pointer',
                  border: 'none',
                  _hover: { opacity: 0.9 },
                })}
              >
                Reload page
              </button>
              <button
                type="button"
                onClick={this.resetError}
                className={css({
                  flex: 1,
                  borderRadius: 'md',
                  borderWidth: '1px',
                  borderStyle: 'solid',
                  borderColor: 'border.subtle',
                  bg: 'surface.default',
                  px: '4',
                  py: '2.5',
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                  cursor: 'pointer',
                  _hover: { bg: 'surface.subtle' },
                })}
              >
                Try again
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
