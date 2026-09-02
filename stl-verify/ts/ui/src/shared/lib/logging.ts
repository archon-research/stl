/**
 * Centralized logging module.
 *
 * Use this module instead of direct console calls to ensure consistent logging
 * behavior across the application. In the future, this can be extended to send
 * logs to external services (Sentry, LogRocket, etc.) without changing call sites.
 */

export const logging = {
  error: (message: string, context?: unknown): void => {
    // eslint-disable-next-line no-console -- logging module is allowed to use console
    console.error(message, context);
  },

  warn: (message: string, context?: unknown): void => {
    // eslint-disable-next-line no-console -- logging module is allowed to use console
    console.warn(message, context);
  },

  info: (message: string, context?: unknown): void => {
    // eslint-disable-next-line no-console -- logging module is allowed to use console
    console.info(message, context);
  },

  debug: (message: string, context?: unknown): void => {
    // eslint-disable-next-line no-console -- logging module is allowed to use console
    console.debug(message, context);
  },
} as const;
