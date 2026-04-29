import { logging } from './logging';

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

export function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    logging.error('Error details', {
      message: error.message,
      stack: error.stack,
      name: error.name,
      ...error,
    });
    return error.message;
  }

  logging.error('Non-Error object thrown', { error });
  return 'Unknown request failure.';
}
