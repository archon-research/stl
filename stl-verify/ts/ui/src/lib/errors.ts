import { isHttpRequestError } from '@archon-research/http-client-react';

import { logging } from './logging';

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function toErrorBody(body: unknown): string {
  if (typeof body === 'string') {
    return body;
  }

  if (body instanceof Error) {
    return body.message;
  }

  if (body === undefined || body === null) {
    return 'No response body.';
  }

  try {
    return JSON.stringify(body);
  } catch (stringifyError) {
    logging.error('Failed to stringify error body', { body, stringifyError });
    return 'Unserializable error body.';
  }
}

/**
 * The message a failed request is shown under.
 *
 * Deliberately silent: this runs during render now that the message is derived
 * from a query's error rather than set once in a `catch`, and the query cache's
 * own `onError` is what logs. The parsed body is kept because the status alone
 * rarely says which of an endpoint's preconditions was the one that failed.
 */
export function toErrorMessage(error: unknown): string {
  if (isHttpRequestError(error)) {
    return `${error.method.toUpperCase()} ${error.path} failed (${error.status}): ${toErrorBody(error.body)}`;
  }

  if (error instanceof Error) {
    return error.message;
  }

  return 'Unknown request failure.';
}

/** The message for a failed query, or `null` while it has not failed. */
export function toQueryErrorMessage(error: unknown): string | null {
  return error === null || error === undefined ? null : toErrorMessage(error);
}
