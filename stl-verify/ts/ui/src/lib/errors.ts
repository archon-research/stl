import { isHttpRequestError } from '@archon-research/http-client-react';

import { logging } from './logging';

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

function toErrorMessage(error: unknown): string {
  if (isHttpRequestError(error)) {
    return `${error.method.toUpperCase()} ${error.path} failed (${error.status}): ${toErrorBody(error.body)}`;
  }

  if (error instanceof Error) {
    return error.message;
  }

  return 'Unknown request failure.';
}

/**
 * The message a failed query is shown under, or `null` while it has not failed.
 *
 * Deliberately silent: this runs during render, off `query.error`, rather than
 * once inside a `catch`, and a logging side effect there would fire on every
 * render — the query cache's own `onError` is what logs. The parsed body is
 * kept because a status alone rarely says which of an endpoint's preconditions
 * was the one that failed.
 */
export function toQueryErrorMessage(error: unknown): string | null {
  return error === null || error === undefined ? null : toErrorMessage(error);
}
