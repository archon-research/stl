export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

export function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown request failure.';
}
