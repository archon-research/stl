/**
 * Handlers that make one endpoint fail, installed at runtime.
 *
 * Every fixture here succeeds, so an error state — a failed card, and whatever
 * gesture is meant to recover it — is unreachable offline. That makes recovery
 * behaviour the least-exercised part of the app while being the part a user
 * meets on a bad day.
 *
 * These are not in the handler array. A caller installs one through the
 * worker's or server's `use()`, and `reset()` drops it, so the offline app is
 * unaffected until something asks for a failure.
 */
import type { MockHandler } from '@archon-research/http-client-msw';

import { mock } from './mock-api.ts';
import { problemResponse, unavailable } from './problem.ts';

/**
 * The reads worth being able to fail: each backs a card or a series that has
 * its own error state, so failing one exercises a distinct recovery path.
 * Names rather than paths, since this is the surface a Playwright case or a
 * developer types.
 */
export const FAILABLE_READS = [
  'risk-capital',
  'prime-debt',
  'exposure',
] as const;

export type FailableRead = (typeof FAILABLE_READS)[number];

export function isFailableRead(value: unknown): value is FailableRead {
  return FAILABLE_READS.includes(value as FailableRead);
}

/**
 * A 503, which is what the real API answers when a downstream lookup fails —
 * and, unlike a 404, is a failure the app is expected to recover from rather
 * than render as an empty prime.
 */
export function failingHandler(
  read: FailableRead,
  detail = `${read} is unavailable`,
): MockHandler {
  const fail = () => problemResponse(unavailable(detail));

  // Exhaustive over `FailableRead` with no default: adding a name above without
  // a path here fails to compile, and each path is a literal so `mock.get`
  // still checks it against the document.
  switch (read) {
    case 'risk-capital':
      return mock.get('/v1/primes/{prime_id}/risk-capital', fail);
    case 'prime-debt':
      return mock.get('/v1/primes/{prime_id}/debt', fail);
    case 'exposure':
      return mock.get('/v1/primes/{prime_id}/exposure', fail);
  }
}
