import { createMockApi } from '@archon-research/http-client-msw';

import type { paths } from './schema.ts';

/**
 * The app builds its client on `VITE_API_BASE_URL ?? ''`, so the API is mounted
 * at the origin root and the handler paths carry no prefix. An empty base leaves
 * `createMockApi` with msw's `*` origin wildcard, which is what lets the one
 * handler array answer the browser's relative `/v1/...` and the absolute URL a
 * node test has to issue.
 */
const MOCK_API_BASE_URL = '';

/**
 * Any base URL works for a client pointed at these handlers, since they match on
 * any origin. This is the one the self-test and node suites use.
 */
export const MOCK_ORIGIN = 'http://stl-mocks.test';

export const mock = createMockApi<paths>({ baseUrl: MOCK_API_BASE_URL });

/**
 * Latency, so the app exercises the pending and skeleton states a real API
 * forces it through. `mockDelay` skips it when `NODE_ENV` or Vite's `MODE` is
 * `test`, which is why `test:mocks` sets it.
 */
export const LIST_DELAY_MS = 220;
export const SERIES_DELAY_MS = 340;
