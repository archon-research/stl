/** Browser entry: `@stl-verify/mocks/browser`. */
import { setupMockWorker } from '@archon-research/http-client-msw/browser';
import type { MockWorker } from '@archon-research/http-client-msw/browser';

import type { FailableRead } from './failure.ts';
import { failingHandler, isFailableRead } from './failure.ts';
import { mocks } from './index.ts';

/** Survives the reload a redirect causes; cleared by `resetMocks`. */
const FAILURE_KEY = 'stl-mock-failures';

function readRequestedFailures(): FailableRead[] {
  return (sessionStorage.getItem(FAILURE_KEY) ?? '')
    .split(',')
    .filter(isFailableRead);
}

/**
 * Starts the worker and resolves once it is intercepting.
 *
 * `appBaseUrl` is the app's public base path (`import.meta.env.BASE_URL`), not
 * the API base: a service worker's scope is the directory it is served from, so
 * a subpath deployment has to load `mockServiceWorker.js` from under that
 * subpath.
 *
 * `window.resetMocks` is how a Playwright test rewinds mock state between cases
 * without a full reload; `window.failMock('risk-capital')` is how it reaches an
 * error state the fixtures never produce, and `resetMocks` drops that too.
 *
 * A requested failure is held in session storage and reinstalled here on every
 * start, because a runtime `use()` does not survive a document navigation — and
 * the app performs one on the provenance-fallback redirect, which would
 * silently heal the very state the case is testing.
 */
export async function startMockWorker(appBaseUrl: string): Promise<MockWorker> {
  const worker = setupMockWorker(mocks, {
    baseUrl: appBaseUrl,
    // msw's own default here is 'bypass', which under this flag sends an
    // uncovered /v1 call to the Vite dev server — whose SPA fallback answers
    // 200 text/html, so the client fails parsing JSON instead of reporting a
    // missing handler. App assets still pass through silently.
    onUnhandledRequest: (request, print) => {
      if (new URL(request.url).pathname.startsWith('/v1/')) {
        print.error();
      }
    },
  });

  const requestedFailures = readRequestedFailures();
  if (requestedFailures.length > 0) {
    worker.worker.use(...requestedFailures.map((read) => failingHandler(read)));
  }

  Object.assign(window, {
    resetMocks: () => {
      sessionStorage.removeItem(FAILURE_KEY);
      worker.reset();
    },
    failMock: (read: unknown) => {
      if (!isFailableRead(read)) {
        throw new Error(`not a failable read: ${String(read)}`);
      }
      sessionStorage.setItem(
        FAILURE_KEY,
        [...new Set([...readRequestedFailures(), read])].join(','),
      );
      worker.worker.use(failingHandler(read));
    },
  });
  await worker.start();

  return worker;
}
