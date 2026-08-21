/** Browser entry: `@stl-verify/mocks/browser`. */
import { setupMockWorker } from '@archon-research/http-client-msw/browser';
import type { MockWorker } from '@archon-research/http-client-msw/browser';

import { mocks } from './index.ts';

/**
 * Starts the worker and resolves once it is intercepting.
 *
 * `appBaseUrl` is the app's public base path (`import.meta.env.BASE_URL`), not
 * the API base: a service worker's scope is the directory it is served from, so
 * a subpath deployment has to load `mockServiceWorker.js` from under that
 * subpath.
 *
 * `window.resetMocks` is how a Playwright test rewinds mock state between cases
 * without a full reload.
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

  Object.assign(window, { resetMocks: () => worker.reset() });
  await worker.start();

  return worker;
}
