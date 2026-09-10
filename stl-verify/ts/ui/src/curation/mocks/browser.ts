import { setupMockWorker } from '@archon-research/http-client-msw/browser';
import type { MockWorker } from '@archon-research/http-client-msw/browser';

import { curationMocks } from './handlers.ts';

/**
 * Starts the curation app's mock worker and resolves once it is intercepting.
 *
 * The whole app runs on this. There is no backend to point at — `VITE_API_MOCKS`
 * is not a mode here, it is the only mode — so unlike the main app this entry is
 * unconditional and there is no proxy fallback to fall back to. That is
 * deliberate: the point of the spike is to have a working client before the
 * service exists, and a mock that can be bypassed invites building against a
 * shape nothing has agreed to yet.
 *
 * `window.resetMocks()` rewinds the store to its seed, which is how a Playwright
 * case gets a clean book between assertions — and, in manual use, how you undo a
 * session of experimental appends without a reload.
 */
export async function startCurationWorker(
  appBaseUrl: string,
): Promise<MockWorker> {
  const worker = setupMockWorker(curationMocks, {
    baseUrl: appBaseUrl,
    // msw's default is 'bypass', which sends an uncovered /v1 call to the Vite
    // dev server — whose SPA fallback answers 200 text/html, so the client fails
    // parsing JSON instead of reporting a handler it does not have.
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
