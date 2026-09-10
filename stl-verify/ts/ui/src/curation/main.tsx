import { ErrorBoundary, ThemeProvider } from '@archon-research/design-system';
import { HttpProvider } from '@archon-research/http-client-react';
import { QueryClient } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import { createRoot } from 'react-dom/client';

import { startCurationWorker } from './mocks/browser.ts';
import { router } from './routes/router.tsx';

// Required global stylesheet side effects.
// oxlint-disable-next-line import/no-unassigned-import
import '../index.css';

/**
 * The curation app's entry.
 *
 * A second entry rather than a route inside the existing app, for two reasons
 * that both point the same way. It writes, and the existing app does not — so
 * its cache, its error handling and its API contract are different animals. And
 * it is not meant to ship yet: Vite's default build input is the root
 * `index.html` alone, so `curation.html` is served in dev and absent from
 * `npm run build`, which is exactly the boundary the work needs while the
 * endpoints behind it are still being designed.
 *
 * The worker start is unconditional and awaited. There is no backend to fall
 * back to, and awaiting it before `createRoot` means no component fires a
 * request the worker is not yet intercepting.
 */
const rootElement = document.getElementById('root');
if (rootElement === null) {
  throw new Error('#root is missing from curation.html');
}

try {
  await startCurationWorker(import.meta.env.BASE_URL);
} catch (error) {
  // A rejected start leaves this module unevaluated, so `createRoot` never runs
  // and the ErrorBoundary never mounts — without this the only symptom is a
  // blank page.
  rootElement.textContent = `The mock service worker could not start: ${
    error instanceof Error ? error.message : String(error)
  }. Check that ui/public/mockServiceWorker.js exists and that this origin allows service workers.`;
  throw error;
}

/**
 * Writes make staleness a correctness question rather than a freshness one, so
 * this client is more conservative than the read-only app's: nothing is served
 * stale by default, and a mutation invalidates by tag.
 */
const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 0, retry: false },
    mutations: { retry: false },
  },
});

createRoot(rootElement).render(
  <ErrorBoundary>
    <ThemeProvider>
      <HttpProvider client={queryClient}>
        <RouterProvider router={router} />
      </HttpProvider>
    </ThemeProvider>
  </ErrorBoundary>,
);
