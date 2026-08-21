import { ErrorBoundary, ThemeProvider } from '@archon-research/design-system';
import { HttpProvider } from '@archon-research/http-client-react';
import { RouterProvider } from '@tanstack/react-router';
import { createRoot } from 'react-dom/client';

import { logging } from './lib/logging';
import { router } from './router/routes';

// Required global stylesheet side effects.
// oxlint-disable-next-line import/no-unassigned-import
import './index.css';

// Awaited before render so no component fires a request the worker is not yet
// intercepting; the dynamic import keeps msw out of a build with the flag unset.
if (import.meta.env.VITE_API_MOCKS === '1') {
  const { startMockWorker } = await import('@stl-verify/mocks/browser');
  try {
    await startMockWorker(import.meta.env.BASE_URL);
  } catch (error) {
    // A rejected start leaves this module unevaluated, so createRoot below never
    // runs and the ErrorBoundary never mounts: without this the only symptom is
    // a blank page. Rethrown because rendering against a backend that was never
    // meant to be there is the worse outcome.
    logging.error('Mock service worker failed to start', { error });
    document.getElementById('root')!.textContent =
      `VITE_API_MOCKS=1, but the mock service worker could not start: ${
        error instanceof Error ? error.message : String(error)
      }. Check that ui/public/mockServiceWorker.js exists (npx msw init public/ --save) and that this origin allows service workers.`;
    throw error;
  }
}

createRoot(document.getElementById('root')!).render(
  <ErrorBoundary
    onError={(error, errorInfo) => {
      logging.error('React error boundary caught rendering error', {
        error,
        componentStack: errorInfo.componentStack,
        errorBoundary: true,
      });
    }}
  >
    <ThemeProvider>
      <HttpProvider>
        <RouterProvider router={router} />
      </HttpProvider>
    </ThemeProvider>
  </ErrorBoundary>,
);
