import { ErrorBoundary, ThemeProvider } from '@archon-research/design-system';
import { HttpProvider } from '@archon-research/http-client-react';
import { RouterProvider } from '@tanstack/react-router';
import { createRoot } from 'react-dom/client';

import { logging } from './lib/logging';
import { router } from './router/routes';

// Required global stylesheet side effects.
// oxlint-disable-next-line import/no-unassigned-import
import './index.css';

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
