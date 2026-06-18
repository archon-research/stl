import { ErrorBoundary, ThemeProvider } from '@archon-research/design-system';
import { HttpProvider } from '@archon-research/http-client-react';
import { createRoot } from 'react-dom/client';

import App from './App.tsx';
import { logging } from './lib/logging';
import { setPathname } from './lib/url-params';

// Required global stylesheet side effects.
// oxlint-disable-next-line import/no-unassigned-import
import './index.css';

if (typeof window !== 'undefined' && window.location.pathname === '/') {
  setPathname('/allocation', 'replace');
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
        <App />
      </HttpProvider>
    </ThemeProvider>
  </ErrorBoundary>,
);
