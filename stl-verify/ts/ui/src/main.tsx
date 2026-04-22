import { ThemeProvider } from '@archon-research/design-system';
import { HttpProvider } from '@archon-research/http-client-react';
import { createRoot } from 'react-dom/client';

import App from './App.tsx';

// Required global stylesheet side effects.
// oxlint-disable-next-line import/no-unassigned-import
import './index.css';

createRoot(document.getElementById('root')!).render(
  <ThemeProvider>
    <HttpProvider>
      <App />
    </HttpProvider>
  </ThemeProvider>,
);
