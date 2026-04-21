import path from 'node:path';

import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const uikitWorktreePath = '/Users/rohit/Code/stl-ui';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    preserveSymlinks: true,
    dedupe: ['react', 'react-dom'],
  },
  server: {
    proxy: {
      '/v1': 'http://localhost:8000',
    },
    fs: {
      allow: [path.resolve(__dirname), uikitWorktreePath],
    },
  },
  optimizeDeps: {
    exclude: ['@archon-research/design-system'],
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
  },
});
