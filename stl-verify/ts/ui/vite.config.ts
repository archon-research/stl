import react from '@vitejs/plugin-react';
import { defineConfig, loadEnv } from 'vite';

import { resolveAppEnv } from './env';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = resolveAppEnv(mode, __dirname);
  // Dev-server host/port/allowedHosts are injected by the local dev tooling
  // (nix-darwin-config projects/stl exports VITE_*); loadEnv reads them from the
  // process env. Defaults keep `npm run dev` working standalone.
  const dev = loadEnv(mode, __dirname, 'VITE_');

  return {
    plugins: [react()],
    optimizeDeps: {
      exclude: ['@archon-research/design-system'],
    },
    resolve: {
      preserveSymlinks: true,
      dedupe: ['react', 'react-dom'],
    },
    server: {
      port: dev.VITE_PORT ? parseInt(dev.VITE_PORT, 10) : 5173,
      strictPort: dev.VITE_STRICT_PORT === 'true',
      host:
        dev.VITE_HOST === 'true'
          ? true
          : dev.VITE_HOST === 'false'
            ? false
            : dev.VITE_HOST,
      allowedHosts: dev.VITE_ALLOWED_HOSTS
        ? dev.VITE_ALLOWED_HOSTS.split(',')
        : true,
      proxy: {
        '/v1': env.API_URL,
      },
    },
    build: {
      outDir: 'dist',
      sourcemap: false,
    },
  };
});
