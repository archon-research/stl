import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

import { resolveAppEnv } from './env';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = resolveAppEnv(mode, __dirname);
  // A port pinned for a proxy or tunnel is a hard requirement: Vite's default
  // is to walk to the next free port, which leaves the dev server up and
  // healthy at an address the proxy is not pointing at.
  const strictPort =
    env.VITE_STRICT_PORT ?? (env.VITE_PORT === undefined ? undefined : true);

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
      // Apply only what the environment actually set, so anything absent keeps
      // Vite's default rather than a restatement of it here. env.ts documents
      // what each value may be and why.
      ...(env.VITE_PORT !== undefined && { port: env.VITE_PORT }),
      ...(strictPort !== undefined && { strictPort }),
      ...(env.VITE_HOST !== undefined && { host: env.VITE_HOST }),
      ...(env.VITE_ALLOWED_HOSTS !== undefined && {
        allowedHosts: env.VITE_ALLOWED_HOSTS,
      }),
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
