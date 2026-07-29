import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

import { resolveAppEnv } from './env';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = resolveAppEnv(mode, __dirname);

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
      // Vite's default. This matters most for allowedHosts: its default rejects
      // unrecognised Host headers (DNS-rebinding protection), which a blanket
      // `true` would disable for every plain `npm run dev`.
      ...(env.VITE_PORT !== undefined && { port: env.VITE_PORT }),
      ...(env.VITE_STRICT_PORT !== undefined && {
        strictPort: env.VITE_STRICT_PORT,
      }),
      ...(env.VITE_HOST !== undefined && { host: env.VITE_HOST }),
      ...(env.VITE_ALLOWED_HOSTS?.length && {
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
