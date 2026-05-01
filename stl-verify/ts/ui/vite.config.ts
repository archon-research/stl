import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

import { resolveAppEnv } from '#env';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = resolveAppEnv(mode, __dirname);

  return {
    plugins: [react()],
    resolve: {
      preserveSymlinks: true,
      dedupe: ['react', 'react-dom'],
    },
    server: {
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
