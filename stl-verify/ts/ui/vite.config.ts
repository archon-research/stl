import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/v1': 'http://localhost:8000',
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
  },
});
