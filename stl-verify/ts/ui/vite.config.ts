import fs from 'node:fs';
import path from 'node:path';

import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';
import type { Plugin } from 'vite';

import { resolveAppEnv } from './env';

/**
 * Vite copies all of `public/` into the bundle, so a production image would ship
 * the msw worker script it never registers. The path comes from the resolved
 * config so a `--outDir` override cannot silently no-op the removal, and the
 * outcome is asserted rather than attempted.
 */
function dropMockWorkerScript(): Plugin {
  let target = '';

  return {
    name: 'stl-drop-mock-worker-script',
    apply: 'build',
    configResolved(config) {
      target = path.resolve(config.root, config.build.outDir, WORKER_SCRIPT);
    },
    closeBundle() {
      fs.rmSync(target, { force: true });
      if (fs.existsSync(target)) {
        throw new Error(`${target} survived the build; it must not ship`);
      }
    },
  };
}

const WORKER_SCRIPT = 'mockServiceWorker.js';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = resolveAppEnv(mode, import.meta.dirname);
  const mocked = env.VITE_API_MOCKS === '1';
  // A port pinned for a proxy or tunnel is a hard requirement: Vite's default
  // is to walk to the next free port, which leaves the dev server up and
  // healthy at an address the proxy is not pointing at.
  const strictPort =
    env.VITE_STRICT_PORT ?? (env.VITE_PORT === undefined ? undefined : true);

  return {
    plugins: [react(), ...(mocked ? [] : [dropMockWorkerScript()])],
    // env.ts is the only validator of this flag, and it reads `.env.default`,
    // which Vite does not. Without this, setting VITE_API_MOCKS there drops the
    // /v1 proxy while leaving the browser worker off, so every call 404s.
    define: {
      'import.meta.env.VITE_API_MOCKS': JSON.stringify(
        env.VITE_API_MOCKS ?? '',
      ),
    },
    optimizeDeps: {
      // preserveSymlinks makes Vite treat a workspace link as a dependency and
      // prebundle it, so an edited fixture keeps serving its old body.
      exclude: ['@archon-research/design-system', '@stl-verify/mocks'],
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
      // Absent only under VITE_API_MOCKS=1, where the service worker answers
      // /v1 in the browser and a proxy would have nothing to forward to.
      ...(env.API_URL !== undefined && {
        proxy: { '/v1': env.API_URL },
      }),
    },
    build: {
      outDir: 'dist',
      sourcemap: false,
    },
  };
});
