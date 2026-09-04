import fs from 'node:fs';
import path from 'node:path';

import babel from '@rolldown/plugin-babel';
import react, { reactCompilerPreset } from '@vitejs/plugin-react';
import type { Plugin } from 'vite';
import { defineConfig } from 'vitest/config';

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

/**
 * The React Compiler, as a rolldown babel preset.
 *
 * React is 19.2, so no `target` is needed: the compiler emits calls into
 * `react/compiler-runtime`, which that version ships. The preset ships only a
 * `code` filter, and Babel is the one thing in this pipeline that is not Oxc,
 * so an id filter is added for the two trees that would cost the most for
 * nothing -- the generated OpenAPI types and Panda's generated styled-system.
 */
function reactCompiler() {
  const preset = reactCompilerPreset();
  preset.rolldown.filter = {
    ...preset.rolldown.filter,
    id: { exclude: ['**/src/generated/**', '**/styled-system/**'] },
  };

  return babel({ presets: [preset] });
}

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
    plugins: [
      react(),
      reactCompiler(),
      ...(mocked ? [] : [dropMockWorkerScript()]),
    ],
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
      // Reached only from the excluded design system, so the dev server serves
      // `@tanstack/react-store` raw -- and its CJS `use-sync-external-store`
      // shim has no named exports, which fails the whole entry graph. Naming
      // the shim prebundles it without collapsing the two `react-store` copies
      // in the tree onto one, which is what `include`ing react-store itself did.
      include: ['use-sync-external-store/shim/with-selector'],
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
      rolldownOptions: {
        output: {
          // Vite 8 bundles with rolldown: `manualChunks` survives only as a
          // deprecated Rollup shim and `advancedChunks` is ignored whenever
          // this is present. Groups only shape chunks that something already
          // splits -- the dynamic imports in `MetricsBand`, `AllocationDrawer`,
          // `MethodologyPanel` and the routes are what create the boundaries;
          // these decide which vendor code lands on which side of them.
          codeSplitting: {
            // Below this a group is dropped and its modules fall back to
            // automatic chunking, so a near-empty group cannot cost a request.
            minSize: 20_000,
            groups: [
              // Reached only from the collapsed methodology panel.
              {
                name: 'markdown',
                priority: 30,
                entriesAware: true,
                test: /node_modules\/(react-markdown|micromark|mdast-|hast-|unified|vfile|property-information|character-entities|decode-named|stringify-entities|trough|bail|zwitch|longest-streak|ccount|devlop|trim-lines|is-plain-obj|extend|@ungap\/structured-clone|inline-style-parser|space-separated|comma-separated|html-url-attributes)/,
              },
              // Reached only from the metrics band.
              {
                name: 'charts',
                priority: 30,
                entriesAware: true,
                test: /node_modules\/(@archon-research\/charting|@visx|d3-|internmap|delaunator|robust-predicates|@react-spring|react-spring|reduce-css-calc|math-expression-evaluator)/,
              },
              {
                name: 'react',
                priority: 20,
                test: /node_modules\/(react|react-dom|scheduler)\//,
              },
              {
                name: 'router',
                priority: 20,
                test: /node_modules\/@tanstack\/[^/]*(router|history|store)/,
              },
              // Everything the design system and its Ark/Zag primitives bring.
              // `entriesAware` matters here: merged flat, the modules only the
              // drawer and the band use would ride in the chunk the entry
              // loads, which is the whole point of splitting them out.
              {
                name: 'design',
                priority: 10,
                entriesAware: true,
                test: /node_modules\/(@archon-research\/|@ark-ui|@zag-js|@floating-ui|@internationalized|@tanstack\/[^/]*(table|virtual))/,
              },
            ],
          },
        },
      },
    },
    // Vitest reuses this config, so `#src/*`, `#styled-system/*` and
    // preserveSymlinks resolve in tests exactly as they do in the app.
    test: {
      // `scripts/` holds the vite-booting regression CLIs, which run themselves
      // and would boot a second server inside a worker if collected here.
      include: ['src/**/*.test.{ts,tsx}'],
      // Every formatter here is locale- and zone-sensitive, so an unpinned
      // runner would assert one thing locally and another in CI.
      env: { TZ: 'UTC' },
    },
  };
});
