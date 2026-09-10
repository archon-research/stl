import path from 'node:path';

import { defineConfig, mergeConfig, type UserConfig } from 'vite';

import baseConfigFn from './vite.config';

/**
 * A build config for the curation app alone.
 *
 * The default build does not produce this app at all, and that is the point:
 * Vite's `build.rollupOptions.input` defaults to the root `index.html`, so
 * `curation.html` is served by the dev server and absent from `npm run build`.
 * No plugin enforces that, no flag guards it — it falls out of Vite's own
 * default, which is the cheapest possible way to keep unfinished work out of the
 * shipping bundle.
 *
 * This config exists for the other direction: producing a standalone static
 * preview to hand someone, or a Playwright target. It writes to `dist-curation`
 * so it can never overwrite the real `dist`, and `scripts/check-bundle-budget.ts`
 * — which reads `dist` — keeps measuring only what ships.
 *
 * `mergeConfig` over the base rather than a fresh config: the react compiler,
 * the `optimizeDeps` exclusions and the `preserveSymlinks` resolution are all
 * load-bearing and all identical here, and a second copy of them would drift.
 */
export default defineConfig(async (env) => {
  const base = (await baseConfigFn(env)) as UserConfig;

  return mergeConfig(base, {
    build: {
      outDir: 'dist-curation',
      rollupOptions: {
        input: path.resolve(import.meta.dirname, 'curation.html'),
      },
    },
  } satisfies UserConfig);
});
