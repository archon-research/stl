#!/usr/bin/env node
// Copies the design system's pre-paint theme bootstrap into `public/` before a
// dev start or a build, replacing the hand-written copy this repo used to keep.
//
// Why a copied file and not the inline form: index.html declares
// `script-src 'self'`, so `THEME_BOOTSTRAP_SCRIPT` cannot be inlined. The
// package ships the same code as a plain browser script for that case.
//
// Why a copy step and not a committed file: the script encodes ThemeProvider's
// storage contract, and a stale copy applies one theme before paint and another
// after mount — the flash it exists to remove. Copying on every build pins it to
// the installed version, so it cannot drift.
//
// The destination keeps the `theme-init.js` name index.html already loads.
//
// Chained into the `dev` and `build` scripts rather than run from `predev` /
// `prebuild`: `.npmrc` sets `ignore-scripts=true`, so npm fires no lifecycle
// hook here — a `prebuild` entry would look wired up and silently never run.

import { copyFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const source = fileURLToPath(
  import.meta.resolve('@archon-research/design-system/theme-bootstrap.js'),
);
const destination = join(
  dirname(dirname(fileURLToPath(import.meta.url))),
  'public',
  'theme-init.js',
);

copyFileSync(source, destination);
