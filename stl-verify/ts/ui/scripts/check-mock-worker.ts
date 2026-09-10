import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

/**
 * `public/mockServiceWorker.js` is a copy msw's CLI wrote, and nothing else
 * reads the version it was written for. An msw upgrade therefore leaves the
 * committed worker silently a version behind: the offline app registers it, the
 * worker and the library disagree about their message protocol, and every
 * request falls through to the network as though the mocks were simply not
 * installed. A byte comparison against the installed copy is the whole check.
 */
const WORKER_SCRIPT = 'mockServiceWorker.js';

const uiRoot = path.resolve(import.meta.dirname, '..');
const installed = path.join(
  path.dirname(createRequire(import.meta.url).resolve('msw/package.json')),
  'lib',
  WORKER_SCRIPT,
);
const committed = path.join(uiRoot, 'public', WORKER_SCRIPT);

assert.deepEqual(
  readFileSync(committed),
  readFileSync(installed),
  `${committed} does not match the worker shipped by the installed msw.\n` +
    `Run 'npx msw init' in stl-verify/ts/ui and commit the result.`,
);

console.log(`ok   ${WORKER_SCRIPT} matches the installed msw`);
