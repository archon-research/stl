import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { gzipSync } from 'node:zlib';

/**
 * Nothing else protects the code split. Rolldown folds a `codeSplitting` group
 * back into its importer the moment no dynamic import reaches it — silently,
 * with no error, no warning and no failing test — so the entry quietly
 * reabsorbs the chart or markdown stack it was split away from, and the only
 * symptom is a slower first load nobody measures.
 *
 * The assertions therefore run from most to least informative: what is on the
 * eager path, what is *inside* it, that the lazy groups still exist off it, and
 * only then bytes. The eager set — entry plus everything `index.html`
 * modulepreloads — is the unit, because an entry-only budget passes happily
 * while the critical path regresses into a sibling preloaded chunk.
 *
 * The byte budgets carry deliberate slack, roughly 40-60% over the measured
 * actuals. A budget that trips on ordinary feature work gets raised reflexively
 * until it means nothing, so these are sized to catch a regression that comes
 * close to doubling a payload, not one that grows it a percent. Catching the
 * silent folding is the job of the content and preload assertions, which do not
 * depend on a threshold at all.
 *
 * When one fails: a content, preload or chunk-count failure is a real defect —
 * find the import that stopped being dynamic. A byte failure is a judgement
 * call — confirm the growth is code someone meant to put on that path, then
 * raise the number here, in that commit, with the new actual in the message.
 */

/** Gzipped kB, against actuals of entry 29, eager 197, charts 43, markdown 35. */
const ENTRY_BUDGET_KB = 45;
const EAGER_BUDGET_KB = 280;

/**
 * Below this the split has structurally collapsed rather than drifted; routes
 * come and go, so it sits well under the 22 the current tree emits.
 */
const MIN_CHUNK_COUNT = 15;

/**
 * The libraries the split exists to keep off the critical path. Each survives
 * minification as a runtime string — a `@visx/xychart` console warning, the
 * `micromarkExtensions`/`mdastExtensions` unified data keys — and appears in no
 * other chunk, which the self-check below re-proves on every run.
 */
const HEAVY_LIBRARIES = /micromark|@visx|react-markdown|mdast/;

/** Prefixes, never whole names: a group's chunk is named for its importers. */
const LAZY_GROUPS = [
  { prefix: 'charts~', budgetKb: 65 },
  { prefix: 'markdown~', budgetKb: 55 },
];

const ENTRY_PREFIX = 'index-';
const distDir = path.resolve(import.meta.dirname, '..', 'dist');
const assetsDir = path.join(distDir, 'assets');

type EagerGraph = {
  entry: string;
  preloaded: string[];
};

function assetsMatching(html: string, pattern: RegExp): string[] {
  return [...html.matchAll(pattern)].flatMap((match) => match[1] ?? []);
}

function readEagerGraph(): EagerGraph {
  const html = readFileSync(path.join(distDir, 'index.html'), 'utf8');
  const entries = assetsMatching(
    html,
    /<script[^>]*\stype="module"[^>]*\ssrc="\/assets\/([^"]+)"/g,
  );
  const preloaded = assetsMatching(
    html,
    /<link[^>]*\srel="modulepreload"[^>]*\shref="\/assets\/([^"]+)"/g,
  );

  const [entry] = entries;
  if (entry === undefined || entries.length !== 1) {
    throw new Error(
      `dist/index.html must reference exactly one module entry under /assets, found ${entries.length}. ` +
        'Either the build changed how it emits the entry or this check is parsing stale markup.',
    );
  }

  return { entry, preloaded };
}

function gzippedKb(chunk: string): number {
  return gzipSync(readFileSync(path.join(assetsDir, chunk))).byteLength / 1000;
}

function totalGzippedKb(chunks: string[]): number {
  return chunks.reduce((total, chunk) => total + gzippedKb(chunk), 0);
}

function describe(chunks: string[]): string {
  return chunks
    .map((chunk) => `  ${gzippedKb(chunk).toFixed(1).padStart(6)} kB  ${chunk}`)
    .join('\n');
}

function assertUnderBudget(what: string, actualKb: number, budgetKb: number) {
  assert.ok(
    actualKb <= budgetKb,
    `${what} is ${actualKb.toFixed(1)} kB gzipped, over its ${budgetKb} kB budget.\n` +
      'Check what landed there before raising the number: this budget is loose on purpose, ' +
      'so tripping it means a large addition, not ordinary drift.',
  );
}

function assertHeavyLibrariesAreLazy(chunks: string[], eager: string[]) {
  const carriers = chunks.filter((chunk) =>
    HEAVY_LIBRARIES.test(readFileSync(path.join(assetsDir, chunk), 'utf8')),
  );
  assert.ok(
    carriers.length > 0,
    `no chunk anywhere matches ${String(HEAVY_LIBRARIES)}, so the assertion below can no longer fail. ` +
      'The bundler most likely stopped emitting the identifiers it keys on; re-pick the markers ' +
      'against a fresh dist/ rather than deleting the check.',
  );

  const offenders = carriers.filter((chunk) => eager.includes(chunk));
  assert.deepEqual(
    offenders,
    [],
    `these eagerly loaded chunks contain code matching ${String(HEAVY_LIBRARIES)}:\n${describe(offenders)}\n` +
      'A codeSplitting group in vite.config.ts folded back into the critical path. Rolldown does this ' +
      'silently when nothing dynamically imports the group, so look for an import of the charting or ' +
      'markdown stack that stopped being a dynamic import() — most likely in MetricsBand, ' +
      'AllocationDrawer, MethodologyPanel or a route module.',
  );
}

function assertLazyGroupsStayOffThePath(chunks: string[], eager: string[]) {
  for (const { prefix, budgetKb } of LAZY_GROUPS) {
    const group = chunks.filter((chunk) => chunk.startsWith(prefix));
    assert.ok(
      group.length > 0,
      `no chunk is named "${prefix}*", so that codeSplitting group produced nothing. ` +
        'Either its `test` pattern in vite.config.ts no longer matches the packages it names, or the ' +
        'group fell under `minSize` and its modules went back to automatic chunking.',
    );
    const eagerlyLoaded = group.filter((chunk) => eager.includes(chunk));
    assert.deepEqual(
      eagerlyLoaded,
      [],
      `part of the "${prefix}*" group is on the critical path, which is what splitting it out was for:\n${describe(eagerlyLoaded)}\n` +
        'Something outside the lazy boundary now imports that package statically, so rolldown put a slice ' +
        'of the group in front of first paint. Find the static import — an eagerly reached module ' +
        '(main.tsx, App, a shared/ helper, a non-lazy route) that pulled the package in — and move it ' +
        'back behind the dynamic import that gates the rest of the group.',
    );
    assertUnderBudget(`"${prefix}*"`, totalGzippedKb(group), budgetKb);
  }
}

function main() {
  const { entry, preloaded } = readEagerGraph();
  const eager = [entry, ...preloaded];
  const chunks = readdirSync(assetsDir).filter((file) => file.endsWith('.js'));

  assert.ok(
    entry.startsWith(ENTRY_PREFIX),
    `the entry chunk is "${entry}", expected a "${ENTRY_PREFIX}*" name; the rest of this check keys off that prefix.`,
  );
  assert.ok(
    chunks.length >= MIN_CHUNK_COUNT,
    `the build emitted ${chunks.length} JS chunks, fewer than the ${MIN_CHUNK_COUNT} a split bundle should have. ` +
      'This is the coarse backstop for the same failure as the assertions above: whole groups folding away.',
  );

  assertHeavyLibrariesAreLazy(chunks, eager);
  assertLazyGroupsStayOffThePath(chunks, eager);
  assertUnderBudget(
    'the eager set (entry + modulepreloads)',
    totalGzippedKb(eager),
    EAGER_BUDGET_KB,
  );
  assertUnderBudget(
    `the entry chunk ${entry}`,
    gzippedKb(entry),
    ENTRY_BUDGET_KB,
  );

  console.log(
    `ok   ${chunks.length} chunks, eager set ${totalGzippedKb(eager).toFixed(1)} kB gzipped over ${eager.length} requests:\n${describe(eager)}`,
  );
}

main();
