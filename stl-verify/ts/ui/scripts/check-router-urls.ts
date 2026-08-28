import assert from 'node:assert/strict';

import type { EntryLeaf } from '@archon-research/router-kit/testing';
import {
  resolveEntryUrl,
  settleEntryUrl,
} from '@archon-research/router-kit/testing';
import { createServer } from 'vite';

/**
 * The validated search, as the router hands it back. `settleEntryUrl` reports it
 * as `unknown` because the shape belongs to the app, so it is restated here to
 * keep these assertions checked — a mistyped key is then a type error rather
 * than a silent `undefined === undefined` pass.
 *
 * Mirrors `AppSearchPatch` (shared + allocation + activities schemas) in
 * `src/shared/lib/search-params.ts`; importing that type would pull an app-project
 * file into this node project's program. Every value is post-validation, so
 * every one of them is a string or absent.
 */
type SettledSearch = Partial<
  Record<
    | 'prime'
    | 'network'
    | 'protocol'
    | 'range'
    | 'from'
    | 'to'
    | 'source'
    | 'reference'
    | 'category'
    | 'tab'
    | 'daa'
    | 'sort'
    | 'q'
    | 'drawer'
    | 'row'
    | 'token'
    | 'aa'
    | 'allp',
    string
  >
>;

// Every URL an entry-time assertion below covers, plus the shapes that only
// need to be shown to converge.
const ENTRY_URLS = [
  '/',
  '/?prime=0xAAA',
  '/?range=90D',
  '/unknown',
  '/unknown?network=1',
  '/unknown/deep/path',
  '/unknown/path?prime=0xAAA',
  '/allocation',
  '/allocation?prime=',
  '/allocation?prime=0xAAA&network=1&sort=symbol%3Adesc&q=usd',
  '/allocation/0xAAA',
  '/allocation/0xAAA?prime=0xBBB&network=1&tab=risk',
  '/allocation/0xAAA?range=bogus&tab=bogus&category=bogus&network=1',
  '/allocation/0xAAA?range=custom&from=nope&to=nope',
  '/allocation/0xAAA?aa=in',
  '/allocation/0xAAA?daa=in',
  '/allocation/0xAAA?drawer=true',
  '/allocation/0xAAA?sort=symbol:desc&network=1',
  '/allocation/0xAAA?q=null',
  '/activities',
  '/activities/',
  '/activities?prime=0xAAA',
  '/activities?q=usd&sort=symbol:desc',
  '/activities?token=USDC&allp=0&aa=in',
];

async function main() {
  const vite = await createServer({
    appType: 'custom',
    logLevel: 'error',
    // See check-prime-grouping.ts: dependency discovery runs a native rolldown
    // pass that segfaults intermittently and has nothing to contribute here.
    optimizeDeps: { noDiscovery: true },
    server: { middlewareMode: true },
  });

  try {
    const { router } = await vite.ssrLoadModule('/src/router/routes.ts');
    // The harness takes the shipped router's own options, so the parse/stringify
    // and trailing-slash behaviour under test is the one the app runs with.
    const resolve = (url: string) => resolveEntryUrl(router.options, url);
    const settle = (url: string) => settleEntryUrl(router.options, url);
    // What the address bar and the page settle on, once every entry-time
    // redirect has run.
    const settledUrl = async (url: string) => (await settle(url)).url;
    const applied = async (url: string) =>
      (await settle(url)).result.search as SettledSearch;
    // A resolution is a redirect or a leaf, and only a leaf carries params.
    const leaf = async (url: string): Promise<EntryLeaf> => {
      const resolution = await resolve(url);

      assert.equal(resolution.redirectTo, null, `${url} redirected`);

      return resolution as EntryLeaf;
    };

    for (const url of ENTRY_URLS) {
      await settle(url);
    }

    // Links shared before the prime moved into the path must keep working, with
    // every other param intact and unchanged in spelling.
    assert.equal(
      (
        await resolve(
          '/allocation?prime=0xAAA&network=1&sort=symbol%3Adesc&q=usd',
        )
      ).redirectTo,
      '/allocation/0xAAA?network=1&sort=symbol%3Adesc&q=usd',
    );
    assert.equal(
      (await resolve('/?prime=0xAAA')).redirectTo,
      '/allocation/0xAAA',
    );
    assert.equal(
      (await resolve('/unknown/path?prime=0xAAA')).redirectTo,
      '/allocation/0xAAA',
    );

    // Activities keeps selecting its prime through the query string.
    assert.equal((await resolve('/activities?prime=0xAAA')).redirectTo, null);
    assert.equal((await applied('/activities?prime=0xAAA')).prime, '0xAAA');

    // Entry points with no prime land on the allocation view, which resolves the
    // default prime once the prime list arrives, and keep their filters.
    assert.equal((await resolve('/')).redirectTo, '/allocation');
    assert.equal(
      (await resolve('/unknown/deep/path')).redirectTo,
      '/allocation',
    );
    assert.equal(
      (await resolve('/unknown?network=1')).redirectTo,
      '/allocation?network=1',
    );
    assert.equal(await settledUrl('/allocation'), '/allocation');
    assert.equal((await resolve('/allocation')).routeId, '/allocation/');

    // A trailing slash resolves to the same route.
    assert.equal((await resolve('/activities/')).routeId, '/activities');

    assert.equal((await leaf('/allocation/0xAAA')).params.primeId, '0xAAA');

    // The prime lives in the path on this view, so a leftover `?prime=` naming a
    // different one is dropped and everything else is carried over.
    assert.equal(
      await settledUrl('/allocation/0xAAA?prime=0xBBB&network=1&tab=risk'),
      '/allocation/0xAAA?network=1&tab=risk',
    );
    assert.equal(await settledUrl('/allocation?prime='), '/allocation');

    // A hand-edited URL degrades to "absent" rather than failing the route, and
    // the address bar is rewritten to the state that was actually applied.
    assert.equal(
      await settledUrl(
        '/allocation/0xAAA?range=bogus&tab=bogus&category=bogus&network=1',
      ),
      '/allocation/0xAAA?network=1',
    );
    assert.equal((await applied('/allocation/0xAAA?range=7d')).range, '7d');
    assert.equal((await applied('/allocation/0xAAA?tab=risk')).tab, 'risk');

    // `custom` is not a preset in the URL: usable bounds are what mark a custom
    // range, so the naked selection is dropped and a usable pair survives it.
    assert.equal(
      (await applied('/allocation/0xAAA?range=custom')).range,
      undefined,
    );
    const customRange = await applied(
      '/allocation/0xAAA?range=custom&from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z',
    );
    assert.equal(customRange.range, undefined);
    assert.equal(customRange.from, '2026-01-01T00:00:00Z');
    assert.equal(customRange.to, '2026-02-01T00:00:00Z');

    // A custom range survives only as bounds that parse in the right order.
    assert.equal(
      await settledUrl('/allocation/0xAAA?range=custom&from=nope&to=nope'),
      '/allocation/0xAAA',
    );
    assert.equal(
      await settledUrl(
        '/allocation/0xAAA?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z',
      ),
      '/allocation/0xAAA',
    );

    // The provenance survives validation and rides a prime switch. It is read
    // once at entry, so being stripped on arrival or on the first navigation
    // would revert the page to STL's own model unannounced.
    assert.equal(
      (await applied('/allocation/0xAAA?source=reference')).source,
      'reference',
    );
    assert.equal(
      (await applied('/allocation/0xAAA?source=indexed')).source,
      'indexed',
    );
    assert.equal(
      (await applied('/allocation/0xAAA?source=both')).source,
      'both',
    );
    // Already canonical, so it is left exactly as it arrived -- no redirect to
    // loop on.
    assert.equal(
      await settledUrl('/allocation/0xAAA?source=reference&network=1'),
      '/allocation/0xAAA?source=reference&network=1',
    );

    // A provenance the vocabulary does not know is dropped rather than carried,
    // so the address bar cannot claim a mode the page is not in.
    assert.equal(
      (await applied('/allocation/0xAAA?source=sky')).source,
      undefined,
    );
    assert.equal(
      await settledUrl('/allocation/0xAAA?source=sky'),
      '/allocation/0xAAA',
    );

    // The superseded spelling is translated on entry, not stripped: shared links
    // carry it, and dropping it would leave the URL disagreeing with the page
    // `shared/lib/provenance` had already built from it.
    assert.equal(
      (await applied('/allocation/0xAAA?reference=true')).source,
      'reference',
    );
    assert.equal(
      (await applied('/allocation/0xAAA?reference')).source,
      'reference',
    );
    assert.equal(
      (await applied('/allocation/0xAAA?reference=true')).reference,
      undefined,
    );
    assert.equal(
      await settledUrl('/allocation/0xAAA?reference=true'),
      '/allocation/0xAAA?source=reference',
    );

    // `reference=false` asked for STL's own figures by name, so it is `indexed`
    // rather than an absent param that would take whatever the default becomes.
    assert.equal(
      (await applied('/allocation/0xAAA?reference=false')).source,
      'indexed',
    );
    assert.equal(
      await settledUrl('/allocation/0xAAA?reference=false'),
      '/allocation/0xAAA?source=indexed',
    );

    // The current spelling wins over the superseded one rather than merging.
    assert.equal(
      (await applied('/allocation/0xAAA?source=indexed&reference=true')).source,
      'indexed',
    );

    // The drawer flag is a closed set of one, so only the spelling links use it.
    assert.equal((await applied('/allocation/0xAAA?drawer=1')).drawer, '1');
    assert.equal(
      await settledUrl('/allocation/0xAAA?drawer=true'),
      '/allocation/0xAAA',
    );

    // The table params belong to the allocation route, so no other route
    // validates them.
    assert.equal(
      (await applied('/allocation/0xAAA?sort=symbol:desc')).sort,
      'symbol:desc',
    );
    assert.equal(
      await settledUrl('/activities?q=usd&sort=symbol:desc'),
      '/activities',
    );

    // Search text is never JSON-decoded, so a term that happens to be valid JSON
    // stays the text the user typed.
    assert.equal((await applied('/allocation/0xAAA?q=null')).q, 'null');
    assert.equal((await applied('/allocation/0xAAA?q=%5B1%2C2%5D')).q, '[1,2]');

    // Each view's action filter has its own key, so neither leaks into the other.
    assert.equal((await applied('/allocation/0xAAA?daa=in')).daa, 'in');
    assert.equal(
      await settledUrl('/allocation/0xAAA?aa=in'),
      '/allocation/0xAAA',
    );
    const activitiesSearch = await applied(
      '/activities?token=USDC&allp=0&aa=in',
    );
    assert.equal(activitiesSearch.aa, 'in');
    assert.equal(activitiesSearch.token, 'USDC');
    assert.equal(activitiesSearch.allp, '0');
  } finally {
    await vite.close();
  }
}

// See check-prime-grouping.ts: vite's dev server swallows unhandled rejections,
// so a failing assertion only fails this script if it is caught here.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
