import assert from 'node:assert/strict';

import { createMemoryHistory, createRouter } from '@tanstack/react-router';
import { createServer } from 'vite';

/**
 * Resolves one entry URL the way a cold page load does: match the route tree,
 * run each matched route's `beforeLoad`, and report either the redirect target
 * or the matched leaf.
 *
 * `router.load()` is not usable here — it commits matches through the React
 * transitioner, so headless it leaves `state.matches` empty.
 */
function resolveEntryUrl(routerOptions, url) {
  // Spread from the shipped router's own options so the parse/stringify and
  // trailing-slash behaviour under test is the one the app runs with.
  const router = createRouter({
    ...routerOptions,
    isServer: false,
    history: createMemoryHistory({ initialEntries: [url] }),
  });

  const matches = router.matchRoutes(router.latestLocation);

  for (const match of matches) {
    const { beforeLoad } = router.routesById[match.routeId].options;
    if (!beforeLoad) {
      continue;
    }

    try {
      beforeLoad({
        cause: 'enter',
        location: router.latestLocation,
        matches,
        params: match.params,
        search: match.search,
      });
    } catch (thrown) {
      if (!thrown?.options) {
        throw thrown;
      }
      return {
        redirectTo:
          thrown.options.href ?? router.buildLocation(thrown.options).href,
        replace: thrown.options.replace,
      };
    }
  }

  const leaf = matches[matches.length - 1];
  return {
    redirectTo: null,
    routeId: leaf.routeId,
    params: leaf.params,
    // `_strictSearch` is the route's own validated view; `search` also carries
    // whatever else the URL happened to hold.
    search: leaf._strictSearch,
  };
}

/**
 * Follows an entry URL through every redirect to the URL that finally renders,
 * asserting each hop replaces rather than pushes and that the chain terminates.
 *
 * Termination is the load-bearing part: the search cleanup rewrites the URL to
 * whatever validation applied, so a schema whose output failed to re-validate
 * to itself would rewrite the same URL forever.
 */
function settleEntryUrl(resolve, url, hopLimit = 4) {
  const visited = [url];
  let current = url;

  for (let hop = 0; hop <= hopLimit; hop += 1) {
    const result = resolve(current);

    if (result.redirectTo === null) {
      return { url: current, result };
    }

    assert.equal(
      result.replace,
      true,
      `"${current}" redirected without replace, putting a rejected URL in the back-history`,
    );
    current = result.redirectTo;
    visited.push(current);
  }

  throw new Error(`"${url}" never stopped redirecting: ${visited.join(' -> ')}`);
}

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
    // See check-prime-grouping.mjs: dependency discovery runs a native rolldown
    // pass that segfaults intermittently and has nothing to contribute here.
    optimizeDeps: { noDiscovery: true },
    server: { middlewareMode: true },
  });

  try {
    const { router } = await vite.ssrLoadModule('/src/router/routes.ts');
    const resolve = (url) => resolveEntryUrl(router.options, url);
    const settle = (url) => settleEntryUrl(resolve, url);
    // What the address bar and the page settle on, once every entry-time
    // redirect has run.
    const settledUrl = (url) => settle(url).url;
    const applied = (url) => settle(url).result.search;

    for (const url of ENTRY_URLS) {
      settle(url);
    }

    // Links shared before the prime moved into the path must keep working, with
    // every other param intact and unchanged in spelling.
    assert.equal(
      resolve('/allocation?prime=0xAAA&network=1&sort=symbol%3Adesc&q=usd')
        .redirectTo,
      '/allocation/0xAAA?network=1&sort=symbol%3Adesc&q=usd',
    );
    assert.equal(resolve('/?prime=0xAAA').redirectTo, '/allocation/0xAAA');
    assert.equal(
      resolve('/unknown/path?prime=0xAAA').redirectTo,
      '/allocation/0xAAA',
    );

    // Activities keeps selecting its prime through the query string.
    assert.equal(resolve('/activities?prime=0xAAA').redirectTo, null);
    assert.equal(applied('/activities?prime=0xAAA').prime, '0xAAA');

    // Entry points with no prime land on the allocation view, which resolves the
    // default prime once the prime list arrives, and keep their filters.
    assert.equal(resolve('/').redirectTo, '/allocation');
    assert.equal(resolve('/unknown/deep/path').redirectTo, '/allocation');
    assert.equal(resolve('/unknown?network=1').redirectTo, '/allocation?network=1');
    assert.equal(settledUrl('/allocation'), '/allocation');
    assert.equal(resolve('/allocation').routeId, '/allocation/');

    // A trailing slash resolves to the same route.
    assert.equal(resolve('/activities/').routeId, '/activities');

    assert.equal(resolve('/allocation/0xAAA').params.primeId, '0xAAA');

    // The prime lives in the path on this view, so a leftover `?prime=` naming a
    // different one is dropped and everything else is carried over.
    assert.equal(
      settledUrl('/allocation/0xAAA?prime=0xBBB&network=1&tab=risk'),
      '/allocation/0xAAA?network=1&tab=risk',
    );
    assert.equal(settledUrl('/allocation?prime='), '/allocation');

    // A hand-edited URL degrades to "absent" rather than failing the route, and
    // the address bar is rewritten to the state that was actually applied.
    assert.equal(
      settledUrl('/allocation/0xAAA?range=bogus&tab=bogus&category=bogus&network=1'),
      '/allocation/0xAAA?network=1',
    );
    assert.equal(applied('/allocation/0xAAA?range=7d').range, '7d');
    assert.equal(applied('/allocation/0xAAA?tab=risk').tab, 'risk');

    // `custom` is not a preset in the URL: usable bounds are what mark a custom
    // range, so the naked selection is dropped and a usable pair survives it.
    assert.equal(applied('/allocation/0xAAA?range=custom').range, undefined);
    const customRange = applied(
      '/allocation/0xAAA?range=custom&from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z',
    );
    assert.equal(customRange.range, undefined);
    assert.equal(customRange.from, '2026-01-01T00:00:00Z');
    assert.equal(customRange.to, '2026-02-01T00:00:00Z');

    // A custom range survives only as bounds that parse in the right order.
    assert.equal(
      settledUrl('/allocation/0xAAA?range=custom&from=nope&to=nope'),
      '/allocation/0xAAA',
    );
    assert.equal(
      settledUrl('/allocation/0xAAA?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z'),
      '/allocation/0xAAA',
    );

    // The drawer flag is a closed set of one, so only the spelling links use it.
    assert.equal(applied('/allocation/0xAAA?drawer=1').drawer, '1');
    assert.equal(settledUrl('/allocation/0xAAA?drawer=true'), '/allocation/0xAAA');

    // The table params belong to the allocation route, so no other route
    // validates them.
    assert.equal(applied('/allocation/0xAAA?sort=symbol:desc').sort, 'symbol:desc');
    assert.equal(settledUrl('/activities?q=usd&sort=symbol:desc'), '/activities');

    // Search text is never JSON-decoded, so a term that happens to be valid JSON
    // stays the text the user typed.
    assert.equal(applied('/allocation/0xAAA?q=null').q, 'null');
    assert.equal(applied('/allocation/0xAAA?q=%5B1%2C2%5D').q, '[1,2]');

    // Each view's action filter has its own key, so neither leaks into the other.
    assert.equal(applied('/allocation/0xAAA?daa=in').daa, 'in');
    assert.equal(settledUrl('/allocation/0xAAA?aa=in'), '/allocation/0xAAA');
    const activitiesSearch = applied('/activities?token=USDC&allp=0&aa=in');
    assert.equal(activitiesSearch.aa, 'in');
    assert.equal(activitiesSearch.token, 'USDC');
    assert.equal(activitiesSearch.allp, '0');
  } finally {
    await vite.close();
  }
}

// See check-prime-grouping.mjs: vite's dev server swallows unhandled rejections,
// so a failing assertion only fails this script if it is caught here.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
