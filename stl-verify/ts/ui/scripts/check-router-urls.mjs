import assert from 'node:assert/strict';

import {
  createMemoryHistory,
  createRouter,
  parseSearchWith,
  stringifySearchWith,
} from '@tanstack/react-router';
import { createServer } from 'vite';

/**
 * Resolves one entry URL the way a cold page load does: match the route tree,
 * run each matched route's `beforeLoad`, and report either the redirect target
 * or the matched leaf.
 *
 * `router.load()` is not usable here — it commits matches through the React
 * transitioner, so headless it leaves `state.matches` empty.
 */
function resolveEntryUrl(routeTree, url) {
  const router = createRouter({
    routeTree,
    trailingSlash: 'never',
    isServer: false,
    parseSearch: parseSearchWith((value) => value),
    stringifySearch: stringifySearchWith(JSON.stringify),
    history: createMemoryHistory({ initialEntries: [url] }),
  });

  const matches = router.matchRoutes(router.latestLocation);

  for (const match of matches) {
    const { beforeLoad } = router.routesById[match.routeId].options;
    if (!beforeLoad) {
      continue;
    }

    try {
      beforeLoad({ search: match.search, params: match.params });
    } catch (thrown) {
      if (!thrown?.options) {
        throw thrown;
      }
      return { redirectTo: router.buildLocation(thrown.options).href };
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
    const { routeTree } = await vite.ssrLoadModule('/src/router/routes.ts');
    const resolve = (url) => resolveEntryUrl(routeTree, url);

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
    assert.equal(resolve('/activities?prime=0xAAA').search.prime, '0xAAA');

    // Entry points with no prime land on the allocation view, which resolves the
    // default prime once the prime list arrives.
    assert.equal(resolve('/').redirectTo, '/allocation');
    assert.equal(resolve('/unknown/deep/path').redirectTo, '/allocation');
    assert.equal(resolve('/allocation').routeId, '/allocation/');

    // A trailing slash resolves to the same route.
    assert.equal(resolve('/activities/').routeId, '/activities');

    assert.equal(resolve('/allocation/0xAAA').params.primeId, '0xAAA');

    // A hand-edited URL degrades to "absent" rather than failing the route.
    const bogus = resolve(
      '/allocation/0xAAA?range=bogus&tab=bogus&category=bogus&network=1',
    ).search;
    assert.equal(bogus.range, undefined);
    assert.equal(bogus.tab, undefined);
    assert.equal(bogus.category, undefined);
    assert.equal(bogus.network, '1');

    // A custom range survives only as bounds that parse in the right order.
    assert.equal(
      resolve('/allocation/0xAAA?range=custom&from=nope&to=nope').search.from,
      undefined,
    );
    assert.equal(
      resolve('/allocation/0xAAA?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z')
        .search.from,
      undefined,
    );
    assert.equal(
      resolve('/allocation/0xAAA?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z')
        .search.to,
      '2026-02-01T00:00:00Z',
    );

    // The table params belong to the allocation route, so no other route
    // validates them — the structural fix for the shared `sort`/`q` namespace.
    assert.equal(
      resolve('/allocation/0xAAA?sort=symbol:desc').search.sort,
      'symbol:desc',
    );
    assert.equal(resolve('/activities?sort=symbol:desc').search.sort, undefined);
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
