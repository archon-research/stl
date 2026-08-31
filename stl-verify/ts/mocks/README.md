# @stl-verify/mocks

Offline mocks for the stl-verify UI's API, built on
[`@archon-research/http-client-msw`](https://www.npmjs.com/package/@archon-research/http-client-msw).

One handler array, three consumers:

- `VITE_API_MOCKS=1 npm run dev -w ui` — the browser service worker, no backend
- `@stl-verify/mocks/node` — msw's node interceptors: `scripts/check-mock-api.ts`
  today, a vitest suite when one lands
- a Playwright run against a dev server started with the same flag, resetting
  between cases through `window.resetMocks()`

```bash
# From stl-verify/ts
VITE_API_MOCKS=1 npm run dev -w ui   # the app, offline
npm run test:mocks -w mocks          # the self-test
```

## Why the types come from the ui workspace

`src/schema.ts` imports the generated `paths` and `components` from
`@stl-verify/ui/openapi-types` — an `exports` subpath on the ui workspace
pointing at `src/generated/openapi-types.ts`, the same module
`ui/src/shared/lib/api-client.ts` builds `createApiClient` on.

That is the point of the layer: `createMockApi<paths>` reads the endpoint list,
the params, and the per-status response bodies off the contract, so a schema
regeneration that renames a field turns the stale fixture into a compile error
rather than a mock that lies.

A relative `../../ui/src/generated/openapi-types.ts` import would also resolve
everywhere, since it is type-only and erased. The subpath is preferred because it
names the generated contract as ui's public artifact instead of reaching through
its directory layout.

## Fixtures are relative to a clock, not to a date

Timestamps in `src/fixtures/` are offsets a handler re-bases at request time, and
the bucketed value series are generated from the two real endpoints of their
capture rather than stored. `src/clock.ts` and `src/fixtures/series.ts` carry the
why.

## Adding an endpoint

1. Add the handler to the right `src/handlers/*.ts` module — `mock.get(path, …)`
   only offers paths the document declares, so a typo will not compile.
2. Put its body in `src/fixtures/`, timestamps as offsets.
3. Honour every query param the app sends, and reject what the API rejects. A
   filter the mock ignores is worse than no mock: the screen looks right and the
   filter is untested. A malformed param the mock accepts is the same trap one
   step earlier — it works offline and 422s in staging.
4. Extend `scripts/check-mock-api.ts`: one scenario per check, and a negative
   case for every failure branch. `expectStatus` is the mirror of `request`.

The window and `limit` rules are ported from `python/app/domain/time_series.py`
into `src/query.ts`. When that file changes, this one is wrong until it changes
too.

`tsconfig.json` turns on `noUncheckedIndexedAccess`, above the shared preset:
the fixtures are lookup tables keyed by address, symbol and bucket index, and a
miss has to be a branch a handler takes rather than an `undefined` that reaches a
response body. It carries no comments because the repo's `check-json` hook parses
it with `jq`, which rejects JSONC.

## Gotchas worth knowing

- **`listen()` before the client is built.** `openapi-fetch` snapshots
  `globalThis.fetch` when `createApiClient` runs, and msw's node interceptors
  replace that global in `listen()`. A client constructed first keeps the
  original fetch and reaches the real network — which surfaces as a DNS failure,
  not as an unhandled-request error. `ui/src/shared/lib/api-client.ts` builds its
  client at module scope, so a vitest setup file must listen at module scope
  too, before that module is imported — including transitively, via
  `ui/src/shared/lib/queries.ts` or anything that reaches it.
- **No operation declares a 404.** Every path in the document answers `200` plus
  `422`, so the handlers that need a miss to fail — both `/v1/tokens/{chain_id}/{token_address}`
  reads, `/v1/risk/{chain_id}/{token_address}/breakdown`, `/v1/risk/rrc`, and all
  five per-prime reads (`allocations`, `risk-capital`, `exposure`,
  `total-capital`, `debt`, which also `400`s on `reference` without `aggregate`)
  — answer through `response.untyped(...)`. That records the gap in the document
  rather than pretending every id resolves. Closing it belongs in the Python
  response models. A 404 there means "not a prime": a real proxy that holds
  nothing answers `200` with `[]`, and `check-mock-api.ts` asserts both, because
  collapsing them would make an empty allocation table indistinguishable from a
  bad address.
- **A mocked production build is deliberate.** `VITE_API_MOCKS=1 npm run build`
  ships msw, the fixtures and `mockServiceWorker.js`, which is what a static
  offline demo or a Playwright target needs. Without the flag a `closeBundle`
  plugin asserts the worker script is gone.
- **`ui/public/mockServiceWorker.js` is a copy, and copies go stale.** msw's CLI
  wrote it, and nothing in the package pins the version it was written for, so an
  msw bump leaves the browser registering a worker that no longer speaks to the
  library — every request reaching the network as though the mocks were not
  installed. `npm run test:mock-worker -w ui` byte-compares it against the
  installed copy and CI runs it; when it fails, `npx msw init` in `ui` is the fix.
