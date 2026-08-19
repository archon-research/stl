import {
  createRootRoute,
  createRoute,
  createRouter,
  parseSearchWith,
  redirect,
  stringifySearchWith,
} from '@tanstack/react-router';
import type { z } from 'zod';

import App from '../App';
import {
  activitiesSearchSchema,
  allocationSearchSchema,
  sharedSearchSchema,
} from './search-params';

const rootRoute = createRootRoute({
  validateSearch: sharedSearchSchema,
  component: App,
});

type EntrySearch = z.infer<typeof sharedSearchSchema>;

function withoutLegacyPrime<T extends EntrySearch>({
  prime: _prime,
  ...rest
}: T) {
  return rest;
}

// Links shared before the prime moved into the path carry `?prime=`; translate
// on entry and hand every other param across untouched.
function redirectToPrimePath<T extends EntrySearch>(search: T): void {
  if (!search.prime) {
    return;
  }

  throw redirect({
    to: '/allocation/$primeId',
    params: { primeId: search.prime },
    search: withoutLegacyPrime(search),
    replace: true,
  });
}

function redirectToAllocation<T extends EntrySearch>(search: T): never {
  redirectToPrimePath(search);

  throw redirect({
    to: '/allocation',
    search: withoutLegacyPrime(search),
    replace: true,
  });
}

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  beforeLoad: ({ search }) => redirectToAllocation(search),
});

// Unknown paths land on the default view rather than a dead end, matching the
// hand-rolled path layer this replaced.
const catchAllRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '$',
  beforeLoad: ({ search }) => redirectToAllocation(search),
});

const allocationRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/allocation',
  validateSearch: allocationSearchSchema,
});

const allocationIndexRoute = createRoute({
  getParentRoute: () => allocationRoute,
  path: '/',
  beforeLoad: ({ search }) => redirectToPrimePath(search),
});

const allocationPrimeRoute = createRoute({
  getParentRoute: () => allocationRoute,
  path: '$primeId',
});

const activitiesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/activities',
  validateSearch: activitiesSearchSchema,
});

export const routeTree = rootRoute.addChildren([
  indexRoute,
  allocationRoute.addChildren([allocationIndexRoute, allocationPrimeRoute]),
  activitiesRoute,
  catchAllRoute,
]);

export const router = createRouter({
  routeTree,
  // "/activities/" must resolve like "/activities" on hosts that append a slash.
  trailingSlash: 'never',
  // Every param here is a plain string, and the default JSON round-trip would
  // write `?network=1` as `?network=%221%22` — a shape no existing link uses.
  parseSearch: parseSearchWith((value) => value),
  stringifySearch: stringifySearchWith(JSON.stringify),
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
