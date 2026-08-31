import {
  createSearchParamStripper,
  createValidatedSearchRedirect,
} from '@archon-research/router-kit';
import {
  createRootRoute,
  createRoute,
  createRouter,
  lazyRouteComponent,
  parseSearchWith,
  redirect,
  stringifySearchWith,
} from '@tanstack/react-router';
import type { z } from 'zod';

import { preloadMetricsBand } from '../features/allocations/preload';
import {
  activitiesSearchSchema,
  allocationSearchSchema,
  sharedSearchSchema,
} from '../shared/lib/search-params';
import { RootLayout } from './RootLayout';

// Every param here is a plain string, and the default JSON round-trip would
// write `?network=1` as `?network=%221%22` — a shape no existing link uses.
const parseSearch = parseSearchWith((value: string) => value);
const stringifySearch = stringifySearchWith(JSON.stringify);

// Reached through an arrow below, not passed as `beforeLoad`: an explicitly
// typed `beforeLoad` param collapses the root route's inference, which leaves
// every `navigate` search callback an implicit `any`.
const redirectToValidatedSearch = createValidatedSearchRedirect({
  stringifySearch,
});

const rootRoute = createRootRoute({
  validateSearch: sharedSearchSchema,
  component: RootLayout,
  beforeLoad: (context) => {
    redirectToValidatedSearch(context);
  },
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

// Unknown paths land on the default view rather than a dead end.
const catchAllRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '$',
  beforeLoad: ({ search }) => redirectToAllocation(search),
});

// The view sits on the branch rather than on either child: the two children
// exist only to normalise the URL in `beforeLoad` and neither renders anything
// of its own, so `/allocation` and `/allocation/$primeId` are one screen.
const allocationRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/allocation',
  validateSearch: allocationSearchSchema,
  // The charts are a chunk of their own and the view's queries take two round
  // trips to answer, so starting the fetch here -- which `defaultPreload` also
  // runs on hover -- lands it well before there is anything to plot.
  loader: preloadMetricsBand,
  component: lazyRouteComponent(
    () => import('./AllocationRoute'),
    'AllocationRoute',
  ),
});

const allocationIndexRoute = createRoute({
  getParentRoute: () => allocationRoute,
  path: '/',
  beforeLoad: ({ search }) => redirectToPrimePath(search),
});

// The prime rides in the path here, so a surviving `?prime=` would name a second
// one that nothing reads — and it may disagree with the prime on screen.
const allocationPrimeRoute = createRoute({
  getParentRoute: () => allocationRoute,
  path: '$primeId',
  beforeLoad: createSearchParamStripper('prime', { stringifySearch }),
});

const activitiesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/activities',
  validateSearch: activitiesSearchSchema,
  component: lazyRouteComponent(
    () => import('./ActivitiesRoute'),
    'ActivitiesRoute',
  ),
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  allocationRoute.addChildren([allocationIndexRoute, allocationPrimeRoute]),
  activitiesRoute,
  catchAllRoute,
]);

export const router = createRouter({
  routeTree,
  // "/activities/" must resolve like "/activities" on hosts that append a slash.
  trailingSlash: 'never',
  parseSearch,
  stringifySearch,
  // Hovering a view's link fetches its chunk and runs its loader, so the switch
  // is instant for anyone who aimed before they clicked.
  defaultPreload: 'intent',
  // TanStack Query owns freshness here. Left at its 30s default the router
  // keeps a second loader cache with its own opinion, and the two disagree.
  defaultPreloadStaleTime: 0,
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
