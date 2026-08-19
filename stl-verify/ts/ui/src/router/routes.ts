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

// Every param here is a plain string, and the default JSON round-trip would
// write `?network=1` as `?network=%221%22` — a shape no existing link uses.
const parseSearch = parseSearchWith((value: string) => value);
const stringifySearch = stringifySearchWith(JSON.stringify);

type SearchRecord = Record<string, unknown>;

type SearchCleanupContext = {
  location: { pathname: string; hash: string; search: unknown };
  matches: ReadonlyArray<{ _strictSearch: unknown }>;
};

function toSearchRecord(value: unknown): SearchRecord {
  return typeof value === 'object' && value !== null
    ? (value as SearchRecord)
    : {};
}

// Compared as URL text and without regard to order: `?network=1` parses to the
// number 1 while the schema yields "1", and a reordering renders the same data.
function rendersSameSearch(raw: SearchRecord, applied: SearchRecord): boolean {
  const appliedEntries = Object.entries(applied).filter(
    ([, value]) => value !== undefined,
  );

  return (
    appliedEntries.length === Object.keys(raw).length &&
    appliedEntries.every(([key, value]) => String(raw[key]) === String(value))
  );
}

// The schemas drop values they cannot honour, but the address bar keeps them, so
// `?range=90D` reads as 90 days of data next to a chart showing the default.
function redirectToValidatedSearch({
  location,
  matches,
}: SearchCleanupContext): void {
  // The leaf's own validated view is the whole applied set: each route's
  // `_strictSearch` already folds in every parent schema.
  // eslint-disable-next-line no-underscore-dangle -- the router names the field
  const applied = toSearchRecord(matches[matches.length - 1]?._strictSearch);

  if (rendersSameSearch(toSearchRecord(location.search), applied)) {
    return;
  }

  const hash = location.hash ? `#${location.hash}` : '';

  throw redirect({
    href: `${location.pathname}${stringifySearch(applied)}${hash}`,
    replace: true,
  });
}

const rootRoute = createRootRoute({
  validateSearch: sharedSearchSchema,
  component: App,
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

// The prime rides in the path here, so a surviving `?prime=` would name a second
// one that nothing reads — and it may disagree with the prime on screen.
const allocationPrimeRoute = createRoute({
  getParentRoute: () => allocationRoute,
  path: '$primeId',
  beforeLoad: ({ params, search }) => {
    if (!search.prime) {
      return;
    }

    throw redirect({
      to: '/allocation/$primeId',
      params,
      search: withoutLegacyPrime(search),
      replace: true,
    });
  },
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
  parseSearch,
  stringifySearch,
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
