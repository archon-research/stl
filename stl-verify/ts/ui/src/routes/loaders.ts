/**
 * What each route starts fetching while it is being navigated to.
 *
 * A loader runs at navigation, before the route's component exists, and
 * `defaultPreload: 'intent'` runs it again on hover — so a read stated here
 * begins a render earlier than the same read inside a component, and on the
 * view switcher it begins before the click.
 *
 * What a loader cannot do here is shorten the chain. Every per-prime read is
 * addressed to an ALM proxy that only `/v1/primes` names, so the prime list is
 * ahead of them either way; moving them here removes the render-and-navigate
 * round trip stacked on top of that request, not the request.
 *
 * Nothing below is awaited, which is the whole judgement: the one read worth
 * awaiting is the prime list, and awaiting it trades the shell's skeletons for
 * an empty content area for as long as it takes — a worse first paint in
 * exchange for the few milliseconds a render costs. So each read is started and
 * left to the component that observes it, which already owns the skeleton it
 * shows while the read is in flight and the error it shows when the read fails.
 */
import {
  findPrimeGroup,
  groupPrimesByVault,
  type PrimeGroup,
} from '../shared/lib/dashboard';
import { showsReference } from '../shared/lib/provenance';
import {
  allocationsQuery,
  chainsQuery,
  latestDebtSnapshotQuery,
  latestReferenceDebtQuery,
  primesQuery,
  protocolsQuery,
  provenanceAvailabilityQuery,
  riskCapitalQuery,
  tokenSymbolsQuery,
} from '../shared/lib/queries';
import { queryClient } from '../shared/lib/query-client';

/**
 * Starts a read and stops caring.
 *
 * A rejection is not the loader's to report — the component observing the same
 * query renders it, and the query cache has already logged it — while an
 * unhandled one would surface as a router error and take the route down with a
 * failure its own screen was built to absorb.
 */
function warm(started: Promise<unknown>): void {
  void started.catch(() => undefined);
}

/** The prime list, grouped, or `null` if it could not be had. */
async function loadPrimeGroups(): Promise<PrimeGroup[] | null> {
  try {
    return groupPrimesByVault(await queryClient.ensureQueryData(primesQuery()));
  } catch {
    return null;
  }
}

/** The reads that belong to a prime rather than to a view. */
function warmPrime(group: PrimeGroup): void {
  // The same fan-out `useAllocationRows` performs: a view carrying Sky's
  // figures is answered prime-wide off the primary proxy, and asking each proxy
  // as well would show every position once per chain.
  const proxies = showsReference
    ? group.proxyAddresses.slice(0, 1)
    : group.proxyAddresses;

  for (const proxy of proxies) {
    warm(queryClient.ensureQueryData(allocationsQuery(proxy)));
  }

  warm(
    queryClient.ensureQueryData(riskCapitalQuery(group.primaryProxyAddress)),
  );
  warm(
    queryClient.ensureQueryData(
      showsReference
        ? latestReferenceDebtQuery(group.primaryProxyAddress)
        : latestDebtSnapshotQuery(group.primaryProxyAddress),
    ),
  );
}

/**
 * The root route's reads: the prime list and the three registries the chrome
 * and both views read, none of which depends on anything else.
 */
export function loadShell(): void {
  warm(queryClient.ensureQueryData(primesQuery()));
  warm(queryClient.ensureQueryData(chainsQuery()));
  warm(queryClient.ensureQueryData(protocolsQuery()));
  warm(queryClient.ensureQueryData(provenanceAvailabilityQuery()));
}

/**
 * `/allocation` with no prime in the path, which resolves to the first one.
 *
 * The fallback is `PrimeSelectionProvider`'s, and it stays there: it redirects,
 * and a redirect issued from here would have to await the prime list. Reading
 * it a second time to start the fetch is what removes that redirect's round
 * trip from the critical path without blocking the paint before it.
 */
export function loadAllocationIndex(): void {
  warm(
    loadPrimeGroups().then((groups) => {
      const fallback = groups?.[0];
      if (fallback !== undefined) {
        warmPrime(fallback);
      }
    }),
  );
}

/**
 * The prime named in the path.
 *
 * A prime the list does not hold warms nothing: the provider's effect is what
 * rewrites the URL for it, and it has the notice to raise. A non-canonical
 * address resolves to its group here, so the rewrite it triggers lands on a
 * cache that is already filling.
 *
 * It takes the id rather than a loader context so the route can hand it over
 * from the params it inferred: hand-typing the context here would let a renamed
 * path param compile into an `undefined` id, and the throw that follows is one
 * `warm` swallows — every per-prime prefetch would stop with nothing failing.
 */
export function loadPrimeAllocations(primeId: string): void {
  warm(
    loadPrimeGroups().then((groups) => {
      const group = groups && findPrimeGroup(groups, primeId);
      if (group !== null) {
        warmPrime(group);
      }
    }),
  );
}

/**
 * The activities view's token filter options.
 *
 * Its feed stays in the component. The rows are keyed by the time range and the
 * filter chips, so a loader would need `loaderDeps` over search params that
 * change on every range pick — and each pick would then re-run the whole loader
 * and block the navigation on a fetch the feed already renders a skeleton for.
 */
export function loadActivities(): void {
  warm(queryClient.ensureQueryData(tokenSymbolsQuery()));
}
