import { useQuery } from '@tanstack/react-query';
import { useParams, useSearch } from '@tanstack/react-router';
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';

import {
  findPrimeGroup,
  groupPrimesByVault,
  truncateMiddle,
  type PrimeGroup,
} from '../shared/lib/dashboard';
import { toQueryErrorMessage } from '../shared/lib/errors';
import { logging } from '../shared/lib/logging';
import { primesQuery } from '../shared/lib/queries';
import type { AppSearchPatch } from '../shared/lib/search-params';
import type { Prime } from '../shared/types/allocation';
import { useSelectedView, useViewNavigation } from './navigation';

export type PrimeSelection = {
  /** One entry per prime, not one per ALM proxy. */
  primeGroups: PrimeGroup[];
  selectedPrimeGroup: PrimeGroup | null;
  selectedPrimeId: string | null;
  /** The registry row for the group's primary proxy, which names the prime. */
  selectedPrime: Prime | null;
  isLoading: boolean;
  errorMessage: string | null;
  /** Set when the URL named a prime the list does not hold; see below. */
  unknownPrimeMessage: string | null;
  selectPrime: (primeKey: string | null) => void;
};

// Everything scoped to the prime that was just left behind. Cleared as part of
// the navigation so the URL never advertises a filter from the previous prime.
const PRIME_SCOPED_RESET: AppSearchPatch = {
  network: undefined,
  protocol: undefined,
  category: undefined,
  // Both action filters: each view owns its own key, and either may be the one
  // the departing prime left behind.
  aa: undefined,
  daa: undefined,
  drawer: undefined,
  row: undefined,
};

// Shared fallback for a prime list that has not answered yet; a literal `?? []`
// would hand `groupPrimesByVault` a fresh array on every render.
const NO_PRIMES: Prime[] = [];

/**
 * Which prime the whole shell is pointed at, resolved once.
 *
 * A context because resolving it *navigates*: an unknown or non-canonical prime
 * in the URL is rewritten, and a second copy of that effect would race the
 * first. The notice it raises is state for the same reason — the URL has been
 * replaced by then, so nothing else still names the prime that was asked for.
 */
const PrimeSelectionContext = createContext<PrimeSelection | null>(null);

export function PrimeSelectionProvider({ children }: { children: ReactNode }) {
  const selectedView = useSelectedView();
  const navigateToView = useViewNavigation();
  const sharedSearch = useSearch({ from: '__root__' });
  const primePathParams = useParams({
    from: '/allocation/$primeId',
    shouldThrow: false,
  });

  const primesResult = useQuery(primesQuery());
  const primes = primesResult.data ?? NO_PRIMES;
  const isLoading = primesResult.isPending;
  const errorMessage = toQueryErrorMessage(primesResult.error);

  // Not derived: the URL is replaced with the fallback prime, so afterwards
  // nothing but this still names the prime that was asked for.
  const [unknownPrimeMessage, setUnknownPrimeMessage] = useState<string | null>(
    null,
  );

  const selectedPrimeId =
    primePathParams?.primeId ?? sharedSearch.prime ?? null;

  // One entry per prime (grouped by prime_vault_address), not one per ALM
  // proxy — a prime allocates through several proxies (one per chain), and
  // the sidebar/selection model addresses the prime, not a single proxy.
  const primeGroups = useMemo(() => groupPrimesByVault(primes), [primes]);

  const selectedPrimeGroup = useMemo(
    () => primeGroups.find((group) => group.key === selectedPrimeId) ?? null,
    [primeGroups, selectedPrimeId],
  );

  const primaryProxyAddress = selectedPrimeGroup?.primaryProxyAddress ?? null;

  const selectedPrime = useMemo(
    () => primes.find((prime) => prime.address === primaryProxyAddress) ?? null,
    [primaryProxyAddress, primes],
  );

  // Resolving the default (first) prime preserves the rest of the URL: a deep
  // link that names filters but no prime must keep those filters.
  useEffect(() => {
    if (isLoading) {
      return;
    }

    const fallbackGroup = primeGroups[0] ?? null;

    if (fallbackGroup === null) {
      // A failed prime fetch is not an empty prime list: dropping the prime out
      // of the URL here would destroy the deep link a retry could still serve.
      if (errorMessage === null && selectedPrimeId !== null) {
        navigateToView({ view: selectedView, primeKey: null, replace: true });
      }
      return;
    }

    if (!selectedPrimeId) {
      navigateToView({
        view: selectedView,
        primeKey: fallbackGroup.key,
        replace: true,
      });
      return;
    }

    const requestedGroup = findPrimeGroup(primeGroups, selectedPrimeId);

    if (requestedGroup?.key === selectedPrimeId) {
      return;
    }

    if (requestedGroup !== null) {
      // The same prime under one of its other addresses — an ALM proxy, or the
      // vault checksummed. Canonicalising the URL is not a prime swap, so it
      // keeps the link's filters and raises no notice: the reader gets the
      // prime they asked for, which is the one already on screen.
      navigateToView({
        view: selectedView,
        primeKey: requestedGroup.key,
        replace: true,
      });
      return;
    }

    // Silently swapping primes renders one prime's data under another's link,
    // and the filters in that link were scoped to the prime that is gone.
    logging.warn('Requested prime is not in the prime list', {
      requestedPrimeKey: selectedPrimeId,
      fallbackPrimeKey: fallbackGroup.key,
    });
    setUnknownPrimeMessage(
      `Prime ${truncateMiddle(selectedPrimeId)} was not found; showing ${fallbackGroup.name}.`,
    );
    navigateToView({
      view: selectedView,
      primeKey: fallbackGroup.key,
      patch: PRIME_SCOPED_RESET,
      replace: true,
    });
  }, [
    errorMessage,
    isLoading,
    navigateToView,
    primeGroups,
    selectedPrimeId,
    selectedView,
  ]);

  const selectPrime = useCallback(
    (primeKey: string | null) => {
      setUnknownPrimeMessage(null);
      navigateToView({
        view: selectedView,
        primeKey,
        patch: PRIME_SCOPED_RESET,
        replace: true,
      });
    },
    [navigateToView, selectedView],
  );

  const value = useMemo<PrimeSelection>(
    () => ({
      primeGroups,
      selectedPrimeGroup,
      selectedPrimeId,
      selectedPrime,
      isLoading,
      errorMessage,
      unknownPrimeMessage,
      selectPrime,
    }),
    [
      errorMessage,
      isLoading,
      primeGroups,
      selectPrime,
      selectedPrime,
      selectedPrimeGroup,
      selectedPrimeId,
      unknownPrimeMessage,
    ],
  );

  return (
    <PrimeSelectionContext.Provider value={value}>
      {children}
    </PrimeSelectionContext.Provider>
  );
}

export function usePrimeSelection(): PrimeSelection {
  const selection = useContext(PrimeSelectionContext);

  if (selection === null) {
    throw new Error(
      'usePrimeSelection must be used inside a PrimeSelectionProvider',
    );
  }

  return selection;
}
