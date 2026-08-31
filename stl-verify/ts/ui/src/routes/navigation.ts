import { useMatchRoute, useNavigate } from '@tanstack/react-router';
import { useCallback } from 'react';

import type { AppSearchPatch } from '../shared/lib/search-params';

export type DashboardView = 'allocation' | 'activities';

export type ViewNavigation = {
  view: DashboardView;
  primeKey: string | null;
  patch?: AppSearchPatch;
  replace?: boolean;
};

/**
 * Which view is mounted, asked of the router rather than passed down: the shell
 * lives on the root route, so it outlives whichever leaf is on screen.
 */
export function useSelectedView(): DashboardView {
  const matchRoute = useMatchRoute();

  return matchRoute({ to: '/activities' }) ? 'activities' : 'allocation';
}

/**
 * Moves to a view, carrying (or clearing) the prime.
 *
 * View and prime are both addresses, not params: the prime rides in the path on
 * the allocation view and in the query on activities.
 */
export function useViewNavigation(): (target: ViewNavigation) => void {
  const navigate = useNavigate();

  return useCallback(
    ({ view, primeKey, patch, replace }: ViewNavigation) => {
      if (view === 'activities') {
        void navigate({
          to: '/activities',
          search: (previous) => ({
            ...previous,
            ...patch,
            prime: primeKey ?? undefined,
          }),
          replace,
        });
        return;
      }

      if (primeKey === null) {
        void navigate({
          to: '/allocation',
          search: (previous) => ({ ...previous, ...patch, prime: undefined }),
          replace,
        });
        return;
      }

      void navigate({
        to: '/allocation/$primeId',
        params: { primeId: primeKey },
        search: (previous) => ({ ...previous, ...patch, prime: undefined }),
        replace,
      });
    },
    [navigate],
  );
}
