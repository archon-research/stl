import { useMatchRoute, useNavigate, useRouter } from '@tanstack/react-router';
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
 * The route a view switch lands on.
 *
 * Stated once because the click and the hover both need it: a hover aimed at a
 * different address than the click takes warms reads nothing will render.
 */
function viewRoute(view: DashboardView, primeKey: string | null) {
  if (view === 'activities') {
    return { to: '/activities' } as const;
  }

  if (primeKey === null) {
    return { to: '/allocation' } as const;
  }

  return { to: '/allocation/$primeId', params: { primeId: primeKey } } as const;
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
      const prime = view === 'activities' ? (primeKey ?? undefined) : undefined;

      void navigate({
        ...viewRoute(view, primeKey),
        search: (previous) => ({ ...previous, ...patch, prime }),
        replace,
      });
    },
    [navigate],
  );
}

/**
 * Fetches a view's chunk and runs its loader, ahead of the click.
 *
 * `defaultPreload: 'intent'` does this for `<Link>`, and the view switcher is a
 * tablist of buttons rather than links -- the two views are one screen with a
 * shared shell, which is what the tab metaphor says. So the hover signal is
 * wired by hand here instead.
 *
 * The prime is half the address, so it is half the warm-up: warming
 * `/allocation` fetches the first prime's figures, which a click carrying any
 * other prime throws away while its own reads start from cold.
 */
export function useViewPreload(): (target: ViewNavigation) => void {
  const router = useRouter();

  return useCallback(
    ({ view, primeKey }: ViewNavigation) => {
      // A preload that fails is a preload that did not help; the navigation it
      // was guessing at reports for itself.
      void router
        .preloadRoute(viewRoute(view, primeKey))
        .catch(() => undefined);
    },
    [router],
  );
}
