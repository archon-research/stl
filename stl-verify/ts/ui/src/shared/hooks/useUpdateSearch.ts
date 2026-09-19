import { useNavigate } from '@tanstack/react-router';
import { useCallback } from 'react';

import type { AppSearchPatch } from '../lib/search-params';

/**
 * Edits search params in place, on whichever route is mounted.
 *
 * Param edits replace rather than push: a filter belongs in the URL but not in
 * the back-history, where it would take a Back press each to undo. `push` lets
 * a caller opt out of that default for an edit a reader would expect Back to
 * undo, such as a chart drag.
 */
export function useUpdateSearch(): (
  patch: AppSearchPatch,
  options?: { push?: boolean },
) => void {
  const navigate = useNavigate();

  return useCallback(
    (patch: AppSearchPatch, options?: { push?: boolean }) => {
      void navigate({
        to: '.',
        search: (previous) => ({ ...previous, ...patch }),
        replace: !options?.push,
      });
    },
    [navigate],
  );
}
