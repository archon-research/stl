import { useNavigate } from '@tanstack/react-router';
import { useCallback } from 'react';

import type { AppSearchPatch } from '../router/search-params';

/**
 * Edits search params in place, on whichever route is mounted.
 *
 * Param edits replace rather than push: a filter belongs in the URL but not in
 * the back-history, where it would take a Back press each to undo.
 */
export function useUpdateSearch(): (patch: AppSearchPatch) => void {
  const navigate = useNavigate();

  return useCallback(
    (patch: AppSearchPatch) => {
      void navigate({
        to: '.',
        search: (previous) => ({ ...previous, ...patch }),
        replace: true,
      });
    },
    [navigate],
  );
}
