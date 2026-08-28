import { useRouter } from '@tanstack/react-router';
import { useEffect, useRef } from 'react';

import type { ProvenanceAvailability } from '../hooks/useProvenanceAvailability';

/**
 * Rewrites the URL when the selected prime cannot be served from the session's
 * provenance, rather than leaving it to fail request by request.
 *
 * A full document load, because `lib/provenance` reads the value once per
 * session on purpose: a client-side switch would leave already-fetched series
 * on the old provenance.
 */
export function useProvenanceRedirect(
  availability: ProvenanceAvailability,
  primeName: string | null | undefined,
): void {
  const router = useRouter();
  const fallback = availability.fallbackFor(primeName);
  const redirected = useRef(false);

  useEffect(() => {
    if (fallback === null || redirected.current) {
      return;
    }

    redirected.current = true;
    const { href } = router.buildLocation({
      to: '.',
      search: (previous: Record<string, unknown>) => ({
        ...previous,
        reference: undefined,
        source: fallback === 'both' ? undefined : fallback,
      }),
    });
    globalThis.location.assign(href);
  }, [fallback, router]);
}
