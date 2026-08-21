import { useEffect, useState } from 'react';

import { getProvenanceAvailability } from '../lib/api';
import { isAbortError } from '../lib/errors';
import { logging } from '../lib/logging';
import { PROVENANCE } from '../lib/provenance';
import type { Provenance } from '../types/allocation';

export type ProvenanceAvailability = {
  /** Provenances the named prime can be served from. */
  forPrime: (primeName: string | null | undefined) => readonly Provenance[];
  /** The provenance to move to, or `null` when the current one is fine. */
  fallbackFor: (primeName: string | null | undefined) => Provenance | null;
};

const EVERYTHING: readonly Provenance[] = ['indexed', 'reference', 'both'];

/**
 * Which provenances each prime can be answered from.
 *
 * Read once, before anything offers a choice: a provenance a prime cannot be
 * served from should never be selectable, and a URL asking for one should be
 * rewritten rather than left to fail request by request.
 *
 * While the answer is in flight, and if it cannot be had at all, everything is
 * treated as available. The alternative — assuming nothing is — would disable
 * the selector and redirect on every slow load, which is worse than letting a
 * request fail and reporting it on the card that asked.
 */
export function useProvenanceAvailability(): ProvenanceAvailability {
  const [byPrime, setByPrime] = useState<Map<string, readonly Provenance[]>>(
    new Map(),
  );

  useEffect(() => {
    const controller = new AbortController();

    void getProvenanceAvailability(controller.signal)
      .then((response) => {
        if (controller.signal.aborted) {
          return;
        }
        setByPrime(
          new Map(
            response.primes.map((prime) => [
              prime.name.toLowerCase(),
              prime.available,
            ]),
          ),
        );
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }
        logging.warn('Provenance coverage unavailable; offering every source', {
          error,
        });
      });

    return () => controller.abort();
  }, []);

  const forPrime = (primeName: string | null | undefined) => {
    if (!primeName) {
      return EVERYTHING;
    }
    return byPrime.get(primeName.toLowerCase()) ?? EVERYTHING;
  };

  return {
    forPrime,
    fallbackFor: (primeName) => {
      const available = forPrime(primeName);
      if (available.includes(PROVENANCE)) {
        return null;
      }
      // `both` first, then STL's own: the most complete answer this prime can
      // give, rather than the nearest to what was asked for.
      return (
        (['both', 'indexed', 'reference'] as const).find((candidate) =>
          available.includes(candidate),
        ) ?? null
      );
    },
  };
}
