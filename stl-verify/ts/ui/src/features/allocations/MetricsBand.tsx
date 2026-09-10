import { LazyRegion, lazyChunk } from '../../shared/ui/LazyRegion';
import { MetricsBandSkeleton } from './metricCards';
import type { PrimeMetricsBandProps } from './PrimeMetricsBand';

/**
 * The metrics band, and the only place it is reached from.
 *
 * The band is the app's sole consumer of `@archon-research/charting`, which
 * with visx underneath is the largest dependency in the graph. Behind a dynamic
 * import it is a chunk of its own that no other view pays for; the route's
 * loader warms it (see `preload.ts`), so in practice it downloads alongside the
 * route's own code rather than after the thirteen requests the view fires.
 */
const PrimeMetricsBand = lazyChunk(
  async () => (await import('./PrimeMetricsBand')).PrimeMetricsBand,
);

type MetricsBandProps = PrimeMetricsBandProps & {
  /**
   * The prime the figures belong to. The band's reads are validated as they are
   * selected, so a prime whose payload breaks the contract throws in here —
   * naming the prime is what stops that throw outliving it.
   */
  primeKey: string | null;
};

export function MetricsBand({ primeKey, ...props }: MetricsBandProps) {
  return (
    <LazyRegion
      title="Charts unavailable"
      subject="metrics charts"
      impact="The grid below is unaffected."
      resetKey={primeKey}
      // The same placeholder the band shows for missing figures, so a slow chunk
      // and a slow query look alike and the grid below never moves.
      pending={<MetricsBandSkeleton />}
    >
      <PrimeMetricsBand {...props} />
    </LazyRegion>
  );
}
