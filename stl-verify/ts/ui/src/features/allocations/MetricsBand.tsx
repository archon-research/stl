import { lazy, Suspense } from 'react';

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
const PrimeMetricsBand = lazy(async () => ({
  default: (await import('./PrimeMetricsBand')).PrimeMetricsBand,
}));

export function MetricsBand(props: PrimeMetricsBandProps) {
  // The same placeholder the band shows for missing figures, so a slow chunk
  // and a slow query look alike and the grid below never moves.
  return (
    <Suspense fallback={<MetricsBandSkeleton />}>
      <PrimeMetricsBand {...props} />
    </Suspense>
  );
}
