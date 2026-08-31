/**
 * Warm-up handles for the allocation view's two dynamic chunks.
 *
 * Their own module, importing nothing: the router's loader and the grid both
 * need to reach them, and reaching them through the components that render
 * them would pull those components' static graph — the card chrome, the
 * skeletons — back into the entry chunk they were split out of.
 *
 * Each is idempotent; repeated calls resolve the module the first one fetched.
 */

/** The metrics band, and with it `@archon-research/charting` and visx. */
export function preloadMetricsBand(): void {
  void import('./PrimeMetricsBand');
}

/** The risk drawer's body: its three tabs and the activity feed. */
export function preloadAllocationDetail(): void {
  void import('./BottomPanel');
}
