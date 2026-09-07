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

/**
 * Starts a fetch and absorbs its rejection.
 *
 * A chunk that fails to arrive is reported by the region that renders it, which
 * has a screen for it; left unhandled here the same failure would also surface
 * as an unhandled rejection from a call nobody awaited. Spelled out rather than
 * shared with `loaders.ts` so this module keeps importing nothing.
 */
function warm(started: Promise<unknown>): void {
  void started.catch(() => undefined);
}

/** The metrics band, and with it `@archon-research/charting` and visx. */
export function preloadMetricsBand(): void {
  warm(import('./PrimeMetricsBand'));
}

/** The risk drawer's body: its three tabs and the activity feed. */
export function preloadAllocationDetail(): void {
  warm(import('./BottomPanel'));
}
