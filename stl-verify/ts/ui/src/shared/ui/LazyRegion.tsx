import { ErrorBoundary, ErrorState } from '@archon-research/design-system';
import {
  lazy,
  Suspense,
  useEffect,
  useRef,
  type ComponentType,
  type ErrorInfo,
  type LazyExoticComponent,
  type ReactNode,
} from 'react';

import { logging } from '../lib/logging';

/**
 * A dynamic import that never arrived.
 *
 * The tag exists because the two failures a region can have reach its boundary
 * by the same path: React throws a rejected `lazy` payload exactly where it
 * throws a component's own error. Only the import site knows which it was, so
 * it says so here rather than leaving the boundary to guess from a message
 * whose wording is the browser's to change.
 */
class ChunkLoadError extends Error {
  constructor(cause: unknown) {
    // The browser's own wording names the URL that 404'd, which is the whole
    // diagnostic; the tag is what carries the classification.
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = 'ChunkLoadError';
  }
}

/**
 * `React.lazy` for a chunk a `LazyRegion` renders.
 *
 * Use it for every dynamic import inside a region: an untagged rejection is
 * read as a bug in the region, which is the safe way round but names the wrong
 * culprit.
 */
export function lazyChunk<P extends object>(
  load: () => Promise<ComponentType<P>>,
): LazyExoticComponent<ComponentType<P>> {
  return lazy(async () => {
    try {
      return { default: await load() };
    } catch (cause) {
      throw new ChunkLoadError(cause);
    }
  });
}

type RegionCopy = {
  /** Noun phrase for what is in the region: "the metrics charts could not …". */
  subject: string;
  /** What still works while the region is down. */
  impact: string;
  title: string;
};

type LazyRegionProps = RegionCopy & {
  children: ReactNode;
  pending: ReactNode;
  /**
   * Identity of what the region is showing. A render error clears when this
   * changes; a missing chunk does not, because nothing but a reload can fetch
   * it.
   */
  resetKey?: string | null;
};

// The chunk is missing from the server -- a deploy that moved it is the usual
// cause -- so a reload is the only retry that can fetch it.
const reloadPage = () => {
  window.location.reload();
};

function isChunkFailure(error: Error): boolean {
  return error instanceof ChunkLoadError;
}

const reportRegionFailure =
  ({ subject }: RegionCopy) =>
  (error: Error, { componentStack }: ErrorInfo) => {
    // Two events, not one with a flag: a missing chunk is an estate problem and
    // a render error is a bug, and they are answered by different people.
    logging.error(
      isChunkFailure(error)
        ? 'Lazy region chunk failed to load'
        : 'Lazy region threw while rendering',
      { region: subject, error, componentStack },
    );
  };

type RegionFallbackProps = RegionCopy & {
  error: Error;
  onReset: () => void;
  resetKey?: string | null;
};

/**
 * What a failed region shows, and the only place the boundary's reset is
 * reachable from — the design system hands it to the fallback and nowhere else.
 *
 * A render error is cleared when `resetKey` moves, so a throw on one prime does
 * not outlive that prime. Clearing on the same terms in the happy path would
 * mean keying the boundary and remounting the region on every change, which
 * would throw away the state of a region that is working.
 */
function RegionFallback({
  error,
  impact,
  onReset,
  resetKey,
  subject,
  title,
}: RegionFallbackProps) {
  const isChunk = isChunkFailure(error);
  const failedAt = useRef(resetKey);

  useEffect(() => {
    if (isChunk || resetKey === failedAt.current) {
      return;
    }
    onReset();
  }, [isChunk, onReset, resetKey]);

  return (
    <ErrorState
      title={title}
      description={
        isChunk
          ? `The ${subject} could not be downloaded. ${impact}`
          : `The ${subject} loaded but could not be displayed. ${impact}`
      }
      errorMessage={isChunk ? error.message : `${error.name}: ${error.message}`}
      onRetry={isChunk ? reloadPage : onReset}
      retryLabel={isChunk ? 'Reload' : 'Try again'}
      tone="critical"
      size="inline"
    />
  );
}

// A factory rather than an inline fallback: `react/no-unstable-nested-components`
// reads any JSX-returning literal in a component body as a component.
const regionErrorFallback =
  (copy: RegionCopy, resetKey?: string | null) =>
  (error: Error, resetError: () => void): ReactNode => (
    <RegionFallback
      {...copy}
      error={error}
      onReset={resetError}
      resetKey={resetKey}
    />
  );

/**
 * A `React.lazy` boundary that keeps its failure local, and says which failure
 * it was.
 *
 * Without the boundary a rejected import reaches the root one and blanks the
 * whole app; with it only this region reports. It catches more than the import,
 * though — everything the region renders throws through here too — so it sorts
 * the two apart: a chunk that never arrived is fixable only by a reload
 * (`React.lazy` caches the rejection and the browser caches the failed module
 * record), while anything else is a bug in the region, and telling a reader to
 * reload for that only re-runs the code that threw.
 */
export function LazyRegion({
  children,
  impact,
  pending,
  resetKey,
  subject,
  title,
}: LazyRegionProps) {
  const copy = { impact, subject, title };

  return (
    <ErrorBoundary
      fallback={regionErrorFallback(copy, resetKey)}
      onError={reportRegionFailure(copy)}
    >
      <Suspense fallback={pending}>{children}</Suspense>
    </ErrorBoundary>
  );
}
