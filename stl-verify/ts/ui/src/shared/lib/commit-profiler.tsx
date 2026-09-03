/**
 * Commit counting for the offline build.
 *
 * A perf claim about an interaction -- "changing the range re-renders the grid",
 * "opening the drawer commits twice" -- is otherwise argued from a flame chart
 * nobody can put in an assertion. React's `<Profiler>` already reports every
 * commit; this only keeps the running totals somewhere a Playwright case can
 * read, next to `window.resetMocks` and `window.failMock`.
 *
 * Reached only through `main.tsx`'s `import.meta.env.VITE_API_MOCKS` branch, so
 * this module and React's profiling build never enter a production bundle.
 */
import { Profiler, type ProfilerOnRenderCallback, type ReactNode } from 'react';

/**
 * What a case reads. Durations are React's `actualDuration` -- the time spent
 * rendering the tree that actually re-rendered, which is the figure an
 * optimisation is trying to move.
 */
export type CommitProfile = {
  /** Commits since the last `reset()`, split by what React was doing. */
  commits: number;
  mounts: number;
  updates: number;
  /** Milliseconds, summed and peak, over those commits. */
  totalMs: number;
  slowestMs: number;
  /** Zeroes the counters, so a case measures one interaction rather than a page. */
  reset: () => void;
};

const EMPTY = { commits: 0, mounts: 0, updates: 0, totalMs: 0, slowestMs: 0 };

function createProfile(): CommitProfile {
  const profile: CommitProfile = {
    ...EMPTY,
    reset: () => Object.assign(profile, EMPTY),
  };

  return profile;
}

/**
 * Wraps a tree in a `<Profiler>` reporting to `window.commitProfile`.
 *
 * A function rather than a component so `main.tsx` can choose it or identity in
 * one expression, which is what keeps the whole thing behind a branch the
 * bundler can fold.
 */
export function withCommitProfiler(tree: ReactNode): ReactNode {
  const profile = createProfile();
  Object.assign(window, { commitProfile: profile });

  // `nested-update` is a commit React scheduled from inside a layout effect of
  // the commit before it, so it is counted as an update rather than dropped --
  // it is exactly the kind a case should be able to catch.
  const onRender: ProfilerOnRenderCallback = (
    _id,
    phase,
    actualDuration,
  ): void => {
    profile.commits += 1;
    if (phase === 'mount') {
      profile.mounts += 1;
    } else {
      profile.updates += 1;
    }
    profile.totalMs += actualDuration;
    profile.slowestMs = Math.max(profile.slowestMs, actualDuration);
  };

  return (
    <Profiler id="app" onRender={onRender}>
      {tree}
    </Profiler>
  );
}
