package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// stubProcessor is a keyProcessor that delegates to a per-test function.
// Used to drive runProcessKeys without the full refiller/S3/RPC machinery.
type stubProcessor struct {
	mu    sync.Mutex
	calls []string
	fn    func(ctx context.Context, key string) Outcome
}

func (s *stubProcessor) Process(ctx context.Context, key string) Outcome {
	s.mu.Lock()
	s.calls = append(s.calls, key)
	s.mu.Unlock()
	return s.fn(ctx, key)
}

func (s *stubProcessor) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// runProcessKeysForTest is the test entry point that mirrors what Run does
// for the worker pool: it creates an errgroup over a base ctx, schedules
// the worker goroutines via runProcessKeys, then waits. The returned error
// is whatever g.Wait reports (first non-nil worker error, or nil).
func runProcessKeysForTest(ctx context.Context, r keyProcessor, jobs <-chan string, concurrency int, logger *slog.Logger) (*summary, error) {
	g, gctx := errgroup.WithContext(ctx)
	sum := newSummary()
	runProcessKeys(g, gctx, r, jobs, concurrency, sum, logger)
	return sum, g.Wait()
}

// TestProcessKeys_FatalAbortsRun: given a processor that returns a fatal
// Outcome on the first key, subsequent keys must NOT be processed and
// g.Wait must return the underlying cause.
func TestProcessKeys_FatalAbortsRun(t *testing.T) {
	t.Parallel()
	fatal := errors.New("synthetic fatal")
	proc := &stubProcessor{fn: func(_ context.Context, key string) Outcome {
		if key == "fatal-key" {
			return Outcome{Stage: StageFail, Reason: "synthetic", Err: fatal, Fatal: true}
		}
		t.Errorf("processor must not be called for key %q after fatal", key)
		return Outcome{Stage: StageSkip}
	}}

	jobs := make(chan string, 4)
	jobs <- "fatal-key"
	jobs <- "after-1"
	jobs <- "after-2"
	jobs <- "after-3"
	close(jobs)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sum, waitErr := runProcessKeysForTest(context.Background(), proc, jobs, 1, logger)

	if !errors.Is(waitErr, fatal) {
		t.Fatalf("g.Wait() = %v, want chain containing %v", waitErr, fatal)
	}
	if got := sum.failed.Load(); got != 1 {
		t.Errorf("summary.failed = %d, want 1 (only the fatal key)", got)
	}
	if proc.callCount() != 1 {
		t.Errorf("processor called %d times, want 1", proc.callCount())
	}
}

// TestProcessKeys_FatalCancelsContextForOtherWorkers asserts a fatal
// outcome cancels gctx so peer workers exit cleanly rather than continuing
// to finalise additional keys. errgroup.WithContext gives us this for free.
func TestProcessKeys_FatalCancelsContextForOtherWorkers(t *testing.T) {
	t.Parallel()
	fatal := errors.New("synthetic fatal")
	var fatalSeen atomic.Bool

	proc := &stubProcessor{fn: func(ctx context.Context, key string) Outcome {
		if key == "fatal-key" {
			fatalSeen.Store(true)
			return Outcome{Stage: StageFail, Reason: "synthetic", Err: fatal, Fatal: true}
		}
		// Peer worker path: block until either ctx is cancelled (expected
		// after fatal returns from a peer) or a 2s safety timeout. The
		// latter would mean ctx propagation is broken.
		select {
		case <-ctx.Done():
			// Cancellation produces an empty Outcome (no state recorded)
			// matching Refiller.cancelled() semantics.
			return Outcome{}
		case <-time.After(2 * time.Second):
			return Outcome{Stage: StageSkip, Reason: "no-cancellation"}
		}
	}}

	jobs := make(chan string, 4)
	jobs <- "peer-blocker-1"
	jobs <- "fatal-key"
	jobs <- "peer-blocker-2"
	jobs <- "peer-blocker-3"
	close(jobs)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := time.Now()
	sum, waitErr := runProcessKeysForTest(context.Background(), proc, jobs, 2, logger)
	elapsed := time.Since(start)

	if !fatalSeen.Load() {
		t.Fatal("fatal-key was never processed")
	}
	if !errors.Is(waitErr, fatal) {
		t.Fatalf("g.Wait() = %v, want chain containing %v", waitErr, fatal)
	}
	// Real ctx propagation finishes in ms; > 1s means workers ignored ctx.
	if elapsed > 1500*time.Millisecond {
		t.Errorf("runProcessKeys took %s, want < 1.5s (gctx propagation likely broken)", elapsed)
	}
	if got := sum.skipped.Load(); got > 0 {
		t.Errorf("summary.skipped = %d, want 0 (peer workers must abort via ctx, not finalise)", got)
	}
}

// TestProcessKeys_CleanRun_WaitReturnsNil asserts that without a fatal
// outcome, g.Wait returns nil and all keys are processed.
func TestProcessKeys_CleanRun_WaitReturnsNil(t *testing.T) {
	t.Parallel()
	proc := &stubProcessor{fn: func(_ context.Context, _ string) Outcome {
		return Outcome{Stage: StageSkip, Reason: "already-healed"}
	}}

	jobs := make(chan string, 3)
	jobs <- "k1"
	jobs <- "k2"
	jobs <- "k3"
	close(jobs)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sum, waitErr := runProcessKeysForTest(context.Background(), proc, jobs, 2, logger)

	if waitErr != nil {
		t.Errorf("g.Wait() = %v, want nil on clean run", waitErr)
	}
	if got := sum.skipped.Load(); got != 3 {
		t.Errorf("summary.skipped = %d, want 3", got)
	}
}

// TestProcessKeys_SkipDoesNotAbort asserts per-key skips (already-healed,
// version-mismatch, no-such-key) keep the run going.
func TestProcessKeys_SkipDoesNotAbort(t *testing.T) {
	t.Parallel()
	proc := &stubProcessor{fn: func(_ context.Context, _ string) Outcome {
		return Outcome{Stage: StageSkip, Reason: "already-healed"}
	}}

	jobs := make(chan string, 3)
	jobs <- "k1"
	jobs <- "k2"
	jobs <- "k3"
	close(jobs)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sum, waitErr := runProcessKeysForTest(context.Background(), proc, jobs, 1, logger)

	if waitErr != nil {
		t.Errorf("g.Wait() = %v, want nil — skips must not abort", waitErr)
	}
	if got := sum.skipped.Load(); got != 3 {
		t.Errorf("summary.skipped = %d, want 3 (all keys processed)", got)
	}
	if proc.callCount() != 3 {
		t.Errorf("processor called %d times, want 3 (no early abort)", proc.callCount())
	}
}
