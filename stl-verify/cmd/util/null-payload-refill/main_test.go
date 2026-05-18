package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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

// syncBuffer is a goroutine-safe writer used to capture log output from
// reportProgress in tests. slog handlers may write concurrently with the test
// goroutine reading the buffer; bytes.Buffer is not safe for that.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestReportProgress_EmitsProgressWithTotal asserts the periodic progress log
// line includes both processed and total so the operator can size completion.
func TestReportProgress_EmitsProgressWithTotal(t *testing.T) {
	t.Parallel()
	sum := newSummary()
	sum.total.Store(100)
	for range 5 {
		sum.record(Outcome{Stage: StageSkip, Reason: "already-healed"})
	}

	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		reportProgress(ctx, sum, logger, 20*time.Millisecond)
		close(done)
	}()
	// Let the ticker fire at least once.
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("reportProgress did not exit after ctx cancel")
	}

	out := buf.String()
	if !strings.Contains(out, "progress") {
		t.Fatalf("expected at least one progress line, got %q", out)
	}
	if !strings.Contains(out, "processed=5") {
		t.Errorf("expected processed=5 in log output, got %q", out)
	}
	if !strings.Contains(out, "total=100") {
		t.Errorf("expected total=100 in log output, got %q", out)
	}
}

// TestReportProgress_DisabledWhenIntervalZero asserts interval<=0 causes an
// immediate return with no log emissions.
func TestReportProgress_DisabledWhenIntervalZero(t *testing.T) {
	t.Parallel()
	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	done := make(chan struct{})
	start := time.Now()
	go func() {
		reportProgress(context.Background(), newSummary(), logger, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("reportProgress(interval=0) did not return immediately")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("reportProgress(interval=0) blocked for %s, want immediate return", elapsed)
	}
	if got := buf.String(); got != "" {
		t.Errorf("expected no log emissions when interval<=0, got %q", got)
	}
}
