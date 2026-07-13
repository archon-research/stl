package transform_worker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockRunner is a hand-written outbound.TransformRunner for unit tests.
type mockRunner struct {
	sources       []string
	listErr       error
	runErrs       map[string]error    // source -> error returned by RunTable
	runRows       map[string]int64    // source -> consumed count returned by RunTable
	runUpserts    map[string]int64    // source -> upserted count returned by RunTable
	runCalls      []string            // sources RunTable was invoked with, in order
	runHook       func(source string) // optional: called at the start of RunTable (e.g. to cancel the parent)
	deExpires     map[string]int      // source -> times RunTable returns DeadlineExceeded before succeeding (drives the second drain pass)
	blockUntilCtx map[string]bool     // source -> block until the slice ctx is done, then return its error (a real slice expiry)
	queue         []outbound.QueueDepth
	queueErr      error
	parity        []outbound.ParityRow
	parityErr     error
	refreshCalls  []string
	refreshErr    error
}

func (m *mockRunner) ListSources(context.Context) ([]string, error) {
	return m.sources, m.listErr
}

func (m *mockRunner) RunTable(ctx context.Context, source string) (int64, int64, error) {
	m.runCalls = append(m.runCalls, source)
	if m.runHook != nil {
		m.runHook(source)
	}
	// Block until the slice ctx expires, then surface its error: simulates a batch
	// that cannot finish within its slice (a real slice expiry / backlog).
	if m.blockUntilCtx[source] {
		<-ctx.Done()
		return 0, 0, ctx.Err()
	}
	// Mirror the adapter: a done context surfaces as the context error, so the
	// service can tell a parent cancellation from a clean time-slice expiry.
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	// Simulate a source that needs several passes: return DeadlineExceeded (a clean
	// slice expiry with backlog) a fixed number of times, then succeed.
	if m.deExpires[source] > 0 {
		m.deExpires[source]--
		return 0, 0, context.DeadlineExceeded
	}
	if err := m.runErrs[source]; err != nil {
		return 0, 0, err
	}
	return m.runRows[source], m.runUpserts[source], nil
}

func (m *mockRunner) QueueStatus(context.Context) ([]outbound.QueueDepth, error) {
	return m.queue, m.queueErr
}

func (m *mockRunner) RefreshParity(_ context.Context, source string) error {
	m.refreshCalls = append(m.refreshCalls, source)
	return m.refreshErr
}

func (m *mockRunner) ParityStatus(context.Context) ([]outbound.ParityRow, error) {
	return m.parity, m.parityErr
}

func TestNewService_NilRunnerErrors(t *testing.T) {
	if _, err := NewService(nil, nil, nil); err == nil {
		t.Fatal("expected error for nil runner")
	}
}

func TestNewService_DefaultsWhenLoggerAndTelemetryNil(t *testing.T) {
	if _, err := NewService(&mockRunner{}, nil, nil); err != nil {
		t.Fatalf("unexpected error with nil logger/telemetry: %v", err)
	}
}

// TestRunOnce_QueueStatusErrorSurfaced: a QueueStatus read failure must fail the
// run. The queue gauges are a silent-failure detector, so a permanently broken read
// has to surface rather than leave stale gauges reading healthy.
func TestRunOnce_QueueStatusErrorSurfaced(t *testing.T) {
	boom := errors.New("queue read boom")
	svc, err := NewService(&mockRunner{
		sources:  []string{"a"},
		runRows:  map[string]int64{"a": 1},
		queueErr: boom,
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err == nil || !errors.Is(err, boom) {
		t.Fatalf("RunOnce should surface a QueueStatus error, got: %v", err)
	}
}

// TestRunOnce_RecordsQueueDepthAndParity exercises the queue-depth and parity
// recording paths (with a nil Telemetry, so it also covers the nil-safe record
// loops and the drift-logging branch).
func TestRunOnce_RecordsQueueDepthAndParity(t *testing.T) {
	m := &mockRunner{
		sources: []string{"a", "b"},
		runRows: map[string]int64{"a": 0, "b": 0},
		queue:   []outbound.QueueDepth{{Source: "a", Pending: 3, OldestAgeSecs: 12}},
		parity:  []outbound.ParityRow{{Source: "a", RawRows: 10, TransformedRows: 9, PendingRows: 0, Drift: 1}},
	}
	svc, err := NewService(m, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Every source's parity ledger is refreshed before reading drift.
	if !slices.Equal(m.refreshCalls, []string{"a", "b"}) {
		t.Errorf("RefreshParity calls = %v, want [a b]", m.refreshCalls)
	}
}

// TestRunOnce_ParityRefreshErrorSurfaced: a RefreshParity failure must fail the run
// (the parity ledger is the correctness backstop; a silently failing refresh cannot
// be allowed to leave a stale gauge reading healthy).
func TestRunOnce_ParityRefreshErrorSurfaced(t *testing.T) {
	boom := errors.New("refresh boom")
	svc, err := NewService(&mockRunner{
		sources:    []string{"a"},
		runRows:    map[string]int64{"a": 1},
		refreshErr: boom,
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err == nil || !errors.Is(err, boom) {
		t.Fatalf("RunOnce should surface a RefreshParity error, got: %v", err)
	}
}

// TestRunOnce_DrainBudgetStopsCleanly: when the per-tick drain budget is already
// spent, RunOnce stops before running any source and does NOT mark the run failed
// (the backlog is carried to the next tick).
func TestRunOnce_DrainBudgetStopsCleanly(t *testing.T) {
	orig := drainBudget
	drainBudget = -time.Second // deadline in the past: budget immediately exhausted
	t.Cleanup(func() { drainBudget = orig })

	m := &mockRunner{sources: []string{"a", "b"}, runRows: map[string]int64{"a": 1, "b": 1}}
	svc, err := NewService(m, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("drain budget expiry should not fail the run, got: %v", err)
	}
	if len(m.runCalls) != 0 {
		t.Errorf("expected no RunTable calls once the budget is spent, got %v", m.runCalls)
	}
}

// TestRunOnce_ParityStatusErrorSurfaced: a ParityStatus read failure must fail the run.
func TestRunOnce_ParityStatusErrorSurfaced(t *testing.T) {
	boom := errors.New("parity read boom")
	svc, err := NewService(&mockRunner{
		sources:   []string{"a"},
		runRows:   map[string]int64{"a": 1},
		parityErr: boom,
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err == nil || !errors.Is(err, boom) {
		t.Fatalf("RunOnce should surface a ParityStatus error, got: %v", err)
	}
}

// TestRunOnce_ParentCancelDuringDrainFails: a parent-context cancellation while
// draining the LAST source (the case with no next-iteration guard) must fail the
// run, not be mistaken for the clean per-source budget expiry.
func TestRunOnce_ParentCancelDuringDrainFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := &mockRunner{
		sources: []string{"a", "b"},
		runRows: map[string]int64{"a": 1, "b": 1},
	}
	m.runHook = func(source string) {
		if source == "b" { // cancel mid-drain of the final source
			cancel()
		}
	}
	svc, err := NewService(m, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	err = svc.RunOnce(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce should fail wrapping context.Canceled on a mid-drain parent cancel, got: %v", err)
	}
}

// TestRunOnce_SecondPassDrainsBacklog: a source whose slice expires with backlog is
// retried in the second pass with the slack the other sources left, until it drains.
// Here "b" reports two slice expiries then succeeds, so it is called three times while
// "a" is called once, and the run succeeds (a clean slice expiry is not a failure).
func TestRunOnce_SecondPassDrainsBacklog(t *testing.T) {
	m := &mockRunner{
		sources:   []string{"a", "b"},
		runRows:   map[string]int64{"a": 1, "b": 5},
		deExpires: map[string]int{"b": 2},
	}
	svc, err := NewService(m, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce should not fail on a clean slice expiry, got: %v", err)
	}
	if !slices.Equal(m.runCalls, []string{"a", "b", "b", "b"}) {
		t.Errorf("RunTable calls = %v, want [a b b b] (b retried in the second pass)", m.runCalls)
	}
}

// TestRunOnce_BudgetExpiryIsCleanNotFailure: a source that never finishes within the
// budget ends the tick budget-expired, which is a clean stop (no run error), and the
// drain-budget-exceeded path fires.
func TestRunOnce_BudgetExpiryIsCleanNotFailure(t *testing.T) {
	origBudget, origFloor := drainBudget, minDrainSlice
	drainBudget, minDrainSlice = 150*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { drainBudget, minDrainSlice = origBudget, origFloor })

	m := &mockRunner{
		sources:       []string{"b"},
		blockUntilCtx: map[string]bool{"b": true},
	}
	svc, err := NewService(m, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("budget expiry should be a clean stop, got: %v", err)
	}
	if !slices.Equal(m.runCalls, []string{"b"}) {
		t.Errorf("RunTable calls = %v, want [b]", m.runCalls)
	}
}

// TestRunOnce_FloorReducesSourceCount: when the fair split falls below minDrainSlice,
// the slice is floored and fewer sources are served this tick (the rest carry
// forward), rather than every source getting a doomed sub-batch slice. With a budget
// under two floors and two slow sources, only the first is served this tick.
func TestRunOnce_FloorReducesSourceCount(t *testing.T) {
	origBudget, origFloor := drainBudget, minDrainSlice
	drainBudget, minDrainSlice = 80*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { drainBudget, minDrainSlice = origBudget, origFloor })

	m := &mockRunner{
		sources:       []string{"a", "b"},
		blockUntilCtx: map[string]bool{"a": true, "b": true},
	}
	svc, err := NewService(m, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("budget expiry should be a clean stop, got: %v", err)
	}
	if !slices.Equal(m.runCalls, []string{"a"}) {
		t.Errorf("RunTable calls = %v, want [a] (budget under two floors serves one source)", m.runCalls)
	}
}

func TestRunOnce(t *testing.T) {
	errBoom := errors.New("boom")

	tests := []struct {
		name      string
		runner    *mockRunner
		wantErr   string // substring; "" means no error
		wantCalls []string
	}{
		{
			name:    "list error short-circuits",
			runner:  &mockRunner{listErr: errBoom},
			wantErr: "listing transform sources",
		},
		{
			name:    "empty source list is an error",
			runner:  &mockRunner{sources: nil},
			wantErr: "no transform sources",
		},
		{
			name: "all sources succeed",
			runner: &mockRunner{
				sources: []string{"a", "b"},
				runRows: map[string]int64{"a": 3, "b": 0},
			},
			wantCalls: []string{"a", "b"},
		},
		{
			name: "one table fails, the rest still run",
			runner: &mockRunner{
				sources: []string{"a", "b", "c"},
				runErrs: map[string]error{"b": errBoom},
				runRows: map[string]int64{"a": 1, "c": 2},
			},
			wantErr:   "1 error(s)",
			wantCalls: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// nil telemetry exercises the nil-safe metric path.
			svc, err := NewService(tt.runner, nil, nil)
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}

			err = svc.RunOnce(context.Background())
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("RunOnce: unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("RunOnce error = %v, want substring %q", err, tt.wantErr)
			}
			if !slices.Equal(tt.runner.runCalls, tt.wantCalls) {
				t.Errorf("RunTable calls = %v, want %v", tt.runner.runCalls, tt.wantCalls)
			}
		})
	}
}

// TestRunOnce_JoinsAllFailures confirms every failing source is surfaced.
func TestRunOnce_JoinsAllFailures(t *testing.T) {
	errA := errors.New("a failed")
	errC := errors.New("c failed")
	svc, err := NewService(&mockRunner{
		sources: []string{"a", "b", "c"},
		runErrs: map[string]error{"a": errA, "c": errC},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errC) {
		t.Errorf("joined error %v should wrap both errA and errC", err)
	}
}
