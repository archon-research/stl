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
	sources   []string
	listErr   error
	runErrs   map[string]error // source -> error returned by RunTable
	runRows   map[string]int64 // source -> rows returned by RunTable
	runCalls  []string         // sources RunTable was invoked with, in order
	queue     []outbound.QueueDepth
	queueErr  error
	parity    []outbound.ParityRow
	parityErr error
}

func (m *mockRunner) ListSources(context.Context) ([]string, error) {
	return m.sources, m.listErr
}

func (m *mockRunner) RunTable(_ context.Context, source string) (int64, error) {
	m.runCalls = append(m.runCalls, source)
	if err := m.runErrs[source]; err != nil {
		return 0, err
	}
	return m.runRows[source], nil
}

func (m *mockRunner) QueueStatus(context.Context) ([]outbound.QueueDepth, error) {
	return m.queue, m.queueErr
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

// TestRunOnce_QueueStatusErrorTolerated: a QueueStatus read failure is logged,
// not fatal — materialization has already succeeded by that point.
func TestRunOnce_QueueStatusErrorTolerated(t *testing.T) {
	svc, err := NewService(&mockRunner{
		sources:  []string{"a"},
		runRows:  map[string]int64{"a": 1},
		queueErr: errors.New("queue read boom"),
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce should tolerate a QueueStatus error, got: %v", err)
	}
}

// TestRunOnce_RecordsQueueDepthAndParity exercises the queue-depth and parity
// recording paths (with a nil Telemetry, so it also covers the nil-safe record
// loops and the drift-logging branch).
func TestRunOnce_RecordsQueueDepthAndParity(t *testing.T) {
	svc, err := NewService(&mockRunner{
		sources: []string{"a"},
		runRows: map[string]int64{"a": 0},
		queue:   []outbound.QueueDepth{{Source: "a", Pending: 3, OldestAgeSecs: 12}},
		parity:  []outbound.ParityRow{{Source: "a", RawRows: 10, TransformedRows: 9, PendingRows: 0, Drift: 1}},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
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

// TestRunOnce_ParityStatusErrorTolerated: a ParityStatus read failure is logged,
// not fatal.
func TestRunOnce_ParityStatusErrorTolerated(t *testing.T) {
	svc, err := NewService(&mockRunner{
		sources:   []string{"a"},
		runRows:   map[string]int64{"a": 1},
		parityErr: errors.New("parity read boom"),
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce should tolerate a ParityStatus error, got: %v", err)
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
			wantErr:   "1 of 3 tables failed",
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
