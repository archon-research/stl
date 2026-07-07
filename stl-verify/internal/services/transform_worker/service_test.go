package transform_worker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// mockRunner is a hand-written outbound.TransformRunner for unit tests.
type mockRunner struct {
	sources  []string
	listErr  error
	runErrs  map[string]error // source -> error returned by RunTable
	runRows  map[string]int64 // source -> rows returned by RunTable
	runCalls []string         // sources RunTable was invoked with, in order
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

func TestNewService(t *testing.T) {
	if _, err := NewService(nil, nil, nil); err == nil {
		t.Fatal("expected error for nil runner")
	}
	if _, err := NewService(&mockRunner{}, nil, nil); err != nil {
		t.Fatalf("unexpected error with nil logger/telemetry: %v", err)
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
			name:      "no sources is a no-op",
			runner:    &mockRunner{sources: nil},
			wantCalls: nil,
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
