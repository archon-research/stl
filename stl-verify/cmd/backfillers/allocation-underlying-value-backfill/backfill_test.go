package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestBackfillParams_Resolve(t *testing.T) {
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	defaultBefore, err := time.Parse(time.RFC3339, defaultBeforeCutover)
	if err != nil {
		t.Fatalf("parsing defaultBeforeCutover: %v", err)
	}

	tests := []struct {
		name      string
		in        BackfillParams
		want      passParams
		wantError string
	}{
		{
			name: "defaults",
			in:   BackfillParams{},
			want: passParams{Before: defaultBefore, Limit: defaultLimit, MaxPriceBlockLag: defaultMaxPriceLag},
		},
		{
			name: "overrides everything",
			in: BackfillParams{
				After: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Before: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
				PrimeID: 3, Limit: 500, Write: true, MaxPriceBlockLag: 100,
			},
			want: passParams{
				After: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Before: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
				PrimeID: 3, Limit: 500, Write: true, MaxPriceBlockLag: 100,
			},
		},
		{
			name: "before not after after",
			in: BackfillParams{
				After: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), Before: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantError: "must be after",
		},
		{
			name:      "before in the future",
			in:        BackfillParams{Before: now.Add(time.Hour)},
			wantError: "is in the future",
		},
		{
			name:      "negative limit",
			in:        BackfillParams{Limit: -1},
			wantError: "limit must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.in.resolve(now)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("resolve(%+v) error = %v, want containing %q", tt.in, err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve(%+v) unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("resolve(%+v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestPassParams_ToCliConfig(t *testing.T) {
	tests := []struct {
		name string
		in   passParams
		want cliConfig
	}{
		{
			name: "write true maps to dry run false",
			in: passParams{
				After: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Before: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
				PrimeID: 3, Limit: 500, Write: true, MaxPriceBlockLag: 100,
			},
			want: cliConfig{
				after: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), before: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
				primeID: 3, limit: 500, dryRun: false, maxPriceLag: 100,
			},
		},
		{
			name: "write false maps to dry run true",
			in:   passParams{Limit: 100, Write: false, MaxPriceBlockLag: 7200},
			want: cliConfig{limit: 100, dryRun: true, maxPriceLag: 7200},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.toCliConfig(); got != tt.want {
				t.Errorf("toCliConfig() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// passCalls records every passParams the stub RunPass activity was invoked
// with, in call order.
type passCalls struct {
	list []passParams
}

// registerRunPassStub stands in for the real RunPass activity, recording what
// it was called with and letting a case script the response sequence.
func registerRunPassStub(env *testsuite.TestWorkflowEnvironment, stub func(passParams) (passActivityResult, error)) *passCalls {
	calls := &passCalls{}
	env.RegisterActivityWithOptions(
		func(_ context.Context, params passParams) (passActivityResult, error) {
			calls.list = append(calls.list, params)
			return stub(params)
		},
		activity.RegisterOptions{Name: "RunPass"},
	)
	return calls
}

func executeBackfill(env *testsuite.TestWorkflowEnvironment, params BackfillParams) {
	env.ExecuteWorkflow(backfillWorkflow, params)
}

func queryProgress(t *testing.T, env *testsuite.TestWorkflowEnvironment) backfillProgress {
	t.Helper()

	encoded, err := env.QueryWorkflow(progressQueryName)
	if err != nil {
		t.Fatalf("querying %q: %v", progressQueryName, err)
	}
	var got backfillProgress
	if err := encoded.Get(&got); err != nil {
		t.Fatalf("decoding progress: %v", err)
	}
	return got
}

// Bad input fails identically on every attempt, so the rejection must reach
// Temporal as non-retryable — otherwise the operator's typo is buried under the
// retry envelope instead of being reported back to them.
func TestBackfillWorkflow_RejectsInvalidParams(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	calls := registerRunPassStub(env, func(passParams) (passActivityResult, error) {
		return passActivityResult{}, nil
	})

	executeBackfill(env, BackfillParams{
		After:  time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Before: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected an error for before <= after")
	}
	var appErr *temporalsdk.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("error = %v, want a non-retryable rejection", err)
	}
	if len(calls.list) != 0 {
		t.Errorf("ran %d pass(es) for invalid params, want none", len(calls.list))
	}
}

// Progress must be queryable: it is the only way to see how far a long run has
// got from the UI without reading raw event history.
func TestBackfillWorkflow_ExposesProgressQuery(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	wantCursor := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	registerRunPassStub(env, func(passParams) (passActivityResult, error) {
		return passActivityResult{Fetched: 40, Written: 40, Cursor: wantCursor}, nil
	})

	executeBackfill(env, BackfillParams{Limit: 100})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	got := queryProgress(t, env)
	if got.PassesDone != 1 {
		t.Errorf("PassesDone = %d, want 1", got.PassesDone)
	}
	if got.RowsFetched != 40 || got.RowsWritten != 40 {
		t.Errorf("progress = %+v, want 40/40 fetched/written", got)
	}
	if !got.Cursor.Equal(wantCursor) {
		t.Errorf("Cursor = %v, want %v", got.Cursor, wantCursor)
	}
}

// A full page (Fetched == Limit) is not necessarily the end of the range, so
// the workflow must keep walking passes, each one starting where the last
// pass's cursor left off, until a short page proves the range is exhausted.
func TestBackfillWorkflow_WalksMultiplePassesUntilAShortPage(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	responses := []passActivityResult{
		{Fetched: 2, Written: 2, Cursor: base.Add(time.Hour)},
		{Fetched: 1, Written: 1, Cursor: base.Add(2 * time.Hour)},
	}
	i := 0
	calls := registerRunPassStub(env, func(passParams) (passActivityResult, error) {
		r := responses[i]
		i++
		return r, nil
	})

	executeBackfill(env, BackfillParams{Limit: 2})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if len(calls.list) != 2 {
		t.Fatalf("ran %d pass(es), want 2", len(calls.list))
	}
	if !calls.list[0].After.IsZero() {
		t.Errorf("first pass After = %v, want the zero time (from the beginning)", calls.list[0].After)
	}
	if !calls.list[1].After.Equal(base.Add(time.Hour)) {
		t.Errorf("second pass After = %v, want the first pass's cursor %v", calls.list[1].After, base.Add(time.Hour))
	}
	got := queryProgress(t, env)
	if got.PassesDone != 2 || got.RowsFetched != 3 || got.RowsWritten != 3 {
		t.Errorf("progress = %+v, want 2 passes / 3 fetched / 3 written", got)
	}
	if !got.Cursor.Equal(base.Add(2 * time.Hour)) {
		t.Errorf("Cursor = %v, want the last pass's %v", got.Cursor, base.Add(2*time.Hour))
	}
}

// -after has only created_at resolution, so a full page whose rows all share
// one created_at can never be advanced past. The workflow must fail that run
// non-retryably rather than re-fetch the identical page forever.
func TestBackfillWorkflow_StalledCursorFailsNonRetryably(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	stuck := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	calls := registerRunPassStub(env, func(passParams) (passActivityResult, error) {
		return passActivityResult{Fetched: 5, Written: 5, Cursor: stuck}, nil
	})

	executeBackfill(env, BackfillParams{Limit: 5})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected a stalled cursor to fail the run")
	}
	var appErr *temporalsdk.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("error = %v, want a non-retryable rejection", err)
	}
	if len(calls.list) != 2 {
		t.Errorf("ran %d pass(es), want exactly 2: the first pass advances the cursor once, "+
			"and the second must fail as soon as it repeats", len(calls.list))
	}
}

// A short (empty) page ends the run cleanly rather than as a failure: an
// already-fully-backfilled range legitimately has nothing left to fetch.
func TestBackfillWorkflow_EmptyPageEndsTheRunSuccessfully(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	calls := registerRunPassStub(env, func(passParams) (passActivityResult, error) {
		return passActivityResult{}, nil
	})

	executeBackfill(env, BackfillParams{Limit: 100})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if len(calls.list) != 1 {
		t.Errorf("ran %d pass(es), want exactly 1", len(calls.list))
	}
	got := queryProgress(t, env)
	if got.PassesDone != 1 || got.RowsFetched != 0 {
		t.Errorf("progress = %+v, want 1 pass / 0 rows", got)
	}
}

// A failing run must still expose the counts an operator needs to decide what
// to re-run. Asserted through the progress query, not the result: Temporal
// discards the result payload of a workflow that returns a non-nil error.
func TestBackfillWorkflow_ExposesPartialCountsAfterAPassFails(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	i := 0
	registerRunPassStub(env, func(passParams) (passActivityResult, error) {
		i++
		// Fails permanently from the second pass on: a one-shot failure would
		// just be retried away by the activity's own retry policy, proving
		// nothing about a failure that actually ends the run.
		if i >= 2 {
			return passActivityResult{}, errors.New("db unreachable")
		}
		return passActivityResult{Fetched: 3, Written: 3, Cursor: base.Add(time.Hour)}, nil
	})

	executeBackfill(env, BackfillParams{Limit: 3})

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the run to fail")
	}
	got := queryProgress(t, env)
	if got.PassesDone != 1 {
		t.Errorf("PassesDone = %d, want 1: the pass before the failure must stay visible", got.PassesDone)
	}
	if got.RowsFetched != 3 || got.RowsWritten != 3 {
		t.Errorf("progress = %+v, want the first pass's 3/3/3", got)
	}
}
