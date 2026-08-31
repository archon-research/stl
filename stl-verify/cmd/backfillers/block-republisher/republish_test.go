package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/services/block_republish"
)

// blocks builds a request of n consecutive heights, so the ceiling cases stay
// pinned to maxBlocksPerRun rather than to a hand-written list.
func blocks(n int) []int64 {
	list := make([]int64, 0, n)
	for i := range n {
		list = append(list, int64(25_000_000+i))
	}
	return list
}

// republishStub stands in for the real activity, recording what it was asked to
// do and letting a case fail a chosen block.
type republishStub struct {
	seen []blockWork
}

func registerRepublishStub(env *testsuite.TestWorkflowEnvironment, fail func(blockWork) error) *republishStub {
	stub := &republishStub{}
	env.RegisterActivityWithOptions(
		func(_ context.Context, work blockWork) (block_republish.Result, error) {
			stub.seen = append(stub.seen, work)
			if err := fail(work); err != nil {
				return block_republish.Result{}, err
			}
			return block_republish.Result{
				BlockNumber: work.Number,
				BlockHash:   fmt.Sprintf("0x%064x", work.Number),
				Version:     work.Version,
				DataTypes:   []string{"block", "receipts", "traces"},
			}, nil
		},
		activity.RegisterOptions{Name: republishActivityName},
	)
	return stub
}

func neverFails(blockWork) error { return nil }

func failAt(number int64) func(blockWork) error {
	return func(work blockWork) error {
		if work.Number == number {
			return errors.New("the node is unreachable")
		}
		return nil
	}
}

func workflowResult(t *testing.T, env *testsuite.TestWorkflowEnvironment) RepublishResult {
	t.Helper()
	var result RepublishResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decoding the workflow result: %v", err)
	}
	return result
}

func queryProgress(t *testing.T, env *testsuite.TestWorkflowEnvironment) republishProgress {
	t.Helper()
	encoded, err := env.QueryWorkflow(progressQueryName)
	if err != nil {
		t.Fatalf("querying %q: %v", progressQueryName, err)
	}
	var progress republishProgress
	if err := encoded.Get(&progress); err != nil {
		t.Fatalf("decoding the %q query: %v", progressQueryName, err)
	}
	return progress
}

func TestRepublishParams_Resolve(t *testing.T) {
	tests := []struct {
		name            string
		in              RepublishParams
		wantBlocks      []int64
		wantVersion     int
		wantErrContains string
	}{
		{
			name:        "an omitted version means the first correction slot",
			in:          RepublishParams{Blocks: []int64{25395651}},
			wantBlocks:  []int64{25395651},
			wantVersion: 1,
		},
		{
			name:        "an explicit version is honoured",
			in:          RepublishParams{Blocks: []int64{25395651}, Version: new(3)},
			wantBlocks:  []int64{25395651},
			wantVersion: 3,
		},
		{
			name:            "version 0 is the slot being corrected, never a target",
			in:              RepublishParams{Blocks: []int64{25395651}, Version: new(0)},
			wantErrContains: "version",
		},
		{
			name:            "a negative version is rejected",
			in:              RepublishParams{Blocks: []int64{25395651}, Version: new(-1)},
			wantErrContains: "version",
		},
		{
			name:            "no blocks is nothing to do, not an empty success",
			in:              RepublishParams{},
			wantErrContains: "blocks",
		},
		{
			name:            "a non-positive block number is a typo",
			in:              RepublishParams{Blocks: []int64{25395651, 0}},
			wantErrContains: "block number",
		},
		{
			name:        "the widest accepted run",
			in:          RepublishParams{Blocks: blocks(maxBlocksPerRun)},
			wantBlocks:  blocks(maxBlocksPerRun),
			wantVersion: 1,
		},
		{
			name:            "one block over the ceiling",
			in:              RepublishParams{Blocks: blocks(maxBlocksPerRun + 1)},
			wantErrContains: fmt.Sprintf("%d", maxBlocksPerRun),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.resolve()

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got.Version != tc.wantVersion {
				t.Errorf("version = %d, want %d", got.Version, tc.wantVersion)
			}
			if len(got.Blocks) != len(tc.wantBlocks) {
				t.Fatalf("blocks = %d entries, want %d", len(got.Blocks), len(tc.wantBlocks))
			}
			for i := range got.Blocks {
				if got.Blocks[i] != tc.wantBlocks[i] {
					t.Fatalf("blocks[%d] = %d, want %d", i, got.Blocks[i], tc.wantBlocks[i])
				}
			}
		})
	}
}

func TestRepublishWorkflow_RepublishesEveryBlockAtTheRequestedVersion(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	stub := registerRepublishStub(env, neverFails)

	env.ExecuteWorkflow(republishWorkflow, RepublishParams{Blocks: []int64{25395651, 25087888}, Version: new(1)})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	want := []blockWork{{Number: 25395651, Version: 1}, {Number: 25087888, Version: 1}}
	if fmt.Sprint(stub.seen) != fmt.Sprint(want) {
		t.Errorf("activity calls = %v, want %v", stub.seen, want)
	}
	result := workflowResult(t, env)
	if result.Requested != 2 || len(result.Republished) != 2 || result.Version != 1 {
		t.Errorf("result = %+v, want 2 of 2 republished at version 1", result)
	}
}

func TestRepublishWorkflow_StopsAtTheFirstFailingBlock(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	stub := registerRepublishStub(env, failAt(25087888))

	env.ExecuteWorkflow(republishWorkflow, RepublishParams{Blocks: []int64{25395651, 25087888, 25396903}})

	if env.GetWorkflowError() == nil {
		t.Fatal("the workflow succeeded despite a failed block")
	}
	for _, work := range stub.seen {
		if work.Number == 25396903 {
			t.Error("republished a block after an earlier one failed")
		}
	}
}

// A failing run must still expose what it managed to republish, so the operator
// knows which blocks to leave out of the retry. Asserted through the progress
// query, not the result: Temporal discards the result payload of a workflow that
// returns a non-nil error.
func TestRepublishWorkflow_ExposesWhatItRepublishedBeforeFailing(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerRepublishStub(env, failAt(25087888))

	env.ExecuteWorkflow(republishWorkflow, RepublishParams{Blocks: []int64{25395651, 25087888}})

	progress := queryProgress(t, env)
	if progress.Total != 2 {
		t.Errorf("Total = %d, want 2", progress.Total)
	}
	if progress.Done != 1 {
		t.Errorf("Done = %d, want the one block that landed before the failure", progress.Done)
	}
}

func TestRepublishWorkflow_RejectsInvalidParamsWithoutTouchingTheChain(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	stub := registerRepublishStub(env, neverFails)

	env.ExecuteWorkflow(republishWorkflow, RepublishParams{Blocks: []int64{25395651}, Version: new(0)})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("the workflow accepted version 0")
	}
	var appErr *temporalsdk.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %v, want a Temporal application error", err)
	}
	if !appErr.NonRetryable() {
		t.Error("invalid parameters must be rejected non-retryably")
	}
	if len(stub.seen) != 0 {
		t.Errorf("republished %v for invalid params, want nothing", stub.seen)
	}
}

func TestNonRetryableIfStructural(t *testing.T) {
	tests := []struct {
		name          string
		in            error
		wantNil       bool
		wantRetryable bool
	}{
		{name: "no error", in: nil, wantNil: true},
		{
			name: "a structural defect cannot be retried into success",
			in:   fmt.Errorf("wrapped: %w", block_republish.ErrStructuralData),
		},
		{
			name:          "a live reorg settles, so it stays retryable",
			in:            fmt.Errorf("wrapped: %w", block_republish.ErrCanonicalHashMoved),
			wantRetryable: true,
		},
		{
			name:          "an unclassified failure stays retryable",
			in:            errors.New("dial tcp: connection refused"),
			wantRetryable: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := nonRetryableIfStructural(tc.in)

			if tc.wantNil {
				if got != nil {
					t.Fatalf("error = %v, want nil", got)
				}
				return
			}
			var appErr *temporalsdk.ApplicationError
			if tc.wantRetryable {
				if errors.As(got, &appErr) && appErr.NonRetryable() {
					t.Fatalf("error = %v, want it left retryable", got)
				}
				return
			}
			if !errors.As(got, &appErr) || !appErr.NonRetryable() {
				t.Fatalf("error = %v, want a non-retryable application error", got)
			}
		})
	}
}

// The envelope has to outlast a live reorg: a height whose canonical hash is
// moving fails ErrCanonicalHashMoved on every attempt until the chain settles,
// and an envelope shorter than that turns a recoverable run into a red one.
func TestRepublishActivityOptions_RideOutALiveReorg(t *testing.T) {
	options := republishActivityOptions()

	if options.StartToCloseTimeout <= 0 {
		t.Error("StartToCloseTimeout is unset; the shared default would apply")
	}
	if options.ScheduleToCloseTimeout < 4*options.StartToCloseTimeout {
		t.Errorf("ScheduleToClose (%s) leaves no room to retry a %s attempt",
			options.ScheduleToCloseTimeout, options.StartToCloseTimeout)
	}
	if options.ScheduleToCloseTimeout < 15*time.Minute {
		t.Errorf("ScheduleToClose = %s, too short to ride out a reorg at the head",
			options.ScheduleToCloseTimeout)
	}
	if options.RetryPolicy == nil {
		t.Fatal("RetryPolicy is unset")
	}
	if options.RetryPolicy.MaximumAttempts != 0 {
		t.Errorf("MaximumAttempts = %d, want 0 — ScheduleToClose is what bounds a run",
			options.RetryPolicy.MaximumAttempts)
	}
}
