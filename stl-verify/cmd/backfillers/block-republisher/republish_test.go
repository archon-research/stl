package main

import (
	"context"
	"encoding/json"
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

// input renders heights as the JSON an operator types into the Input box.
func input(numbers ...int64) json.RawMessage {
	encoded, err := json.Marshal(struct {
		Blocks []int64 `json:"blocks"`
	}{Blocks: numbers})
	if err != nil {
		panic(err)
	}
	return encoded
}

// deriveStub stands in for the archive listing, answering the version each
// height would be given and recording how often it was asked.
type deriveStub struct {
	seen     []int64
	versions map[int64]int
}

func registerDeriveStub(env *testsuite.TestWorkflowEnvironment, versions map[int64]int) *deriveStub {
	stub := &deriveStub{versions: versions}
	env.RegisterActivityWithOptions(
		func(_ context.Context, number int64) (int, error) {
			stub.seen = append(stub.seen, number)
			if version, ok := stub.versions[number]; ok {
				return version, nil
			}
			return 1, nil
		},
		activity.RegisterOptions{Name: deriveVersionActivityName},
	)
	return stub
}

// republishStub stands in for the real activity, recording every attempt it was
// asked for and letting a case fail a chosen one.
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

// failFirstAttempt is the shape a rolled pod leaves behind: the event is already
// on the topic, and the activity never got to report it.
func failFirstAttempt() func(blockWork) error {
	attempts := 0
	return func(blockWork) error {
		attempts++
		if attempts == 1 {
			return errors.New("the pod was rolled after publishing")
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

func TestBlocksFromInput(t *testing.T) {
	tests := []struct {
		name            string
		in              json.RawMessage
		want            []int64
		wantErrContains string
	}{
		{
			name: "the heights, in the order they were given",
			in:   json.RawMessage(`{"blocks":[25395651,25087888]}`),
			want: []int64{25395651, 25087888},
		},
		{
			name:            "an input still choosing the version",
			in:              json.RawMessage(`{"blocks":[25395651],"version":1}`),
			wantErrContains: "version",
		},
		{
			name:            "an input naming a field this workflow does not take",
			in:              json.RawMessage(`{"blocks":[25395651],"dryRun":true}`),
			wantErrContains: "dryRun",
		},
		{
			name:            "no blocks is nothing to do, not an empty success",
			in:              json.RawMessage(`{}`),
			wantErrContains: "blocks",
		},
		{
			name:            "an empty list",
			in:              json.RawMessage(`{"blocks":[]}`),
			wantErrContains: "blocks",
		},
		{
			name:            "a non-positive block number is a typo",
			in:              json.RawMessage(`{"blocks":[25395651,0]}`),
			wantErrContains: "block number",
		},
		{
			name:            "the same height twice",
			in:              json.RawMessage(`{"blocks":[25395651,25087888,25395651]}`),
			wantErrContains: "25395651",
		},
		{
			name: "the widest accepted run",
			in:   input(blocks(maxBlocksPerRun)...),
			want: blocks(maxBlocksPerRun),
		},
		{
			name:            "one block over the ceiling",
			in:              input(blocks(maxBlocksPerRun + 1)...),
			wantErrContains: fmt.Sprintf("%d", maxBlocksPerRun),
		},
		{
			name:            "a run started with no input at all",
			wantErrContains: "EOF",
		},
		{
			name:            "an input that is not an object",
			in:              json.RawMessage(`[25395651]`),
			wantErrContains: "cannot unmarshal array",
		},
		{
			name:            "a second object after the first",
			in:              json.RawMessage(`{"blocks":[25395651]} {"blocks":[25087888]}`),
			wantErrContains: "one object",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := blocksFromInput(tc.in)

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("blocksFromInput: %v", err)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("blocks = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRepublishWorkflow_ReportsThePerHeightVersionEachBlockLandedAt(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerDeriveStub(env, map[int64]int{25395651: 1, 25087888: 2})
	stub := registerRepublishStub(env, neverFails)

	env.ExecuteWorkflow(republishWorkflow, input(25395651, 25087888))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	want := []blockWork{{Number: 25395651, Version: 1}, {Number: 25087888, Version: 2}}
	if fmt.Sprint(stub.seen) != fmt.Sprint(want) {
		t.Errorf("activity calls = %v, want %v", stub.seen, want)
	}
	result := workflowResult(t, env)
	if result.Requested != 2 || len(result.Republished) != 2 {
		t.Fatalf("result = %+v, want 2 of 2 republished", result)
	}
	for i, want := range []int{1, 2} {
		if got := result.Republished[i].Version; got != want {
			t.Errorf("block %d republished at version %d, want %d", result.Republished[i].BlockNumber, got, want)
		}
	}
}

func TestRepublishWorkflow_StopsAtTheFirstFailingBlock(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerDeriveStub(env, nil)
	stub := registerRepublishStub(env, failAt(25087888))

	env.ExecuteWorkflow(republishWorkflow, input(25395651, 25087888, 25396903))

	if env.GetWorkflowError() == nil {
		t.Fatal("the workflow succeeded despite a failed block")
	}
	for _, work := range stub.seen {
		if work.Number == 25396903 {
			t.Error("republished a block after an earlier one failed")
		}
	}
}

// A retried republish must land in the slot the run already settled on. Deriving
// the version inside the retry would step past the objects the first attempt's
// own publish caused, correcting the height twice.
func TestRepublishWorkflow_RetriesARepublishAtTheVersionItAlreadyChose(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	derive := registerDeriveStub(env, map[int64]int{25395651: 2})
	stub := registerRepublishStub(env, failFirstAttempt())

	env.ExecuteWorkflow(republishWorkflow, input(25395651))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	want := []blockWork{{Number: 25395651, Version: 2}, {Number: 25395651, Version: 2}}
	if fmt.Sprint(stub.seen) != fmt.Sprint(want) {
		t.Errorf("republish attempts = %v, want the retry at the same version", stub.seen)
	}
	if fmt.Sprint(derive.seen) != fmt.Sprint([]int64{25395651}) {
		t.Errorf("derived the version for %v, want it read once and reused", derive.seen)
	}
}

// A failing run must still expose what it managed to republish, so the operator
// knows which blocks to leave out of the retry. Asserted through the progress
// query, not the result: Temporal discards the result payload of a workflow that
// returns a non-nil error.
func TestRepublishWorkflow_ExposesWhatItRepublishedBeforeFailing(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerDeriveStub(env, nil)
	registerRepublishStub(env, failAt(25087888))

	env.ExecuteWorkflow(republishWorkflow, input(25395651, 25087888))

	progress := queryProgress(t, env)
	if progress.Total != 2 {
		t.Errorf("Total = %d, want 2", progress.Total)
	}
	if progress.Done != 1 {
		t.Errorf("Done = %d, want the one block that landed before the failure", progress.Done)
	}
}

// An input written against the old runbook must stop the run rather than quietly
// mean something else: the version it names is no longer the version the blocks
// would land at.
func TestRepublishWorkflow_RejectsAnUnusableInputWithoutTouchingTheChain(t *testing.T) {
	tests := []struct {
		name string
		in   json.RawMessage
	}{
		{name: "one still choosing the version", in: json.RawMessage(`{"blocks":[25395651],"version":1}`)},
		{name: "one naming a field this workflow does not take", in: json.RawMessage(`{"blocks":[25395651],"dryRun":true}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			derive := registerDeriveStub(env, nil)
			stub := registerRepublishStub(env, neverFails)

			env.ExecuteWorkflow(republishWorkflow, tc.in)

			err := env.GetWorkflowError()
			if err == nil {
				t.Fatal("the workflow accepted the input")
			}
			var appErr *temporalsdk.ApplicationError
			if !errors.As(err, &appErr) {
				t.Fatalf("error = %v, want a Temporal application error", err)
			}
			if !appErr.NonRetryable() {
				t.Error("an unusable input must be rejected non-retryably")
			}
			if len(derive.seen) != 0 || len(stub.seen) != 0 {
				t.Errorf("touched %v / %v for an unusable input, want nothing", derive.seen, stub.seen)
			}
		})
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

// heartbeatStub stands in for the activity's progress store, recording what a
// beat would carry.
type heartbeatStub struct {
	saved []republishHeartbeat
	beats int
}

func (h *heartbeatStub) SaveProgress(_ context.Context, beat republishHeartbeat) error {
	h.saved = append(h.saved, beat)
	return nil
}

func (h *heartbeatStub) Beat(context.Context) { h.beats++ }

func (h *heartbeatStub) Reset() { h.saved, h.beats = nil, 0 }

// The details are what tells an operator whether a slow block is fetching,
// caching or publishing, rather than only that its worker is alive.
func TestRecordPhase_CarriesTheHeightItsVersionAndThePhase(t *testing.T) {
	store := &heartbeatStub{}
	activities := &republishActivities{progress: store}

	report := activities.recordPhase(blockWork{Number: 25395651, Version: 2})
	report(context.Background(), block_republish.PhaseFetching)
	report(context.Background(), block_republish.PhaseCaching)

	want := []republishHeartbeat{
		{Block: 25395651, Version: 2, Phase: "fetching"},
		{Block: 25395651, Version: 2, Phase: "caching"},
	}
	if fmt.Sprint(store.saved) != fmt.Sprint(want) {
		t.Errorf("heartbeats = %v, want %v", store.saved, want)
	}
}

// A rolled pod is noticed within the heartbeat timeout rather than at the
// attempt's own ceiling, so the retry republishes the same event while SNS FIFO
// is still deduplicating it.
func TestRepublishActivityOptions_NoticeADeadWorkerInsideTheDedupWindow(t *testing.T) {
	options := republishActivityOptions()

	if options.HeartbeatTimeout != heartbeatTimeoutFactor*heartbeatInterval {
		t.Errorf("HeartbeatTimeout = %s, want %s", options.HeartbeatTimeout, heartbeatTimeoutFactor*heartbeatInterval)
	}
	if options.HeartbeatTimeout <= heartbeatInterval {
		t.Errorf("HeartbeatTimeout %s leaves no grace over the %s ticker", options.HeartbeatTimeout, heartbeatInterval)
	}
	if options.HeartbeatTimeout >= options.StartToCloseTimeout {
		t.Errorf("HeartbeatTimeout %s is not tighter than StartToClose %s, so it detects nothing sooner",
			options.HeartbeatTimeout, options.StartToCloseTimeout)
	}
	if options.HeartbeatTimeout+options.RetryPolicy.InitialInterval >= snsDeduplicationWindow {
		t.Errorf("a dead worker costs %s before the retry, past the %s deduplication window",
			options.HeartbeatTimeout+options.RetryPolicy.InitialInterval, snsDeduplicationWindow)
	}
}
