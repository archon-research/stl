package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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

// deriveStub stands in for the archive listing and the canonical read, answering
// the derivation each height would be given and recording how often it was asked.
type deriveStub struct {
	seen     []versionRequest
	versions map[int64]int
	err      error
}

// blocks are the heights the stub was asked about, for a test that cares about
// the order and not about how each was derived.
func (d *deriveStub) blocks() []int64 {
	numbers := make([]int64, 0, len(d.seen))
	for _, request := range d.seen {
		numbers = append(numbers, request.Block)
	}
	return numbers
}

func registerDeriveStub(env *testsuite.TestWorkflowEnvironment, versions map[int64]int) *deriveStub {
	stub := &deriveStub{versions: versions}
	env.RegisterActivityWithOptions(
		func(_ context.Context, request versionRequest) (derivation, error) {
			stub.seen = append(stub.seen, request)
			if stub.err != nil {
				return derivation{}, stub.err
			}
			derived := derivation{Version: 1, Hash: stubHash(request.Block)}
			if version, ok := stub.versions[request.Block]; ok {
				derived.Version = version
			}
			return derived, nil
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

func TestRunFromInput(t *testing.T) {
	tests := []struct {
		name             string
		in               json.RawMessage
		want             []int64
		wantRepairedFlag bool
		wantErrContains  string
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
			name:            "an input still naming the version, as a JSON null",
			in:              json.RawMessage(`{"blocks":[25395651],"version":null}`),
			wantErrContains: "version",
		},
		{
			name:             "an input opting the run into an archive already repaired",
			in:               json.RawMessage(`{"blocks":[25395651],"archiveRepaired":true}`),
			want:             []int64{25395651},
			wantRepairedFlag: true,
		},
		{
			name: "an input spelling the flag out as off",
			in:   json.RawMessage(`{"blocks":[25395651],"archiveRepaired":false}`),
			want: []int64{25395651},
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
			got, err := runFromInput(tc.in)

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("runFromInput: %v", err)
			}
			if fmt.Sprint(got.Blocks) != fmt.Sprint(tc.want) {
				t.Errorf("blocks = %v, want %v", got.Blocks, tc.want)
			}
			if got.ArchiveRepaired != tc.wantRepairedFlag {
				t.Errorf("archiveRepaired = %v, want %v", got.ArchiveRepaired, tc.wantRepairedFlag)
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
	want := []blockWork{
		{Number: 25395651, Version: 1, Hash: stubHash(25395651)},
		{Number: 25087888, Version: 2, Hash: stubHash(25087888)},
	}
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
	want := []blockWork{
		{Number: 25395651, Version: 2, Hash: stubHash(25395651)},
		{Number: 25395651, Version: 2, Hash: stubHash(25395651)},
	}
	if fmt.Sprint(stub.seen) != fmt.Sprint(want) {
		t.Errorf("republish attempts = %v, want the retry at the same version", stub.seen)
	}
	if fmt.Sprint(derive.blocks()) != fmt.Sprint([]int64{25395651}) {
		t.Errorf("derived the version for %v, want it read once and reused", derive.blocks())
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
				t.Errorf("touched %v / %v for an unusable input, want nothing", derive.blocks(), stub.seen)
			}
		})
	}
}

// A height the archive already holds the canonical block for is refused while
// deriving its version, so nothing is cached and nothing is published.
func TestRepublishWorkflow_PublishesNothingWhenTheVersionCannotBeDerived(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	derive := registerDeriveStub(env, nil)
	derive.err = temporalsdk.NewNonRetryableApplicationError(
		"block 25395651 is already canonical in the archive at version 1", "StructuralData", block_republish.ErrStructuralData)
	stub := registerRepublishStub(env, neverFails)

	env.ExecuteWorkflow(republishWorkflow, input(25395651))

	if env.GetWorkflowError() == nil {
		t.Fatal("the workflow succeeded for a height it could not derive a version for")
	}
	if len(derive.seen) != 1 {
		t.Errorf("derived %v, want the one height it was given", derive.blocks())
	}
	if len(stub.seen) != 0 {
		t.Errorf("republished %v after the version could not be derived", stub.seen)
	}
}

// The flag decides where each height's event lands, so it has to reach every
// derivation in the run rather than only the first: a run half-derived one way
// and half the other would publish some heights at a slot the archive already
// holds and some one past it.
func TestRepublishWorkflow_CarriesArchiveRepairedToEveryDerivation(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	derive := registerDeriveStub(env, map[int64]int{25395651: 0, 25087888: 2})
	registerRepublishStub(env, neverFails)

	env.ExecuteWorkflow(republishWorkflow, json.RawMessage(`{"blocks":[25395651,25087888],"archiveRepaired":true}`))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	want := []versionRequest{
		{Block: 25395651, ArchiveRepaired: true},
		{Block: 25087888, ArchiveRepaired: true},
	}
	if fmt.Sprint(derive.seen) != fmt.Sprint(want) {
		t.Errorf("derivations = %v, want %v", derive.seen, want)
	}
}

// A run that does not ask for it must derive exactly as it did before: the flag
// is an opt-in into a state only a bulk-downloader repair creates.
func TestRepublishWorkflow_DerivesTheNextFreeSlotWithoutTheFlag(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	derive := registerDeriveStub(env, nil)
	registerRepublishStub(env, neverFails)

	env.ExecuteWorkflow(republishWorkflow, input(25395651))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	want := []versionRequest{{Block: 25395651}}
	if fmt.Sprint(derive.seen) != fmt.Sprint(want) {
		t.Errorf("derivations = %v, want %v", derive.seen, want)
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

	report := recordPhase(store, blockWork{Number: 25395651, Version: 2})
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

// stubHead sits far enough above every height these tests republish to clear the
// finality guard.
const stubHead = int64(26_000_000)

func stubHash(blockNumber int64) string { return fmt.Sprintf("0x%064x", blockNumber) }

// chainStub serves the canonical block at every height it is asked for. The
// embedded port is nil, so a read the republish does not issue panics rather
// than answering zero.
type chainStub struct {
	outbound.BlockchainClient
}

func (chainStub) GetCurrentBlockNumber(context.Context) (int64, error) { return stubHead, nil }

func (chainStub) GetBlockByNumber(_ context.Context, blockNumber int64, fullTx bool) (json.RawMessage, error) {
	transactions := ""
	if fullTx {
		transactions = `,"transactions":[]`
	}
	return json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":%q,"parentHash":"0x02","timestamp":"0x68b0c0c0"%s}`,
		blockNumber, stubHash(blockNumber), transactions)), nil
}

func (chainStub) GetBlockReceipts(context.Context, int64) (json.RawMessage, error) {
	return json.RawMessage(`[]`), nil
}

// archiveStub is a raw archive holding nothing, which is all RepublishBlock
// needs: it is handed its version rather than deriving one.
type archiveStub struct{}

func (archiveStub) HighestVersion(context.Context, int64) (int, bool, error) { return 0, false, nil }

func (archiveStub) BlockHashAt(context.Context, int64, int) (string, bool, error) {
	return "", false, nil
}

func newRepublishService(t *testing.T) *block_republish.Service {
	t.Helper()
	service, err := block_republish.NewService(
		block_republish.Config{ChainID: 1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		chainStub{}, archiveStub{}, memory.NewBlockCache(), memory.NewEventSink())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

// The worker caps no activity concurrency, so two operator-started runs can
// execute RepublishBlock on one pod at the same time. A progress store shared
// between them interleaves their phases into one set of heartbeat details, and
// each execution's Reset erases the other's.
func TestRepublishBlock_GivesEachExecutionItsOwnProgressStore(t *testing.T) {
	var handed []*heartbeatStub
	activities := &republishActivities{
		service: newRepublishService(t),
		newProgress: func() heartbeater {
			store := &heartbeatStub{}
			handed = append(handed, store)
			return store
		},
	}

	for _, work := range []blockWork{
		{Number: 25395651, Version: 1, Hash: stubHash(25395651)},
		{Number: 25087888, Version: 3, Hash: stubHash(25087888)},
	} {
		if _, err := activities.RepublishBlock(context.Background(), work); err != nil {
			t.Fatalf("RepublishBlock(%d): %v", work.Number, err)
		}
	}

	if len(handed) != 2 {
		t.Fatalf("progress stores created = %d, want one per execution", len(handed))
	}
	if handed[0] == handed[1] {
		t.Fatal("both executions recorded into the same progress store")
	}
	for i, want := range []blockWork{
		{Number: 25395651, Version: 1, Hash: stubHash(25395651)},
		{Number: 25087888, Version: 3, Hash: stubHash(25087888)},
	} {
		for _, beat := range handed[i].saved {
			if beat.Block != want.Number || beat.Version != want.Version {
				t.Errorf("execution %d's store holds %+v, from another execution", i, beat)
			}
		}
	}
}

// The flag picks a different derivation, not a different argument to the same
// one. A height the archive holds nothing at is repaired at version 1 by
// default; with archiveRepaired there is nothing repaired to publish at, and the
// run stops.
func TestDeriveVersion_ChoosesTheDerivationTheFlagAsksFor(t *testing.T) {
	tests := []struct {
		name            string
		request         versionRequest
		wantVersion     int
		wantErrContains string
	}{
		{
			name:        "the next free slot above what the archive holds",
			request:     versionRequest{Block: 25395651},
			wantVersion: 1,
		},
		{
			name:            "the version a repaired archive already holds",
			request:         versionRequest{Block: 25395651, ArchiveRepaired: true},
			wantErrContains: "archiveRepaired",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activities := &republishActivities{service: newRepublishService(t)}

			derived, err := activities.DeriveVersion(context.Background(), tc.request)

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeriveVersion: %v", err)
			}
			if derived.Version != tc.wantVersion {
				t.Errorf("version = %d, want %d", derived.Version, tc.wantVersion)
			}
			if derived.Hash != stubHash(tc.request.Block) {
				t.Errorf("hash = %q, want the canonical %q the republish verifies against",
					derived.Hash, stubHash(tc.request.Block))
			}
		})
	}
}
