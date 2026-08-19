package main

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
)

// mainnetChainID is the only chain with a known VaultV2 factory deploy block, so
// it is the one every fromV2Deploy case resolves against.
const mainnetChainID = 1

// v2DeployBlockMainnet is the VaultV2 factory deploy block on Ethereum mainnet.
const v2DeployBlockMainnet = 23_375_073

// partitionsWide builds a request expanding to exactly n partitions, so the
// ceiling cases stay pinned to maxPartitionsPerRun rather than to a hand-counted
// block number.
func partitionsWide(n int) BackfillParams {
	return BackfillParams{
		From: partition.BlockRangeSize,
		To:   int64(n) * partition.BlockRangeSize,
	}
}

// registerActivityStubs stands in for both real activities, recording which
// partitions were replayed and letting a case fail a chosen one.
func registerActivityStubs(
	env *testsuite.TestWorkflowEnvironment,
	discover func(blockRange) (discoveryResult, error),
	replay func(partitionWork) (int, error),
) *[]string {
	var replayed []string
	env.RegisterActivityWithOptions(
		func(_ context.Context, rng blockRange) (discoveryResult, error) { return discover(rng) },
		activity.RegisterOptions{Name: "DiscoverVaults"},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, work partitionWork) (int, error) {
			replayed = append(replayed, work.Partition)
			return replay(work)
		},
		activity.RegisterOptions{Name: "ReplayPartition"},
	)
	return &replayed
}

func noVaultsDiscovered(blockRange) (discoveryResult, error) { return discoveryResult{}, nil }

func noEventsReplayed(partitionWork) (int, error) { return 0, nil }

// executeBackfill runs the workflow the deployed worker registers, bound to
// mainnet so fromV2Deploy resolves.
func executeBackfill(env *testsuite.TestWorkflowEnvironment, params BackfillParams) {
	env.ExecuteWorkflow((&backfillWorkflows{chainID: mainnetChainID}).Backfill, params)
}

func TestBackfillParams_Resolve(t *testing.T) {
	tests := []struct {
		name            string
		in              BackfillParams
		chainID         int64
		wantFrom        int64
		wantTo          int64
		wantErrContains string
	}{
		{
			name:     "explicit from and to",
			in:       BackfillParams{From: 100, To: 200},
			wantFrom: 100,
			wantTo:   200,
		},
		{
			name:     "fromV2Deploy defaults from on mainnet",
			in:       BackfillParams{To: 23_500_000, FromV2Deploy: true},
			wantFrom: v2DeployBlockMainnet,
			wantTo:   23_500_000,
		},
		{
			name:     "explicit from wins over fromV2Deploy",
			in:       BackfillParams{From: 23_400_000, To: 23_500_000, FromV2Deploy: true},
			wantFrom: 23_400_000,
			wantTo:   23_500_000,
		},
		{
			name:            "fromV2Deploy on a chain with no known factory",
			in:              BackfillParams{To: 200, FromV2Deploy: true},
			chainID:         8453,
			wantErrContains: "fromV2Deploy",
		},
		{
			name:            "from missing and fromV2Deploy unset",
			in:              BackfillParams{To: 200},
			wantErrContains: "from must be a positive block number",
		},
		{
			name:            "to missing",
			in:              BackfillParams{From: 100},
			wantErrContains: "to must be a positive block number",
		},
		{
			name:            "negative from",
			in:              BackfillParams{From: -1, To: 200},
			wantErrContains: "from must be a positive block number",
		},
		{
			name:            "from after to",
			in:              BackfillParams{From: 300, To: 200},
			wantErrContains: "must be <= to",
		},
		// Both sides of the ceiling, so the boundary itself is pinned: without
		// these, flipping `>` to `>=` passes the suite.
		{
			name:     "exactly at the partition ceiling is accepted",
			in:       partitionsWide(maxPartitionsPerRun),
			wantFrom: partition.BlockRangeSize,
			wantTo:   int64(maxPartitionsPerRun) * partition.BlockRangeSize,
		},
		{
			name:            "one partition past the ceiling is rejected",
			in:              partitionsWide(maxPartitionsPerRun + 1),
			wantErrContains: "over the 10000 limit",
		},
		// math.MaxInt64 overflows a partition walk that steps by BlockRangeSize —
		// the cursor goes negative and the loop never terminates — so the ceiling
		// has to be measured without walking.
		{
			name:            "to at the int64 ceiling is rejected",
			in:              BackfillParams{From: 1, To: math.MaxInt64},
			wantErrContains: "partitions, over the",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chainID := tc.chainID
			if chainID == 0 {
				chainID = mainnetChainID
			}

			got, err := tc.in.resolve(chainID)

			if tc.wantErrContains != "" {
				if err == nil {
					t.Fatalf("expected a validation error containing %q, got %+v", tc.wantErrContains, got)
				}
				// Matching the message, not just non-nil: the rejection rows span
				// distinct guards, and a bare wantErr lets a row pass on the wrong one.
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("error = %q, want it to contain %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.From != tc.wantFrom {
				t.Errorf("From = %d, want %d", got.From, tc.wantFrom)
			}
			if got.To != tc.wantTo {
				t.Errorf("To = %d, want %d", got.To, tc.wantTo)
			}
		})
	}
}

// A run replays the full requested range, one activity per partition, in
// ascending block order — so an AddAdapter in an earlier partition always lands
// before a later partition's Allocate.
func TestBackfillWorkflow_ReplaysEveryPartitionInBlockOrder(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env,
		func(blockRange) (discoveryResult, error) { return discoveryResult{Candidates: 9, Vaults: 2}, nil },
		func(partitionWork) (int, error) { return 3, nil },
	)

	executeBackfill(env, BackfillParams{From: 2000, To: 4500})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if want := []string{"2000-2999", "3000-3999", "4000-4999"}; !slices.Equal(*replayed, want) {
		t.Fatalf("replayed %v, want %v", *replayed, want)
	}

	var got BackfillResult
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("reading workflow result: %v", err)
	}
	if got.PartitionsRun != 3 {
		t.Errorf("PartitionsRun = %d, want 3", got.PartitionsRun)
	}
	if got.EventsReplayed != 9 {
		t.Errorf("EventsReplayed = %d, want 9 (3 partitions x 3 events)", got.EventsReplayed)
	}
	if got.Discovered.Vaults != 2 {
		t.Errorf("Discovered.Vaults = %d, want 2", got.Discovered.Vaults)
	}
}

// A failing partition stops the run there. Continuing would replay later
// partitions on top of the hole the failure left, and nothing downstream can
// detect that hole.
func TestBackfillWorkflow_StopsAtTheFirstFailingPartition(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env, noVaultsDiscovered, func(work partitionWork) (int, error) {
		if work.Partition == "1000-1999" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})

	executeBackfill(env, BackfillParams{From: 1, To: 2500})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected a failing partition to fail the run")
	}
	if !strings.Contains(err.Error(), "1000-1999") {
		t.Errorf("error %v does not name the failing partition", err)
	}
	// The stub is retried, so the same partition can appear more than once;
	// what must not appear is anything after it.
	if slices.Contains(*replayed, "2000-2999") {
		t.Errorf("replayed %v, want nothing after the failing partition", *replayed)
	}
}

// Discovery persists the vaults the replay then loads from the database, so a
// failed discovery must stop the run rather than replay against a stale set.
func TestBackfillWorkflow_SkipsReplayWhenDiscoveryFails(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env,
		func(blockRange) (discoveryResult, error) { return discoveryResult{}, errors.New("s3 unreachable") },
		noEventsReplayed,
	)

	executeBackfill(env, BackfillParams{From: 2000, To: 4500})

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail when discovery fails")
	}
	if len(*replayed) != 0 {
		t.Errorf("replayed %v after a failed discovery, want none", *replayed)
	}
}

func TestBackfillWorkflow_RejectsInvalidParams(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env, noVaultsDiscovered, noEventsReplayed)

	executeBackfill(env, BackfillParams{To: 200})

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected an error for parameters that fail validation")
	}
	if len(*replayed) != 0 {
		t.Errorf("replayed %v for invalid params, want none", *replayed)
	}
}

// A pasted millisecond timestamp in `to` spans ~1.75 billion partitions. The
// ceiling has to be measured arithmetically, so the run is rejected before the
// prefix list is built: building it first exhausts the single replica's memory
// and CrashLoops the worker instead of returning this error.
func TestBackfillWorkflow_RejectsAnAbsurdRangeBeforeBuildingItsPartitionList(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env, noVaultsDiscovered, noEventsReplayed)

	executeBackfill(env, BackfillParams{From: 1, To: 1_750_000_000_000})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected a range of billions of partitions to be rejected")
	}
	if !strings.Contains(err.Error(), "partitions, over the") {
		t.Errorf("error = %v, want it to name the partition ceiling", err)
	}
	if len(*replayed) != 0 {
		t.Errorf("replayed %v for a rejected range, want none", *replayed)
	}
}

// Progress must be queryable: it is the only way to see how far a long run has
// got from the UI without reading raw event history.
func TestBackfillWorkflow_ExposesProgressQuery(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerActivityStubs(env, noVaultsDiscovered, func(partitionWork) (int, error) { return 2, nil })

	executeBackfill(env, BackfillParams{From: 2000, To: 4500})

	got := queryProgress(t, env)
	if got.PartitionsTotal != 3 {
		t.Errorf("PartitionsTotal = %d, want 3", got.PartitionsTotal)
	}
	if got.PartitionsDone != 3 {
		t.Errorf("PartitionsDone = %d, want 3", got.PartitionsDone)
	}
	if got.EventsReplayed != 6 {
		t.Errorf("EventsReplayed = %d, want 6", got.EventsReplayed)
	}
}

// A failing run must still expose the counts an operator needs to decide what to
// re-run. Asserted through the progress query, not the result: Temporal discards
// the result payload of a workflow that returns a non-nil error.
func TestBackfillWorkflow_ExposesPartialCountsAfterFailure(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerActivityStubs(env, noVaultsDiscovered, func(work partitionWork) (int, error) {
		if work.Partition == "4000-4999" {
			return 0, errors.New("boom")
		}
		return 5, nil
	})

	executeBackfill(env, BackfillParams{From: 2000, To: 4500})

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the run to fail")
	}
	got := queryProgress(t, env)
	if got.PartitionsDone != 2 {
		t.Errorf("PartitionsDone = %d, want 2 — partitions completed before the failure must stay visible", got.PartitionsDone)
	}
	if got.EventsReplayed != 10 {
		t.Errorf("EventsReplayed = %d, want 10", got.EventsReplayed)
	}
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
