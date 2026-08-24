package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
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
	replay func(partitionWork) (partitionReplay, error),
) *[]string {
	var replayed []string
	env.RegisterActivityWithOptions(
		func(_ context.Context, rng blockRange) (discoveryResult, error) { return discover(rng) },
		activity.RegisterOptions{Name: "DiscoverVaults"},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, work partitionWork) (partitionReplay, error) {
			replayed = append(replayed, work.Partition)
			return replay(work)
		},
		activity.RegisterOptions{Name: "ReplayPartition"},
	)
	return &replayed
}

// noNewVaultsDiscovered: the range added no vault, but the database already
// holds one, so the replay phase still has something to run against.
func noNewVaultsDiscovered(blockRange) (discoveryResult, error) {
	return discoveryResult{KnownV2Vaults: 1}, nil
}

func noEventsReplayed(partitionWork) (partitionReplay, error) { return partitionReplay{}, nil }

// replayedEvents is a stub replay of n events that appended one adapter-state row each.
func replayedEvents(n int) func(partitionWork) (partitionReplay, error) {
	return func(partitionWork) (partitionReplay, error) {
		return partitionReplay{EventsReplayed: n, RowsAppended: appendedRows{AdapterStates: n}}, nil
	}
}

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
			wantErrContains: "over the 8000 limit",
		},
		// math.MaxInt64 overflows a partition walk that steps by BlockRangeSize —
		// the cursor goes negative and the loop never terminates — so the ceiling
		// has to be measured without walking.
		{
			name:            "to at the int64 ceiling is rejected",
			in:              BackfillParams{From: 1, To: math.MaxInt64},
			wantErrContains: "partitions, over the",
		},
		// The shape the partition ceiling cannot see: a WIDTH of ten partitions,
		// so the count guard passes, at a POSITION where the walk building them
		// steps past math.MaxInt64 on its last iteration. Only a ceiling on `to`
		// itself rejects it — and it has to reject by arithmetic, since walking
		// the range to find out is the very loop that never terminates.
		{
			name:            "to at the int64 ceiling with small width is rejected",
			in:              BackfillParams{From: math.MaxInt64 - 9_999, To: math.MaxInt64},
			wantErrContains: "not a plausible block number",
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
		func(blockRange) (discoveryResult, error) {
			return discoveryResult{Candidates: 9, Vaults: 2, KnownV2Vaults: 2}, nil
		},
		replayedEvents(3),
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
	// The rows are what a replay is FOR: every event can replay and still write nothing.
	if want := (appendedRows{AdapterStates: 9}); got.RowsAppended != want {
		t.Errorf("RowsAppended = %+v, want %+v", got.RowsAppended, want)
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
	replayed := registerActivityStubs(env, noNewVaultsDiscovered, func(work partitionWork) (partitionReplay, error) {
		if work.Partition == "1000-1999" {
			return partitionReplay{}, errors.New("boom")
		}
		return partitionReplay{}, nil
	})

	executeBackfill(env, BackfillParams{From: 1, To: 2500})

	if env.GetWorkflowError() == nil {
		t.Fatal("expected a failing partition to fail the run")
	}
	// The stub is retried, so the same partition can appear more than once;
	// what must not appear is anything after it.
	if slices.Contains(*replayed, "2000-2999") {
		t.Errorf("replayed %v, want nothing after the failing partition", *replayed)
	}
}

// With no VaultV2 vault in the database there is nothing for any partition to
// replay, and each activity would still pay a replay-service build and a
// registry read per 1000 blocks before finding that out for itself.
func TestBackfillWorkflow_SkipsReplayWhenNoV2VaultIsKnown(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env,
		func(blockRange) (discoveryResult, error) { return discoveryResult{Candidates: 4}, nil },
		noEventsReplayed,
	)

	executeBackfill(env, BackfillParams{From: 2000, To: 4500})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if len(*replayed) != 0 {
		t.Errorf("replayed %v with no V2 vault known, want none", *replayed)
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

// Bad input fails identically on every attempt, so the rejection must reach
// Temporal as non-retryable — otherwise the operator's typo is buried under the
// retry envelope instead of being reported back to them.
func TestBackfillWorkflow_RejectsInvalidParams(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env, noNewVaultsDiscovered, noEventsReplayed)

	executeBackfill(env, BackfillParams{To: 200})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected an error for parameters that fail validation")
	}
	var appErr *temporalsdk.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("want a Temporal application error, got %v", err)
	}
	if !appErr.NonRetryable() {
		t.Error("invalid parameters must be rejected non-retryably")
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
	replayed := registerActivityStubs(env, noNewVaultsDiscovered, noEventsReplayed)

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

// The same rejection, for the range shape the partition ceiling cannot catch: a
// `to` at the int64 ceiling with a ten-partition width. The workflow builds the
// prefix list itself, and that walk steps past math.MaxInt64, wraps negative and
// never terminates — so an unrejected run hangs the single-replica worker rather
// than failing it.
func TestBackfillWorkflow_RejectsATopOfInt64RangeBeforeWalkingIt(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	replayed := registerActivityStubs(env, noNewVaultsDiscovered, noEventsReplayed)

	executeBackfill(env, BackfillParams{From: math.MaxInt64 - 9_999, To: math.MaxInt64})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected a `to` at the int64 ceiling to be rejected")
	}
	var appErr *temporalsdk.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Errorf("error = %v, want a non-retryable rejection: no attempt of this range can succeed", err)
	}
	if len(*replayed) != 0 {
		t.Errorf("replayed %v for a rejected range, want none", *replayed)
	}
}

// Progress must be queryable: it is the only way to see how far a long run has
// got from the UI without reading raw event history.
func TestBackfillWorkflow_ExposesProgressQuery(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerActivityStubs(env, noNewVaultsDiscovered, replayedEvents(2))

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
	if want := (appendedRows{AdapterStates: 6}); got.RowsAppended != want {
		t.Errorf("RowsAppended = %+v, want %+v", got.RowsAppended, want)
	}
}

// A failing run must still expose the counts an operator needs to decide what to
// re-run. Asserted through the progress query, not the result: Temporal discards
// the result payload of a workflow that returns a non-nil error.
func TestBackfillWorkflow_ExposesPartialCountsAfterFailure(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	registerActivityStubs(env, noNewVaultsDiscovered, func(work partitionWork) (partitionReplay, error) {
		if work.Partition == "4000-4999" {
			return partitionReplay{}, errors.New("boom")
		}
		return replayedEvents(5)(work)
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
	if want := (appendedRows{AdapterStates: 10}); got.RowsAppended != want {
		t.Errorf("RowsAppended = %+v, want %+v", got.RowsAppended, want)
	}
}

// The workflow surfaces only the activity's own error, so every failure path has
// to name the partition — including the ones that fail BEFORE the replay starts
// (service build, registry load, topic derivation), which otherwise reach the
// operator as a bare "database pool cannot be nil" with nothing to re-run.
func TestReplayPartition_NamesThePartitionOnAnEarlyFailure(t *testing.T) {
	// A nil pool fails the first step, buildReplayService, which is the earliest
	// of those paths.
	activities := &backfillActivities{logger: slog.Default(), archiveDrain: func() {}}

	_, err := activities.ReplayPartition(context.Background(), partitionWork{
		Range:     blockRange{From: 1000, To: 1999},
		Partition: "1000-1999",
	})

	if err == nil {
		t.Fatal("expected a nil database pool to fail the activity")
	}
	if !strings.Contains(err.Error(), "replaying partition 1000-1999") {
		t.Errorf("error = %q, want it to name the partition being replayed", err)
	}
}

// buildReplayService only validates and assembles: nil ports, the SQS config,
// the embedded ABIs, the chain's deploy-block table. Nothing it does can succeed
// on a later attempt, so leaving it retryable spends the partition's whole 2h
// envelope on a verdict the first millisecond already reached.
func TestReplayPartition_ConstructorValidationIsNotRetried(t *testing.T) {
	activities := &backfillActivities{logger: slog.Default(), archiveDrain: func() {}}

	_, err := activities.ReplayPartition(context.Background(), partitionWork{Partition: "1000-1999"})

	var appErr *temporalsdk.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("error = %v, want a non-retryable failure: no attempt can build a service from a nil pool", err)
	}
}

// The mirror, and the constraint that tag must not overreach: the step right
// after the constructor reads the vault registry from Postgres, and a database
// that is unreachable right now is exactly what the retry envelope is for.
func TestReplayPartition_AnUnreachableDatabaseStaysRetryable(t *testing.T) {
	activities := &backfillActivities{
		logger:       slog.Default(),
		pool:         unreachablePool(t),
		multicaller:  testutil.NewMockMulticaller(),
		cfg:          config{chainID: mainnetChainID},
		archiveDrain: func() {},
	}

	_, err := activities.ReplayPartition(context.Background(), partitionWork{Partition: "1000-1999"})

	if err == nil {
		t.Fatal("expected an unreachable database to fail the activity")
	}
	// Pinned so the case cannot drift onto an earlier, deterministic guard and
	// keep passing for the wrong reason.
	if !strings.Contains(err.Error(), "loading the vault registry") {
		t.Fatalf("error = %v, want the failure to come from the registry read", err)
	}
	var appErr *temporalsdk.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		t.Errorf("error = %v, want it left retryable: a database that is down comes back", err)
	}
}

// unreachablePool hands back a real pool pointed at a port nothing listens on,
// so the first query fails as a dial error rather than a wait. pgxpool connects
// lazily, which is what lets the constructor succeed and the registry read be
// the step that fails.
func unreachablePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:nobody@127.0.0.1:1/nodb?sslmode=disable")
	if err != nil {
		t.Fatalf("building an unreachable pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Neither activity caps its attempts, so a deterministic defect that stays
// retryable is redone at up to a minute's backoff until the ScheduleToClose
// envelope runs out — surfacing a ~20s partition failure two hours late, and a
// discovery failure a day late.
func TestNonRetryableIfStructural_MarksDeterministicFailuresNonRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "an S3 gap inside a partition", err: requireCompletePartition("1000-1999", []int64{1000}, 1000, 1002)},
		{name: "an unparseable partition prefix", err: requireCompletePartition("not-a-range", nil, 0, 999)},
		{name: "a log the replay path cannot take", err: fmt.Errorf("replaying log: %w", morpho_indexer.ErrUnreplayableLog)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				t.Fatal("the fixture produced no error to classify")
			}
			var appErr *temporalsdk.ApplicationError
			if !errors.As(nonRetryableIfStructural(tc.err), &appErr) {
				t.Fatalf("want a Temporal application error, got %v", tc.err)
			}
			if !appErr.NonRetryable() {
				t.Error("this failure reproduces on every attempt, so the activity must not be retried")
			}
		})
	}
}

// The mirror image, and the more expensive mistake of the two: a transient
// S3/RPC/DB fault must keep its retries, or a blip fails a run that would have
// succeeded on the next attempt.
func TestNonRetryableIfStructural_LeavesTransientFailuresRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "a request that timed out", err: fmt.Errorf("streaming s3 object: %w", context.DeadlineExceeded)},
		{name: "an upstream that was throttling", err: errors.New("SlowDown: please reduce your request rate")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var appErr *temporalsdk.ApplicationError
			if errors.As(nonRetryableIfStructural(tc.err), &appErr) && appErr.NonRetryable() {
				t.Error("a transient fault must stay retryable; the retry envelope is what absorbs it")
			}
		})
	}
}

// Both activities heartbeat, so both must declare a timeout: without one
// Temporal notices a worker killed mid-activity only at StartToClose — 30
// minutes for a partition, 30 hours for discovery.
func TestActivityOptions_DeclareAHeartbeatTimeoutWithGraceOverTheTicker(t *testing.T) {
	tests := []struct {
		name string
		opts workflow.ActivityOptions
	}{
		{name: "discovery", opts: discoverActivityOptions()},
		{name: "replay", opts: replayActivityOptions()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.opts.HeartbeatTimeout <= heartbeatInterval {
				t.Errorf("HeartbeatTimeout = %s, want more than the %s ticker so one late ping cannot fail the activity",
					tc.opts.HeartbeatTimeout, heartbeatInterval)
			}
			if tc.opts.HeartbeatTimeout >= tc.opts.StartToCloseTimeout {
				t.Errorf("HeartbeatTimeout %s is not tighter than StartToCloseTimeout %s, so it detects nothing sooner",
					tc.opts.HeartbeatTimeout, tc.opts.StartToCloseTimeout)
			}
		})
	}
}

// The partition ceiling and the discovery timeouts have to agree: the widest
// range resolve ACCEPTS must be one a single attempt can finish. It cannot resume
// — the scan keeps no progress state — so an attempt killed at StartToClose is
// redone from block one, and a ceiling wider than the timeout covers turns every
// such range into an envelope that expires having persisted nothing.
func TestDiscoverActivityOptions_TimeoutsCoverTheWidestAcceptedRange(t *testing.T) {
	opts := discoverActivityOptions()
	widestScan := time.Duration(maxPartitionsPerRun) * discoveryScanPerPartition

	if opts.StartToCloseTimeout < widestScan {
		t.Errorf("StartToCloseTimeout = %s, want at least %s (%d partitions x %s)",
			opts.StartToCloseTimeout, widestScan, maxPartitionsPerRun, discoveryScanPerPartition)
	}
	// The envelope's claim is "one full redo", so an attempt that burns the whole
	// StartToClose ceiling must still leave room for a second one — the case the
	// widest accepted range is precisely about.
	if opts.ScheduleToCloseTimeout < 2*opts.StartToCloseTimeout {
		t.Errorf("ScheduleToCloseTimeout = %s, want at least twice the %s a single attempt may take",
			opts.ScheduleToCloseTimeout, opts.StartToCloseTimeout)
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
