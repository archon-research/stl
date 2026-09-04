package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/services/block_republish"
)

const (
	// deriveVersionActivityName and republishActivityName are registered
	// explicitly so the workflow's history keeps naming the same activities
	// across a Go rename.
	deriveVersionActivityName = "DeriveVersion"
	republishActivityName     = "RepublishBlock"

	// progressQueryName is queryable mid-run from the Temporal UI's Query tab.
	// It is also the only channel a FAILING run has: Temporal discards the result
	// payload of a workflow that returns an error.
	progressQueryName = "progress"

	// heartbeatInterval is how often a running republish reports liveness, and
	// heartbeatTimeoutFactor the grace Temporal allows over it so one missed ping
	// cannot fail a live attempt. Together they decide how long a killed worker
	// looks alive: short enough that the retry's identical event still lands
	// inside snsDeduplicationWindow.
	heartbeatInterval      = 10 * time.Second
	heartbeatTimeoutFactor = 3

	// snsDeduplicationWindow is SNS FIFO's fixed deduplication window. A retry
	// that reaches it inside this window never reaches the queues at all.
	snsDeduplicationWindow = 5 * time.Minute

	// maxBlocksPerRun bounds a mistyped input. Each block is one activity of
	// ~6 history events, so 200 is nowhere near Temporal's 51,200-event ceiling —
	// the real bound is the operator's: a repair list this long is a symptom to
	// investigate, not a run to start.
	maxBlocksPerRun = 200
)

// RepublishParams is the JSON an operator supplies in the Temporal UI's Input box:
//
//	{"blocks":[25395651,25087888]}
//
// blocks are the heights to republish, in the order they will be processed. The
// version is not one of them: each height lands one past whatever its raw
// archive already holds. Version is declared only so an input written against
// the runbook that took it is refused by name rather than silently republishing
// somewhere else.
type RepublishParams struct {
	Blocks  []int64 `json:"blocks"`
	Version *int    `json:"version,omitempty"`
}

func (p RepublishParams) resolve() ([]int64, error) {
	if p.Version != nil {
		return nil, fmt.Errorf(
			"version is no longer an input: every height is republished one past the highest version its raw " +
				"archive already holds. Drop it and start the run again")
	}
	if len(p.Blocks) == 0 {
		return nil, fmt.Errorf("blocks must name at least one height to republish")
	}
	if len(p.Blocks) > maxBlocksPerRun {
		return nil, fmt.Errorf(
			"this run names %d blocks, over the %d limit: split it into smaller runs", len(p.Blocks), maxBlocksPerRun)
	}
	seen := make(map[int64]bool, len(p.Blocks))
	for _, number := range p.Blocks {
		if number <= 0 {
			return nil, fmt.Errorf("block number must be positive, got %d", number)
		}
		// A height republished twice in one run lands at two versions: the second
		// pass reads the archive the first one has by then been written into.
		if seen[number] {
			return nil, fmt.Errorf("block %d is named twice; republish a height once per run", number)
		}
		seen[number] = true
	}
	return p.Blocks, nil
}

// republishHeartbeat is what a beat carries, so a slow block shows in the
// Temporal UI as the step it is in rather than only as a live worker.
type republishHeartbeat struct {
	Block   int64  `json:"block"`
	Version int    `json:"version"`
	Phase   string `json:"phase"`
}

// blockWork is one height and the version settled for it. The version travels
// through workflow history, so a retried republish reuses the slot rather than
// deriving a later one from the archive its own first attempt filled.
type blockWork struct {
	Number  int64 `json:"number"`
	Version int   `json:"version"`
}

// RepublishResult is the workflow's return value, shown in the UI's Result
// panel. Each entry carries the version its own height landed at.
type RepublishResult struct {
	Requested   int                      `json:"requested"`
	Republished []block_republish.Result `json:"republished"`
}

type republishProgress struct {
	Total       int                      `json:"total"`
	Done        int                      `json:"done"`
	Republished []block_republish.Result `json:"republished"`
}

// republishWorkflow republishes every named block in order and hard-stops on the
// first failure. One activity per block, sequentially: every completed block is
// already in the event history, so a retry or a rolled pod resumes at the next
// one. The hard stop is the usual rule — continuing past a block that failed
// would end the run reporting success over a height still holding the losing
// fork.
func republishWorkflow(ctx workflow.Context, input json.RawMessage) (RepublishResult, error) {
	var state republishProgress
	if err := registerProgressQuery(ctx, &state); err != nil {
		return RepublishResult{}, err
	}

	blocks, err := resolveBlocks(input)
	if err != nil {
		return RepublishResult{}, err
	}
	state.Total = len(blocks)

	logger := workflow.GetLogger(ctx)
	logger.Info("starting block republish", "blocks", state.Total)

	// Temporal discards the result payload of a workflow that returns an error,
	// so a failing run reports what it managed through the progress query above.
	if err := republishEachBlock(ctx, blocks, &state); err != nil {
		return RepublishResult{}, err
	}

	logger.Info("block republish complete", "blocks", state.Done)
	return RepublishResult{Requested: state.Total, Republished: state.Republished}, nil
}

// registerProgressQuery is registered before validation so the Query tab answers
// for every run, including a rejected one — which would otherwise reply "unknown
// queryType progress" and read like a broken worker.
func registerProgressQuery(ctx workflow.Context, state *republishProgress) error {
	if err := workflow.SetQueryHandler(ctx, progressQueryName, func() (republishProgress, error) {
		return *state, nil
	}); err != nil {
		return fmt.Errorf("registering %q query handler: %w", progressQueryName, err)
	}
	return nil
}

func resolveBlocks(input json.RawMessage) ([]int64, error) {
	blocks, err := blocksFromInput(input)
	if err != nil {
		return nil, temporalsdk.NewNonRetryableApplicationError(
			"invalid republish input", "InvalidParams", err)
	}
	return blocks, nil
}

// blocksFromInput reads the operator's JSON strictly: a field this workflow does
// not take fails the run instead of being ignored, so an input written against
// an older runbook cannot quietly do something else.
func blocksFromInput(input json.RawMessage) ([]int64, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()

	var params RepublishParams
	if err := decoder.Decode(&params); err != nil {
		return nil, fmt.Errorf(`the input must name the blocks to republish, as {"blocks":[25395651]}: %w`, err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("the input must be one object; everything after the first is ignored, so it is refused instead")
	}
	return params.resolve()
}

// republishEachBlock runs one activity per block, in the order given, and
// hard-stops on the first failure. Every completed block is already in the event
// history, so a retry or a rolled pod resumes at the next one; continuing past a
// failure would end the run reporting success over a height still holding the
// losing fork.
func republishEachBlock(ctx workflow.Context, blocks []int64, state *republishProgress) error {
	ctx = workflow.WithActivityOptions(ctx, republishActivityOptions())

	var activities *republishActivities
	for _, number := range blocks {
		var version int
		if err := workflow.ExecuteActivity(ctx, activities.DeriveVersion, number).Get(ctx, &version); err != nil {
			return err
		}

		var republished block_republish.Result
		work := blockWork{Number: number, Version: version}
		if err := workflow.ExecuteActivity(ctx, activities.RepublishBlock, work).Get(ctx, &republished); err != nil {
			return err
		}
		workflow.GetLogger(ctx).Info("republished block", "block", number, "version", republished.Version)
		state.Republished = append(state.Republished, republished)
		state.Done++
	}
	return nil
}

func republishActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// One block is three RPC reads, a Redis pipeline write and one SNS
		// publish — seconds on a healthy node. The ceiling is minutes because an
		// archive node serving a months-old block by hash can be slow.
		StartToCloseTimeout: 5 * time.Minute,

		// Total time for ONE block INCLUDING retries. This, not an attempt cap,
		// is what lets a run ride out the case it exists to be careful about: a
		// height whose canonical hash is still moving fails every attempt until
		// the chain settles, which is minutes.
		ScheduleToCloseTimeout: 30 * time.Minute,

		// Without this a worker killed mid-block looks alive until StartToClose
		// above, and the retry's identical event would arrive after SNS FIFO has
		// forgotten the first one.
		HeartbeatTimeout: heartbeatTimeoutFactor * heartbeatInterval,

		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
		},
	}
}

// heartbeater is the slice of temporal.ActivityProgress these activities use:
// the phase a block is in, and the liveness ping that re-sends it.
type heartbeater interface {
	temporal.ProgressHeartbeater
	SaveProgress(ctx context.Context, beat republishHeartbeat) error
}

type republishActivities struct {
	service *block_republish.Service

	// A factory, not an instance: the worker caps no activity concurrency, so two
	// runs on one pod would otherwise interleave their phases into one set of details.
	newProgress func() heartbeater
}

// DeriveVersion reads the version this height's raw archive leaves free. It is
// its own activity so the answer lands in workflow history before anything is
// published: a RepublishBlock that fails after its publish is retried at the
// version recorded here, not at the one the archive would report by then.
func (a *republishActivities) DeriveVersion(ctx context.Context, blockNumber int64) (version int, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	stopHeartbeat := temporal.StartHeartbeat(ctx, heartbeatInterval, nil)
	defer stopHeartbeat()

	version, err = a.service.NextFreeVersion(ctx, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("deriving the version for block %d: %w", blockNumber, err)
	}
	return version, nil
}

// RepublishBlock caches the canonical block at this height under the version the
// run settled on and announces it on the chain's SNS topic.
func (a *republishActivities) RepublishBlock(ctx context.Context, work blockWork) (result block_republish.Result, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	progress := a.newProgress()
	stopHeartbeat := temporal.StartHeartbeat(ctx, heartbeatInterval, progress)
	defer stopHeartbeat()

	result, err = a.service.Republish(ctx, work.Number, work.Version, recordPhase(progress, work))
	if err != nil {
		return block_republish.Result{}, fmt.Errorf("republishing block %d at version %d: %w", work.Number, work.Version, err)
	}
	return result, nil
}

// recordPhase puts the step a block is in into the heartbeat details, which the
// liveness ticker then re-sends until the next phase replaces it.
func recordPhase(progress heartbeater, work blockWork) block_republish.PhaseReporter {
	return func(ctx context.Context, phase block_republish.Phase) {
		// SaveProgress cannot fail: a heartbeat the server rejects surfaces as a
		// cancelled activity context on the next read, not as an error here.
		_ = progress.SaveProgress(ctx, republishHeartbeat{
			Block:   work.Number,
			Version: work.Version,
			Phase:   string(phase),
		})
	}
}

// nonRetryableIfStructural stops Temporal retrying a verdict that cannot change.
// The activity caps no attempts, so an unclassified structural failure would
// burn its whole 30-minute envelope before an operator sees a fault only a
// corrected input or a different node can clear.
func nonRetryableIfStructural(err error) error {
	if errors.Is(err, block_republish.ErrStructuralData) {
		return temporalsdk.NewNonRetryableApplicationError(err.Error(), "StructuralData", err)
	}
	return err
}
