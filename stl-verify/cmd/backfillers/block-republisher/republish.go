package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/services/block_republish"
)

const (
	// republishActivityName is registered explicitly so the workflow's history
	// keeps naming the same activity across a Go rename.
	republishActivityName = "RepublishBlock"

	// progressQueryName is queryable mid-run from the Temporal UI's Query tab.
	// It is also the only channel a FAILING run has: Temporal discards the result
	// payload of a workflow that returns an error.
	progressQueryName = "progress"

	// maxBlocksPerRun bounds a mistyped input. Each block is one activity of
	// ~6 history events, so 200 is nowhere near Temporal's 51,200-event ceiling —
	// the real bound is the operator's: a repair list this long is a symptom to
	// investigate, not a run to start.
	maxBlocksPerRun = 200

	defaultVersion = 1
)

// RepublishParams is the JSON an operator supplies in the Temporal UI's Input box:
//
//	{"blocks":[25395651,25087888],"version":1}
//
// blocks are the heights to republish, in the order they will be processed.
// version is the slot every one of them is published under, defaulting to 1; it
// is a pointer so an explicit 0 is rejected rather than read as "unset".
type RepublishParams struct {
	Blocks  []int64 `json:"blocks"`
	Version *int    `json:"version,omitempty"`
}

// republishPlan is validated params: what the workflow actually runs.
type republishPlan struct {
	Blocks  []int64
	Version int
}

func (p RepublishParams) resolve() (republishPlan, error) {
	version := defaultVersion
	if p.Version != nil {
		version = *p.Version
	}
	if version < defaultVersion {
		return republishPlan{}, fmt.Errorf(
			"version must be at least %d, got %d — version 0 is the slot holding the data being corrected",
			defaultVersion, version)
	}
	if len(p.Blocks) == 0 {
		return republishPlan{}, fmt.Errorf("blocks must name at least one height to republish")
	}
	if len(p.Blocks) > maxBlocksPerRun {
		return republishPlan{}, fmt.Errorf(
			"this run names %d blocks, over the %d limit: split it into smaller runs", len(p.Blocks), maxBlocksPerRun)
	}
	for _, number := range p.Blocks {
		if number <= 0 {
			return republishPlan{}, fmt.Errorf("block number must be positive, got %d", number)
		}
	}
	return republishPlan{Blocks: p.Blocks, Version: version}, nil
}

type blockWork struct {
	Number  int64 `json:"number"`
	Version int   `json:"version"`
}

// RepublishResult is the workflow's return value, shown in the UI's Result panel.
type RepublishResult struct {
	Version     int                      `json:"version"`
	Requested   int                      `json:"requested"`
	Republished []block_republish.Result `json:"republished"`
}

type republishProgress struct {
	Version     int                      `json:"version"`
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
func republishWorkflow(ctx workflow.Context, params RepublishParams) (RepublishResult, error) {
	var state republishProgress
	if err := registerProgressQuery(ctx, &state); err != nil {
		return RepublishResult{}, err
	}

	plan, err := resolvePlan(params)
	if err != nil {
		return RepublishResult{}, err
	}
	state.Version = plan.Version
	state.Total = len(plan.Blocks)

	logger := workflow.GetLogger(ctx)
	logger.Info("starting block republish", "blocks", state.Total, "version", state.Version)

	// Temporal discards the result payload of a workflow that returns an error,
	// so a failing run reports what it managed through the progress query above.
	if err := republishEachBlock(ctx, plan, &state); err != nil {
		return RepublishResult{}, err
	}

	logger.Info("block republish complete", "blocks", state.Done, "version", state.Version)
	return RepublishResult{Version: state.Version, Requested: state.Total, Republished: state.Republished}, nil
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

func resolvePlan(params RepublishParams) (republishPlan, error) {
	plan, err := params.resolve()
	if err != nil {
		return republishPlan{}, temporalsdk.NewNonRetryableApplicationError(
			"invalid republish parameters", "InvalidParams", err)
	}
	return plan, nil
}

// republishEachBlock runs one activity per block, in the order given, and
// hard-stops on the first failure. Every completed block is already in the event
// history, so a retry or a rolled pod resumes at the next one; continuing past a
// failure would end the run reporting success over a height still holding the
// losing fork.
func republishEachBlock(ctx workflow.Context, plan republishPlan, state *republishProgress) error {
	ctx = workflow.WithActivityOptions(ctx, republishActivityOptions())

	var activities *republishActivities
	for _, number := range plan.Blocks {
		var republished block_republish.Result
		work := blockWork{Number: number, Version: plan.Version}
		if err := workflow.ExecuteActivity(ctx, activities.RepublishBlock, work).Get(ctx, &republished); err != nil {
			return err
		}
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

		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
		},
	}
}

type republishActivities struct {
	service *block_republish.Service
}

// RepublishBlock caches the canonical block at this height under the run's
// version and announces it on the chain's SNS topic.
func (a *republishActivities) RepublishBlock(ctx context.Context, work blockWork) (result block_republish.Result, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	result, err = a.service.Republish(ctx, work.Number, work.Version)
	if err != nil {
		return block_republish.Result{}, fmt.Errorf("republishing block %d at version %d: %w", work.Number, work.Version, err)
	}
	return result, nil
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
