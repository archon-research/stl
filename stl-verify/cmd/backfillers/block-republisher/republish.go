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
//	{"blocks":[25395651],"archiveRepaired":true}
//
// blocks are the heights to republish, in the order they will be processed. The
// version is not one of them: each height lands one past whatever its raw
// archive already holds, or — with archiveRepaired — at the version a
// bulk-downloader repair already wrote the canonical block into. Version is
// declared only so an input written against the runbook that took it is refused
// by name rather than silently republishing somewhere else.
type RepublishParams struct {
	Blocks          []int64 `json:"blocks"`
	ArchiveRepaired bool    `json:"archiveRepaired,omitempty"`

	// Raw, not *int: a literal null decodes a pointer to nil, which would read as
	// an input that never named the field at all.
	Version json.RawMessage `json:"version,omitempty"`
}

// republishRun is one operator input, validated: the heights and the derivation
// they are all republished under.
type republishRun struct {
	Blocks          []int64
	ArchiveRepaired bool
}

func (p RepublishParams) resolve() (republishRun, error) {
	if p.Version != nil {
		return republishRun{}, fmt.Errorf(
			"version is no longer an input: every height is republished one past the highest version its raw " +
				"archive already holds, or at that version with archiveRepaired. Drop it and start the run again")
	}
	if len(p.Blocks) == 0 {
		return republishRun{}, fmt.Errorf("blocks must name at least one height to republish")
	}
	if len(p.Blocks) > maxBlocksPerRun {
		return republishRun{}, fmt.Errorf(
			"this run names %d blocks, over the %d limit: split it into smaller runs", len(p.Blocks), maxBlocksPerRun)
	}
	seen := make(map[int64]bool, len(p.Blocks))
	for _, number := range p.Blocks {
		if number <= 0 {
			return republishRun{}, fmt.Errorf("block number must be positive, got %d", number)
		}
		// A height republished twice in one run lands at two versions: the second
		// pass reads the archive the first one has by then been written into.
		if seen[number] {
			return republishRun{}, fmt.Errorf("block %d is named twice; republish a height once per run", number)
		}
		seen[number] = true
	}
	return republishRun{Blocks: p.Blocks, ArchiveRepaired: p.ArchiveRepaired}, nil
}

// versionRequest is what one height's version is derived from: the height, and
// whether the archive was already repaired ahead of the indexers.
type versionRequest struct {
	Block           int64 `json:"block"`
	ArchiveRepaired bool  `json:"archiveRepaired"`
}

// republishHeartbeat is what a beat carries, so a slow block shows in the
// Temporal UI as the step it is in rather than only as a live worker.
type republishHeartbeat struct {
	Block   int64  `json:"block"`
	Version int    `json:"version"`
	Phase   string `json:"phase"`
}

// derivation is what DeriveVersion settled for one height: the version the
// repair lands in, and the canonical block it must carry.
type derivation struct {
	Version int    `json:"version"`
	Hash    string `json:"hash"`
}

// blockWork is one height and the derivation settled for it. Both travel through
// workflow history, so a retried republish reuses the slot rather than deriving a
// later one from the archive its own first attempt filled, and verifies against
// the block the derivation read rather than whatever the height holds by then.
type blockWork struct {
	Number  int64  `json:"number"`
	Version int    `json:"version"`
	Hash    string `json:"hash"`
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

	run, err := resolveRun(input)
	if err != nil {
		return RepublishResult{}, err
	}
	state.Total = len(run.Blocks)

	logger := workflow.GetLogger(ctx)
	logger.Info("starting block republish", "blocks", state.Total, "archiveRepaired", run.ArchiveRepaired)

	// Temporal discards the result payload of a workflow that returns an error,
	// so a failing run reports what it managed through the progress query above.
	if err := republishEachBlock(ctx, run, &state); err != nil {
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

func resolveRun(input json.RawMessage) (republishRun, error) {
	run, err := runFromInput(input)
	if err != nil {
		return republishRun{}, temporalsdk.NewNonRetryableApplicationError(
			"invalid republish input", "InvalidParams", err)
	}
	return run, nil
}

// runFromInput reads the operator's JSON strictly: a field this workflow does
// not take fails the run instead of being ignored, so an input written against
// an older runbook cannot quietly do something else.
func runFromInput(input json.RawMessage) (republishRun, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()

	var params RepublishParams
	if err := decoder.Decode(&params); err != nil {
		return republishRun{}, fmt.Errorf(`the input must name the blocks to republish, as {"blocks":[25395651]}: %w`, err)
	}
	if decoder.More() {
		return republishRun{}, fmt.Errorf("the input must be one object; everything after the first is ignored, so it is refused instead")
	}
	return params.resolve()
}

// republishEachBlock runs one activity per block, in the order given, and
// hard-stops on the first failure. Every completed block is already in the event
// history, so a retry or a rolled pod resumes at the next one; continuing past a
// failure would end the run reporting success over a height still holding the
// losing fork.
func republishEachBlock(ctx workflow.Context, run republishRun, state *republishProgress) error {
	ctx = workflow.WithActivityOptions(ctx, republishActivityOptions())

	var activities *republishActivities
	for _, number := range run.Blocks {
		request := versionRequest{Block: number, ArchiveRepaired: run.ArchiveRepaired}
		var derived derivation
		if err := workflow.ExecuteActivity(ctx, activities.DeriveVersion, request).Get(ctx, &derived); err != nil {
			return err
		}

		var republished block_republish.Result
		work := blockWork{Number: number, Version: derived.Version, Hash: derived.Hash}
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
		// One block is three RPC reads, a Redis pipeline write and one SNS publish
		// — seconds on a healthy node. The ceiling is minutes because an archive
		// node serving a months-old block can be slow.
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

// DeriveVersion settles the version this height is republished at and the
// canonical block it is repaired to. It is its own activity so both land in
// workflow history before anything is published: a RepublishBlock that fails
// after its publish is retried at the version recorded here, not at the one the
// archive would report by then, and against the block this read saw.
func (a *republishActivities) DeriveVersion(ctx context.Context, request versionRequest) (derived derivation, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	stopHeartbeat := temporal.StartHeartbeat(ctx, heartbeatInterval, nil)
	defer stopHeartbeat()

	derived.Version, derived.Hash, err = a.deriveVersion(ctx, request)
	if err != nil {
		return derivation{}, fmt.Errorf("deriving the version for block %d: %w", request.Block, err)
	}
	return derived, nil
}

// deriveVersion picks the derivation the run asked for: the slot above what the
// archive holds, or the slot a repair already wrote the canonical block into.
func (a *republishActivities) deriveVersion(ctx context.Context, request versionRequest) (int, string, error) {
	if request.ArchiveRepaired {
		return a.service.ArchivedVersion(ctx, request.Block)
	}
	return a.service.NextFreeVersion(ctx, request.Block)
}

// RepublishBlock caches the canonical block at this height under the version the
// run settled on and announces it on the chain's SNS topic.
func (a *republishActivities) RepublishBlock(ctx context.Context, work blockWork) (result block_republish.Result, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	progress := a.newProgress()
	stopHeartbeat := temporal.StartHeartbeat(ctx, heartbeatInterval, progress)
	defer stopHeartbeat()

	result, err = a.service.Republish(ctx, work.Number, work.Version, work.Hash, recordPhase(progress, work))
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
