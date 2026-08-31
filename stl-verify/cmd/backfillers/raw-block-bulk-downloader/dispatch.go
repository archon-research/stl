package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// traceRequest is a height whose traces are still to fetch, and the version they
// belong to.
type traceRequest struct {
	BlockNum int64
	Version  int
}

// applyBlockPlan decides what a fetched block needs and acts on it: it queues the
// uploads, hands the trace collector the heights whose traces are still missing,
// and writes nothing at all in a dry run.
func applyBlockPlan(
	ctx context.Context,
	r outbound.BlockData,
	planner *blockPlanner,
	cfg Config,
	uploadCh chan<- UploadJob,
	traceCh chan<- traceRequest,
	stats *Stats,
	logger *slog.Logger,
) error {
	if r.BlockErr != nil || r.ReceiptsErr != nil {
		return fmt.Errorf("fetching block %d: %w", r.BlockNumber, errors.Join(r.BlockErr, r.ReceiptsErr))
	}

	decision, err := planner.decide(ctx, r)
	if err != nil {
		return err
	}
	logDecision(logger, cfg.DryRun, decision)
	stats.recordPlan(decision.Plan.Action)

	if cfg.DryRun {
		stats.blocksSkipped.Add(1)
		stats.tracesSkipped.Add(1)
		return nil
	}
	return queuePlan(ctx, decision.Plan, r, cfg.Bucket, uploadCh, traceCh, stats)
}

// queuePlan enqueues the block and receipt uploads a plan calls for, then signals
// the trace collector when the plan wants traces too.
func queuePlan(
	ctx context.Context,
	plan blockPlan,
	r outbound.BlockData,
	bucket string,
	uploadCh chan<- UploadJob,
	traceCh chan<- traceRequest,
	stats *Stats,
) error {
	queued, err := queueBlockUploads(ctx, plan, r, bucket, uploadCh, stats)
	if err != nil {
		return err
	}
	if queued {
		stats.blocksProcessed.Add(1)
	} else {
		stats.blocksSkipped.Add(1)
	}
	return requestTraces(ctx, plan, r.BlockNumber, traceCh, stats)
}

// queueBlockUploads enqueues the block and receipt objects a plan calls for, at
// the version it chose.
func queueBlockUploads(
	ctx context.Context,
	plan blockPlan,
	r outbound.BlockData,
	bucket string,
	uploadCh chan<- UploadJob,
	stats *Stats,
) (bool, error) {
	uploads, err := plannedUploads(plan, r, bucket)
	if err != nil {
		return false, err
	}

	for _, job := range uploads {
		select {
		case uploadCh <- job:
			stats.uploadsQueued.Add(1)
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return len(uploads) > 0, nil
}

// plannedUploads builds every block and receipt upload a plan calls for, so a
// payload the RPC did not return stops the height before half a version lands.
func plannedUploads(plan blockPlan, r outbound.BlockData, bucket string) ([]UploadJob, error) {
	part := partition.GetPartition(r.BlockNumber)
	jobs := make([]UploadJob, 0, len(plan.DataTypes))

	for _, dataType := range plan.DataTypes {
		if dataType == s3key.Traces {
			continue
		}

		payload := payloadFor(r, dataType)
		if len(payload) == 0 {
			return nil, fmt.Errorf("block %d: the RPC returned no %s payload", r.BlockNumber, dataType)
		}
		jobs = append(jobs, UploadJob{
			Bucket:   bucket,
			Key:      s3key.BuildWithPartition(part, r.BlockNumber, plan.Version, dataType),
			Data:     payload,
			DataType: dataType,
		})
	}
	return jobs, nil
}

// requestTraces hands the trace collector a height whose traces the plan wants.
func requestTraces(ctx context.Context, plan blockPlan, blockNum int64, traceCh chan<- traceRequest, stats *Stats) error {
	if !slices.Contains(plan.DataTypes, s3key.Traces) {
		stats.tracesSkipped.Add(1)
		return nil
	}

	select {
	case traceCh <- traceRequest{BlockNum: blockNum, Version: plan.Version}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// logDecision reports one height's decision: every block in a dry run, and
// anything but a plain skip otherwise.
func logDecision(logger *slog.Logger, dryRun bool, d blockDecision) {
	if !dryRun && d.Plan.Action == actionSkip {
		return
	}
	logger.Info("block plan",
		"number", d.BlockNumber,
		"vmax", d.State.Version,
		"archivedHash", d.ArchivedHash,
		"canonicalHash", d.CanonicalHash,
		"action", string(d.Plan.Action),
		"version", d.Plan.Version,
	)
}

func payloadFor(r outbound.BlockData, dataType s3key.DataType) json.RawMessage {
	switch dataType {
	case s3key.Block:
		return r.Block
	case s3key.Receipts:
		return r.Receipts
	default:
		return nil
	}
}
