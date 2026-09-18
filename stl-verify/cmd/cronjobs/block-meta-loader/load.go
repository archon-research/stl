package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockmetacfg"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/block_meta_loader"
)

const (
	// loadActivityName is registered explicitly so the workflow history keeps
	// naming the same activity across a Go rename.
	loadActivityName = "LoadBlockMeta"

	// heartbeatInterval is how often a running load reports liveness, and
	// heartbeatTimeoutFactor the grace Temporal allows over it so one missed ping
	// cannot fail a live attempt.
	heartbeatInterval      = 30 * time.Second
	heartbeatTimeoutFactor = 3

	// activityTimeout is the ceiling on one attempt, not a budget the run must finish inside:
	// chain 1's first run is ~982k blocks, and a timed-out attempt resumes from the committed
	// work list rather than re-enumerating.
	activityTimeout = 24 * time.Hour
)

// LoadParams is the JSON an operator supplies in the Temporal UI's Input box.
// Empty is the normal case:
//
//	{}
//	{"batchSize":200}
//
// The chain is deliberately not here — see config.
type LoadParams struct {
	// BatchSize overrides the deployment's BATCH_SIZE for this run. Zero uses the
	// deployment's own setting. It is the one knob worth having per run: the
	// deadline is dominated by S3 reads, which are per block.
	BatchSize int `json:"batchSize,omitempty"`
}

// LoadProgress is both the heartbeat payload and the workflow's query result, so
// a run that is still going and a run that has failed report the same shape.
type LoadProgress struct {
	Loaded int64 `json:"loaded"`
}

// loadWorkflow runs one pass of the loader for this deployment's chain.
//
// One activity, not one per batch: the loader's own work list already pages the
// chain and survives a restart, so splitting it across activities would put a
// second cursor in the workflow history that could disagree with the one in the
// database.
func loadWorkflow(ctx workflow.Context, params LoadParams) (LoadProgress, error) {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: activityTimeout,
		HeartbeatTimeout:    heartbeatInterval * heartbeatTimeoutFactor,
	})

	var result LoadProgress
	if err := workflow.ExecuteActivity(actx, loadActivityName, params).Get(actx, &result); err != nil {
		return LoadProgress{}, err
	}
	return result, nil
}

// loadActivities holds what the activity needs for the life of the worker, so a
// run does not pay for opening the pool or the archive reader.
type loadActivities struct {
	cfg    blockmetacfg.Config
	pool   *pgxpool.Pool
	reader outbound.S3Reader
	logger *slog.Logger
}

// LoadBlockMeta fills block_meta for this deployment's chain and returns what it
// wrote. It heartbeats the running total after every committed batch: a run with
// nothing to do finishes in seconds, and a first full pass takes hours, so
// silence has to be distinguishable from progress.
func (a *loadActivities) LoadBlockMeta(ctx context.Context, params LoadParams) (LoadProgress, error) {
	var out LoadProgress

	// A fresh writer run per attempt, so rows carry the attempt that wrote them
	// rather than the one that opened the worker.
	buildReg, runID, err := writerrun.Open(ctx, a.pool)
	if err != nil {
		return out, err
	}
	repo, err := postgres.NewBlockMetaRepository(a.pool, a.logger, buildReg.BuildID(), runID)
	if err != nil {
		return out, fmt.Errorf("creating block_meta repository: %w", err)
	}

	batchSize := a.cfg.BatchSize
	if params.BatchSize > 0 {
		batchSize = params.BatchSize
	}

	heartbeat := temporal.NewActivityProgress[LoadProgress]()
	svc, err := block_meta_loader.New(block_meta_loader.Config{
		ChainID:     a.cfg.ChainID,
		Bucket:      a.cfg.Bucket,
		BatchSize:   batchSize,
		Concurrency: a.cfg.Concurrency,
		HeadMargin:  a.cfg.HeadMargin,
		OnProgress: func(total int64) {
			_ = heartbeat.SaveProgress(ctx, LoadProgress{Loaded: total})
		},
	}, repo, a.reader, a.logger)
	if err != nil {
		return out, err
	}

	a.logger.Info("block-meta-loader run starting",
		"chain", a.cfg.ChainID, "bucket", a.cfg.Bucket, "batchSize", batchSize)
	loaded, err := svc.Run(ctx)
	out.Loaded = loaded
	if err != nil {
		return out, err
	}
	a.logger.Info("block-meta-loader run complete", "chain", a.cfg.ChainID, "rows", loaded)
	return out, nil
}

// activityRegistration keeps the registered name next to the method it names.
func (a *loadActivities) register(r interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}) {
	r.RegisterActivityWithOptions(a.LoadBlockMeta, activity.RegisterOptions{Name: loadActivityName})
}
