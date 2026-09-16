// Command block-meta-topup keeps one chain's block_meta current on a schedule.
//
// It is the scheduled half of VEC-491. The on-demand block-meta-loader fills a chain's history in one
// operator-started pass; this ticks hourly and picks up what has been referenced since. Both read the
// same service and the same per-chain environment, and both write the same rows, so a tick that
// overlaps a pass is a no-op rather than a conflict.
//
// A tick is bounded by MAX_BLOCKS. Unbounded, the first tick on a chain that has never been
// bootstrapped IS the multi-hour pass, started by a deploy rather than by a person — which is the one
// thing the loader's design refuses. Bounded, a tick either completes the small delta or chips away at
// a backlog and says so, and the backlog is what the staleness alert watches.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockmetacfg"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/services/block_meta_loader"
)

// defaultMaxBlocks bounds one tick. At the loader's measured throughput a tick of this size finishes
// well inside the hourly interval, and a chain that has been bootstrapped never reaches it: the delta
// between ticks is the blocks referenced in an hour. MAX_BLOCKS tunes it; 0 is unbounded and is only
// correct for a run a person is watching.
// queueBaseName is this component's deployed name.
const queueBaseName = "block-meta-topup"

const defaultMaxBlocks = 20000

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx)
	cancel()
	if err != nil {
		slog.Error("block-meta-topup exited with error", "error", err)
		os.Exit(1)
	}
}

// run resolves the per-chain task queue before the worker starts, so a deployment pointed at a chain
// the register does not know fails here rather than polling a queue nothing schedules.
func run(ctx context.Context) error {
	taskQueue, err := chainutil.TaskQueueName(queueBaseName)
	if err != nil {
		return fmt.Errorf("resolving the task queue: %w", err)
	}

	cfg, err := blockmetacfg.Load()
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	return temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:            taskQueue,
		IntervalEnv:     "TOPUP_INTERVAL",
		IntervalDefault: "1h",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(cfg.DSN)),
		Setup: func(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
			return setup(ctx, cfg, deps)
		},
	})
}

// setup proves the archive grants and builds the runner once, at startup. The loader does the same:
// a missing Pod Identity association is then a pod that will not start, rather than a tick that fails
// every hour until someone reads the logs.
func setup(ctx context.Context, cfg blockmetacfg.Config, deps temporal.Dependencies) (temporal.Runner, error) {
	maxBlocks, err := blockmetacfg.PositiveEnv("MAX_BLOCKS", defaultMaxBlocks)
	if err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{})
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	reader := s3adapter.NewReaderFromEnv(awsCfg, deps.Logger)
	if err := s3adapter.NewArchiveReader(reader, cfg.Bucket).Ping(ctx); err != nil {
		return nil, fmt.Errorf("the raw archive %s is unusable: %w", cfg.Bucket, err)
	}

	deps.Logger.Info("block-meta-topup configured",
		"chain", cfg.ChainID, "bucket", cfg.Bucket, "maxBlocks", maxBlocks, "environment", cfg.DeployEnv)

	// A writer run per tick, not per worker: rows carry the tick that wrote them, and a worker that
	// lives for weeks does not attribute every row to the run that happened to start it.
	return temporal.RunnerFunc(func(ctx context.Context) error {
		return tick(ctx, cfg, deps, reader, int64(maxBlocks))
	}), nil
}

func tick(ctx context.Context, cfg blockmetacfg.Config, deps temporal.Dependencies, reader *s3adapter.Reader, maxBlocks int64) error {
	buildReg, runID, err := writerrun.Open(ctx, deps.Pool)
	if err != nil {
		return err
	}
	repo, err := postgres.NewBlockMetaRepository(deps.Pool, deps.Logger, buildReg.BuildID(), runID)
	if err != nil {
		return fmt.Errorf("creating block_meta repository: %w", err)
	}

	svc, err := block_meta_loader.New(block_meta_loader.Config{
		ChainID:     cfg.ChainID,
		Bucket:      cfg.Bucket,
		BatchSize:   cfg.BatchSize,
		Concurrency: cfg.Concurrency,
		HeadMargin:  cfg.HeadMargin,
		MaxBlocks:   maxBlocks,
	}, repo, reader, deps.Logger)
	if err != nil {
		return err
	}

	loaded, err := svc.Run(ctx)
	if err != nil {
		return err
	}
	deps.Logger.Info("block-meta-topup tick complete", "chain", cfg.ChainID, "rows", loaded)
	return nil
}
