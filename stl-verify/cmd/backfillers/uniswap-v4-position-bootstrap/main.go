// Package main implements the uniswap-v4-position-bootstrap Temporal worker: a
// one-shot, hand-started backfill that snapshots every historical Uniswap V4 LP
// position the live indexer's event-driven coverage can never reach (VEC-639).
//
// # What it closes
//
// v4-core exposes no position enumeration, so the live indexer only ever learns
// a position from a ModifyLiquidity log. One minted before the indexer went live
// and never touched since is invisible to it forever. A run replays the
// PoolManager's whole ModifyLiquidity history for the registered
// snapshot-supported pools, decodes the position keys with the indexer's own
// decoder, and reads each one through the same StateView.getPositionInfo getter
// and append-on-change write path the live indexer uses, at ONE pinned
// finality-safe block. Operation is in docs/runbooks/vector-indexers.md,
// section "Uniswap V4 indexer".
//
// # How to start a run
//
// This carries no schedule: the worker idles on its task queue, so deploying it
// never starts a run. An operator starts one from the Temporal UI or the CLI and
// supplies nothing — the run reads the chain from the environment, the pool set
// from the database, and pins its own finalized head:
//
//	temporal workflow start --namespace vector \
//	  --task-queue uniswap-v4-position-bootstrap --type UniswapV4PositionBootstrap \
//	  --workflow-id uniswap-v4-position-bootstrap-<date>
//
// The workflow ID is the concurrency guard: Temporal rejects a duplicate while a
// run with that ID is in flight.
//
// # Idempotency
//
// Every row goes through the append-on-change writer, which inserts only where
// the stored value for a slot differs, so a second run over covered history
// writes nothing (positionsWritten=0) — except at a pinned height where the live
// indexer holds a block_version > 0 row, the edge the runbook documents.
//
// # Resuming an interrupted run
//
// The run records its pin and every pool it has finished in the activity's
// Temporal heartbeat details. A worker killed mid-run (a deploy rolls this
// Deployment like any other) therefore does not re-derive a fresh head-64 pin on
// the next attempt, which would stitch one snapshot across two heights: the
// attempt reads the record back, holds the pinned height to the recorded hash,
// and continues with the pools still to do. A recorded pin whose height has
// since been reorged fails the run; the answer is a new run on a fresh pin.
//
// Heartbeat details belong to one activity execution, so this only spans the
// automatic attempts within a single run. A run that goes red and is started
// again by hand is a NEW workflow execution with no heartbeat history: it pins
// afresh and starts from the beginning, which is safe because every write is
// idempotent.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/worker"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving/archivingwire"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx)
	cancel()
	if err != nil {
		slog.Error("uniswap-v4-position-bootstrap exited with error", "error", err)
		os.Exit(1)
	}
}

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

const (
	// taskQueueName is the Temporal task queue an operator starts a run on, and
	// also the OTel service name and the Deployment name the vector-cronjobs
	// alerts select by.
	taskQueueName = "uniswap-v4-position-bootstrap"

	// workflowTypeName is what an operator types into the Temporal UI's "Workflow
	// Type" field, so it is registered explicitly rather than derived from a Go
	// name — a rename must not invalidate the runbook or muscle memory.
	workflowTypeName = "UniswapV4PositionBootstrap"

	// A wide eth_getLogs window can take longer than the client's default to
	// answer or to refuse; a timeout is retried as transient and then fails the
	// attempt.
	logScanTimeout = 60 * time.Second
)

func run(ctx context.Context) error {
	// Require DATABASE_URL rather than default to localhost: a deployed worker
	// that silently connected to a local (empty) database would look healthy
	// while bootstrapping nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		return fmt.Errorf("startup configuration: %w", err)
	}

	bootstrap := &bootstrapWorker{}
	defer bootstrap.close()

	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         taskQueueName,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(dbURL)),
		Register:     bootstrap.register,
	})
}

// bootstrapWorker owns process-scoped resources because WorkerConfig cannot
// return cleanup from registration.
type bootstrapWorker struct {
	cleanup func()
}

func (b *bootstrapWorker) close() {
	if b.cleanup != nil {
		b.cleanup()
	}
}

func (b *bootstrapWorker) register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	// One store, shared by the run and the liveness heartbeat: the ticker
	// re-sends what the run recorded instead of erasing it with a bare ping.
	progress := temporal.NewActivityProgress[uniswapv4bootstrap.Progress]()

	runner, cleanup, err := setupRunner(ctx, deps, progress)
	if err != nil {
		return fmt.Errorf("setting up bootstrap runner: %w", err)
	}
	if err := temporal.RegisterRunner(r, temporal.RunnerJob{
		WorkflowType: workflowTypeName,
		Runner:       runner,
		Timeouts:     bootstrapActivityTimeouts,
		Progress:     progress,
	}); err != nil {
		cleanup()
		return fmt.Errorf("registering bootstrap runner: %w", err)
	}
	b.cleanup = cleanup
	return nil
}

// bootstrapActivityTimeouts sizes one run against a full mainnet history: a
// few eth_getLogs windows over ~4M blocks, then one pinned multicall batch per
// 500 keys. The first mainnet run (21 pools, 4,451 keys, 2026-09) took minutes;
// the ceilings are headroom for provider slowness and the bisect's worst case,
// not an estimate. The Heartbeat is what makes them tolerable: without it a
// worker killed mid-run would hold the activity open until StartToClose
// expired. With it, Temporal notices in minutes.
//
// MaximumAttempts is bounded rather than 1: heartbeat details are readable only
// by a LATER attempt of the same activity, so a single attempt has nothing to
// resume into and an interrupted run would pin afresh. Three keeps the operator
// signal: a run still red after them has a cause no retry clears (a reorged
// pin, a provider that keeps refusing) and needs a human.
//
// Errors are not classified retryable vs not. Doing that honestly would mean the
// bootstrap service returning Temporal-typed errors, which would put the SDK
// inside a service that must not know about it; a small attempt count buys the
// resume without that.
var bootstrapActivityTimeouts = temporal.ActivityTimeouts{
	StartToClose:    2 * time.Hour,
	ScheduleToClose: 6 * time.Hour,
	MaximumAttempts: 3,
	Heartbeat:       time.Minute,
}

func setupRunner(ctx context.Context, deps temporal.Dependencies, progress uniswapv4bootstrap.ProgressStore) (temporal.Runner, func(), error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("loading configuration: %w", err)
	}

	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, nil, fmt.Errorf("registering build: %w", err)
	}

	logScan, err := alchemy.NewClient(alchemy.ClientConfig{HTTPURL: cfg.rpcURL, Timeout: logScanTimeout, Logger: deps.Logger})
	if err != nil {
		return nil, nil, fmt.Errorf("creating log scan client: %w", err)
	}

	multicaller, closeRPC, err := newMulticaller(ctx, deps.Logger, cfg, buildReg.BuildID())
	if err != nil {
		return nil, nil, err
	}

	svc, err := newBootstrapService(ctx, deps.Logger, deps.Pool, buildReg.BuildID(), logScan, multicaller, progress, cfg)
	if err != nil {
		closeRPC()
		return nil, nil, err
	}
	return temporal.RunnerFunc(func(ctx context.Context) error {
		return runBootstrap(ctx, deps.Logger, svc, cfg.bootstrap.ChainID)
	}), closeRPC, nil
}

// runBootstrap is one attempt of the activity: the service's run plus the one
// log line that reports what it did, or how far it got before failing.
func runBootstrap(ctx context.Context, logger *slog.Logger, svc *uniswapv4bootstrap.Service, chainID int64) error {
	summary, err := svc.Run(ctx)
	if err != nil {
		if summary.PinnedBlock != 0 {
			logger.Warn("uniswap-v4 position bootstrap stopped with partial progress",
				"chainId", chainID, "pinnedBlock", summary.PinnedBlock, "fromBlock", summary.FromBlock,
				"poolsResumed", summary.PoolsResumed, "scanWindows", summary.ScanWindows, "scanLogs", summary.ScanLogs,
				"keys", summary.Keys, "positionsRead", summary.PositionsRead, "positionsWritten", summary.PositionsWritten,
				"batches", summary.Batches)
		}
		return err
	}
	logger.Info("uniswap-v4 position bootstrap finished",
		"chainId", chainID,
		"pinnedBlock", summary.PinnedBlock, "pinnedHash", summary.PinnedHash,
		"fromBlock", summary.FromBlock, "pools", summary.Pools, "poolsResumed", summary.PoolsResumed,
		"keys", summary.Keys, "keysByPool", summary.KeysByPool,
		"positionsRead", summary.PositionsRead, "positionsWritten", summary.PositionsWritten,
		"batches", summary.Batches, "scanWindows", summary.ScanWindows,
		"scanNarrowings", summary.ScanNarrowings, "scanLogs", summary.ScanLogs)
	return nil
}

func newMulticaller(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	buildID buildregistry.BuildID,
) (outbound.Multicaller, func(), error) {
	ethClient, err := rpchttp.DialEthereum(ctx, cfg.rpcURL)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to the RPC endpoint: %w", err)
	}

	if err := chainutil.AssertChainID(ctx, ethClient, cfg.bootstrap.ChainID); err != nil {
		ethClient.Close()
		return nil, nil, err
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		ethClient.Close()
		return nil, nil, fmt.Errorf("creating the multicall client: %w", err)
	}

	archiveWrap, _, archiveDrain, err := archivingwire.Bootstrap(ctx, logger, cfg.bootstrap.ChainID, int64(buildID), taskQueueName)
	if err != nil {
		ethClient.Close()
		return nil, nil, err
	}
	return archiveWrap(multicaller), func() {
		archiveDrain()
		ethClient.Close()
	}, nil
}

func newBootstrapService(
	ctx context.Context,
	logger *slog.Logger,
	db *pgxpool.Pool,
	buildID buildregistry.BuildID,
	logScan outbound.LogScanClient,
	multicaller outbound.Multicaller,
	progress uniswapv4bootstrap.ProgressStore,
	cfg config,
) (*uniswapv4bootstrap.Service, error) {
	repo := postgres.NewUniswapV4Repository(db, buildID)
	poolRows, err := repo.LoadPools(ctx, cfg.bootstrap.ChainID)
	if err != nil {
		return nil, fmt.Errorf("loading uniswap v4 pools: %w", err)
	}
	if len(poolRows) == 0 {
		return nil, fmt.Errorf("no uniswap v4 pools registered for chain %d", cfg.bootstrap.ChainID)
	}

	txMgr, err := postgres.NewTxManager(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating the tx manager: %w", err)
	}

	return uniswapv4bootstrap.New(uniswapv4bootstrap.Deps{
		Pools:       uniswapv4indexer.RegisteredPoolsFromRows(poolRows),
		LogScan:     logScan,
		Multicaller: multicaller,
		Repo:        repo,
		TxManager:   txMgr,
		Progress:    progress,
		Logger:      logger,
		Config:      cfg.bootstrap,
	})
}
