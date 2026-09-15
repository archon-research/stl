// Package main implements the uniswap-v4-position-bootstrap Temporal worker: two
// one-shot, hand-started backfills that close the holes the live Uniswap V4
// indexer's event-driven coverage can never reach. Neither carries a schedule, so
// deploying the worker never starts anything. Operation is in
// docs/runbooks/vector-indexers.md, section "Uniswap V4 indexer".
//
// # UniswapV4PositionBootstrap — the LP positions (VEC-639)
//
// v4-core exposes no position enumeration, so the live indexer only ever learns a
// position from a ModifyLiquidity log. One minted before the indexer went live
// and never touched since is invisible to it forever. A run replays the
// PoolManager's whole ModifyLiquidity history for the registered
// snapshot-supported pools, decodes the position keys with the indexer's own
// decoder, and reads each one through the same StateView.getPositionInfo getter
// and append-on-change write path the live indexer uses, at ONE pinned
// finality-safe block.
//
// Idempotent: the append-on-change writer inserts only where the stored value for
// a slot differs, so a second run over covered history writes nothing
// (positionsWritten=0) — except at a pinned height where the live indexer holds a
// block_version > 0 row, the edge the runbook documents.
//
// # UniswapV4PosmTransferBackfill — the posm token holders (VEC-790)
//
// uniswap_v4_position.owner is the PoolManager-level owner, which for a
// PositionManager-managed position is the PositionManager contract rather than a
// person; the holder lives only in uniswap_v4_position_nft_transfer. A token that
// was minted and then held emits no Transfer at all, so forward-only coverage
// never reaches it and the holder query answers "no row" — indistinguishable from
// a burned or non-existent token. A run replays the PositionManager's whole
// ERC-721 Transfer history from uniswap_v4_position_manager.deploy_block up to a
// pinned finality-safe height, above which the live indexer owns the stream.
//
// Nothing is read from chain state: a log carries its own height, timestamp,
// token id and both parties. Idempotent for a different reason than the position
// run — the writer appends only log sites that hold no row, so replaying history
// the live indexer already covered adds nothing rather than a correction version
// per site.
//
// # How to start a run
//
// An operator starts one from the Temporal UI or the CLI and supplies nothing:
// the run reads the chain from the environment, the registry from the database,
// and pins its own finalized head.
//
//	temporal workflow start --namespace vector \
//	  --task-queue uniswap-v4-position-bootstrap --type UniswapV4PositionBootstrap \
//	  --workflow-id uniswap-v4-position-bootstrap-<date>
//
// The workflow ID is the concurrency guard: Temporal rejects a duplicate while a
// run with that ID is in flight. It is per ID, not per queue, so the two workflow
// types can run at once — they write different tables.
//
// # Resuming an interrupted run
//
// Each run records its pin, and how far it got, in its activity's Temporal
// heartbeat details. A worker killed mid-run (a deploy rolls this Deployment like
// any other) therefore does not re-derive a fresh pin on the next attempt, which
// would stitch one snapshot across two heights: the attempt reads the record
// back, holds the pinned height to the recorded hash, and continues — with the
// pools still to do, or from the first scan window it had not finished. A
// recorded pin whose height has since been reorged fails the run; the answer is a
// new run on a fresh pin.
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
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
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

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)
}

const (
	// taskQueueName is the Temporal task queue an operator starts a run on, and
	// also the OTel service name and the Deployment name the vector-cronjobs
	// alerts select by.
	taskQueueName = "uniswap-v4-position-bootstrap"

	// The workflow type names are what an operator types into the Temporal UI's
	// "Workflow Type" field, so they are registered explicitly rather than derived
	// from Go names — a rename must not invalidate the runbook or muscle memory.
	// One queue, two types: the two backfills close different holes in the same
	// data and neither is a phase of the other, so an operator starts whichever
	// one a gap calls for.
	positionWorkflowTypeName = "UniswapV4PositionBootstrap"
	transferWorkflowTypeName = "UniswapV4PosmTransferBackfill"

	// metricPrefix must match uniswapV4Factory.MetricPrefix() in the dex-indexer:
	// the transfer backfill's rows have to land on the SAME counter the live
	// indexer's do, because that counter is what the table's growth tripwire reads.
	metricPrefix = "uniswap_v4"

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
	// One store per workflow type, each shared by its run and the liveness
	// heartbeat: the ticker re-sends what the run recorded instead of erasing it
	// with a bare ping. Separate stores because heartbeat details belong to one
	// activity execution, and the two runs record different shapes.
	positionProgress := temporal.NewActivityProgress[uniswapv4bootstrap.Progress]()
	transferProgress := temporal.NewActivityProgress[uniswapv4bootstrap.TransferProgress]()

	jobs, cleanup, err := setupRunners(ctx, deps, positionProgress, transferProgress)
	if err != nil {
		return fmt.Errorf("setting up the uniswap-v4 backfill runners: %w", err)
	}
	for _, job := range jobs {
		if err := temporal.RegisterRunner(r, job); err != nil {
			cleanup()
			return fmt.Errorf("registering the %s runner: %w", job.WorkflowType, err)
		}
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

// setupRunners builds both of this worker's jobs off one registry read, one
// Alchemy client and one RPC connection.
//
// newMulticaller comes before the registry read because it asserts the endpoint's
// chain id: a worker pointed at the wrong endpoint must say so, not report the
// empty registry that a wrong CHAIN_ID also produces.
func setupRunners(
	ctx context.Context,
	deps temporal.Dependencies,
	positionProgress *temporal.ActivityProgress[uniswapv4bootstrap.Progress],
	transferProgress *temporal.ActivityProgress[uniswapv4bootstrap.TransferProgress],
) ([]temporal.RunnerJob, func(), error) {
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

	jobs, err := loadAndBuildJobs(ctx, deps, jobInputs{
		cfg: cfg, buildID: buildReg.BuildID(), logScan: logScan, multicaller: multicaller,
		positionProgress: positionProgress, transferProgress: transferProgress,
	})
	if err != nil {
		closeRPC()
		return nil, nil, err
	}
	return jobs, closeRPC, nil
}

// jobInputs is what setupRunners has built by the time the database reads happen.
type jobInputs struct {
	cfg              config
	buildID          buildregistry.BuildID
	logScan          outbound.LogScanClient
	multicaller      outbound.Multicaller
	positionProgress *temporal.ActivityProgress[uniswapv4bootstrap.Progress]
	transferProgress *temporal.ActivityProgress[uniswapv4bootstrap.TransferProgress]
}

func loadAndBuildJobs(ctx context.Context, deps temporal.Dependencies, in jobInputs) ([]temporal.RunnerJob, error) {
	repo := postgres.NewUniswapV4Repository(deps.Pool, in.buildID)
	pools, err := loadRegisteredPools(ctx, repo, in.cfg.bootstrap.ChainID)
	if err != nil {
		return nil, err
	}
	txMgr, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating the tx manager: %w", err)
	}

	return buildRunnerJobs(deps, runnerWiring{
		cfg: in.cfg, pools: pools, repo: repo, txMgr: txMgr,
		logScan: in.logScan, multicaller: in.multicaller,
		positionProgress: in.positionProgress, transferProgress: in.transferProgress,
	})
}

// runnerWiring is the dependency set both jobs draw from, named so the two
// constructions below read as a list of what each job takes.
type runnerWiring struct {
	cfg              config
	pools            []uniswapv4indexer.RegisteredPool
	repo             *postgres.UniswapV4Repository
	txMgr            outbound.TxManager
	logScan          outbound.LogScanClient
	multicaller      outbound.Multicaller
	positionProgress *temporal.ActivityProgress[uniswapv4bootstrap.Progress]
	transferProgress *temporal.ActivityProgress[uniswapv4bootstrap.TransferProgress]
}

func buildRunnerJobs(deps temporal.Dependencies, w runnerWiring) ([]temporal.RunnerJob, error) {
	chainID := w.cfg.bootstrap.ChainID

	positions, err := uniswapv4bootstrap.New(uniswapv4bootstrap.Deps{
		Pools:       w.pools,
		LogScan:     w.logScan,
		Multicaller: w.multicaller,
		Repo:        w.repo,
		TxManager:   w.txMgr,
		Progress:    w.positionProgress,
		Logger:      deps.Logger,
		Config:      w.cfg.bootstrap,
	})
	if err != nil {
		return nil, err
	}

	// Deferred rather than returned: this job's refusals would otherwise fail the
	// whole worker's registration and CrashLoop the Deployment, taking the
	// unrelated position bootstrap with it. Its workflow type carries the refusal
	// instead, so only a run of it fails.
	transfers, transferErr := newTransferService(deps.Logger, w)
	if transferErr != nil {
		deps.Logger.Error("the uniswap-v4 posm transfer backfill is not runnable; its workflow type is registered but will refuse every run",
			"chainId", chainID, "error", transferErr)
	}

	return []temporal.RunnerJob{
		{
			WorkflowType: positionWorkflowTypeName,
			Runner: temporal.RunnerFunc(func(ctx context.Context) error {
				return runBootstrap(ctx, deps.Logger, positions, chainID)
			}),
			Timeouts: bootstrapActivityTimeouts,
			Progress: w.positionProgress,
		},
		{
			WorkflowType: transferWorkflowTypeName,
			ActivityName: temporal.RunnerActivityName(transferWorkflowTypeName),
			Runner: temporal.RunnerFunc(func(ctx context.Context) error {
				if transferErr != nil {
					return fmt.Errorf("the posm transfer backfill was refused at worker startup on chain %d: %w", chainID, transferErr)
				}
				return runTransferBackfill(ctx, deps.Logger, transfers, chainID)
			}),
			Timeouts: transferActivityTimeouts,
			Progress: w.transferProgress,
		},
	}, nil
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

// transferActivityTimeouts sizes one run against a full mainnet posm history plus
// the archive reads that stamp each block's version.
//
// Measured 2026-09-14/15 against mainnet: 487,908 Transfer logs over 4.29M blocks
// in 68 eth_getLogs windows, ~2 minutes of RPC; ~4,300 partition listings; and a
// hash read for each height carrying more than one archived version, which is the
// ~100k in the band two archive writers overlapped. StartToClose therefore has to
// clear a run of several hours end to end — 12h leaves room for provider slowness
// and for a chain with an order of magnitude more tokens, and ScheduleToClose
// admits three attempts of it.
//
// A later attempt is cheaper than the first: the scan resumes from the recorded
// cursor, so committed windows are neither re-listed nor re-read.
var transferActivityTimeouts = temporal.ActivityTimeouts{
	StartToClose:    12 * time.Hour,
	ScheduleToClose: 36 * time.Hour,
	MaximumAttempts: 3,
	Heartbeat:       time.Minute,
}

func newTransferService(logger *slog.Logger, w runnerWiring) (*uniswapv4bootstrap.TransferService, error) {
	positionManager, err := uniswapv4indexer.PositionManagerFor(w.pools)
	if err != nil {
		return nil, err
	}
	// The prefix is the live indexer's, so both writers move one counter; the
	// chain name comes from the chain id, as it does for every dex worker.
	telemetry, err := dextelemetry.NewTelemetry(metricPrefix, w.cfg.bootstrap.ChainID)
	if err != nil {
		return nil, fmt.Errorf("creating uniswap-v4 transfer telemetry: %w", err)
	}

	return uniswapv4bootstrap.NewTransferService(uniswapv4bootstrap.TransferDeps{
		PositionManager: positionManager,
		LogScan:         w.logScan,
		Repo:            w.repo,
		TxManager:       w.txMgr,
		Progress:        w.transferProgress,
		Telemetry:       telemetry,
		Logger:          logger,
		Config:          w.cfg.bootstrap,
	})
}

// runTransferBackfill is one attempt of the transfer activity: the service's run
// plus the one log line that reports what it did, or how far it got before failing.
func runTransferBackfill(ctx context.Context, logger *slog.Logger, svc *uniswapv4bootstrap.TransferService, chainID int64) error {
	summary, err := svc.Run(ctx)
	if err != nil {
		if summary.PinnedBlock != 0 {
			logger.Warn("uniswap-v4 posm transfer backfill stopped with partial progress",
				"chainId", chainID, "pinnedBlock", summary.PinnedBlock, "fromBlock", summary.FromBlock,
				"resumedFromBlock", summary.ResumedFromBlock, "scanWindows", summary.ScanWindows,
				"scanNarrowings", summary.ScanNarrowings, "scanLogs", summary.ScanLogs,
				"transfersDecoded", summary.TransfersDecoded, "transfersWritten", summary.TransfersWritten,
				"batches", summary.Batches)
		}
		return err
	}
	logger.Info("uniswap-v4 posm transfer backfill finished",
		"chainId", chainID,
		"pinnedBlock", summary.PinnedBlock, "pinnedHash", summary.PinnedHash,
		"fromBlock", summary.FromBlock, "resumedFromBlock", summary.ResumedFromBlock,
		"scanWindows", summary.ScanWindows, "scanNarrowings", summary.ScanNarrowings,
		"scanLogs", summary.ScanLogs, "transfersDecoded", summary.TransfersDecoded,
		"transfersWritten", summary.TransfersWritten, "batches", summary.Batches,
		"lowestBlockSeen", summary.LowestBlockSeen, "highestBlockSeen", summary.HighestBlockSeen)
	return nil
}

func loadRegisteredPools(ctx context.Context, repo *postgres.UniswapV4Repository, chainID int64) ([]uniswapv4indexer.RegisteredPool, error) {
	poolRows, err := repo.LoadPools(ctx, chainID)
	if err != nil {
		return nil, fmt.Errorf("loading uniswap v4 pools: %w", err)
	}
	if len(poolRows) == 0 {
		return nil, fmt.Errorf("no uniswap v4 pools registered for chain %d", chainID)
	}
	return uniswapv4indexer.RegisteredPoolsFromRows(poolRows), nil
}
