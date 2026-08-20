// Package main implements the morpho-v2-bootstrap Temporal worker: a one-shot,
// hand-started repair for Morpho VaultV2 vaults that were discovered before
// VaultV2 discovery became atomic (VEC-218).
//
// # What it heals
//
// Those vaults have a morpho_vault row but no morpho_adapter, adapter_state,
// vault_cap or vault_fee rows. Live indexing will never fix them on its own: the
// worker short-circuits on IsKnownVault before it would enumerate a vault's
// adapters, and the AddAdapter / cap / fee events that would have built those
// rows are historical, so they never arrive on SNS/SQS again. One run replays the
// full VaultV2 governance-event history through the live handler path, then
// enumerates every V2 vault's current adapter set at a pinned finalized block and
// snapshots each adapter's realAssets().
//
// # How to start a run
//
// This carries no schedule: the worker idles on its task queue, so deploying it
// never starts a run. An operator starts one exactly the way they start the
// morpho-vault-backfill, and supplies nothing — the run reads the chain from the
// environment, the vault set from the database, and pins its own finalized head:
//
//	temporal workflow start --namespace vector \
//	  --task-queue morpho-v2-bootstrap --type MorphoV2Bootstrap \
//	  --workflow-id morpho-v2-bootstrap-<date>
//
// The workflow ID is the concurrency guard: Temporal rejects a duplicate while a
// run with that ID is in flight. The run takes hours (a full mainnet log sweep),
// which is why the activity timeouts below are far larger than the shared
// cronjob defaults.
//
// # Idempotency
//
// Every write goes through the same idempotent repository methods live indexing
// uses (append-only membership observations keyed on their own block position,
// ON CONFLICT DO NOTHING snapshots), so starting a second run is safe — it
// redoes the work and reaches the same state. The head seed in particular is an
// assertion, so a repeat run whose answer already matches the log writes nothing
// at all.
//
// # Resuming an interrupted run
//
// The sweep records its position in the activity's Temporal heartbeat details
// after every completed block chunk. A worker killed mid-run (a deploy rolls
// this Deployment like any other) therefore does not restart at the factory
// deploy block: the next attempt of the same activity reads the details back and
// resumes at the next chunk. Resume is chunk-aligned and scoped to the chain,
// the sweep start, and the V2 vault set it was computed for — a run whose vault
// set has changed since sweeps the whole range again rather than skip blocks
// that were never read for the new vault.
//
// Heartbeat details belong to one activity execution, so this only spans the
// automatic attempts within a single run. A run that goes red and is started
// again by hand is a NEW workflow execution with no heartbeat history: it starts
// from the beginning, which is safe because every write is idempotent. Any
// failure past the last attempt shows red in the Temporal UI; the fix is to
// start another run once the cause is addressed.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"go.temporal.io/sdk/worker"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_v2_bootstrap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("morpho-v2-bootstrap exited with error", "error", err)
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
	// also the OTel service name the vector-cronjobs alerts select by.
	taskQueueName = "morpho-v2-bootstrap"

	workflowTypeName = "MorphoV2Bootstrap"
)

func run(ctx context.Context) error {
	// Require DATABASE_URL rather than default to localhost: a deployed worker
	// that silently connected to a local (empty) database would look healthy
	// while healing nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		return fmt.Errorf("startup configuration: %w", err)
	}

	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         taskQueueName,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(dbURL)),
		Register:     register,
	})
}

func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	// One store, shared by the sweep and the liveness heartbeat: the ticker
	// re-sends what the sweep recorded instead of erasing it with a bare ping.
	progress := temporal.NewActivityProgress[morpho_v2_bootstrap.SweepProgress]()

	runner, err := setupRunner(ctx, deps, progress)
	if err != nil {
		return err
	}
	return temporal.RegisterRunner(r, temporal.RunnerJob{
		WorkflowType: workflowTypeName,
		Runner:       runner,
		Timeouts:     bootstrapActivityTimeouts,
		Progress:     progress,
	})
}

// bootstrapActivityTimeouts sizes one run against a full mainnet sweep: ~2M
// blocks of eth_getLogs plus a per-vault adapter enumeration. The shared 10m
// default would kill it mid-sweep. They are bound here rather than left to
// whoever starts a run: an operator supplies no input at all, and a mistyped
// ceiling would surface hours in as a killed sweep.
//
// StartToClose is a safety ceiling, not an expectation — the sweep is sparse
// (10 topics over a few hundred chunks), so a healthy run finishes far sooner.
// The Heartbeat is what makes that ceiling tolerable: without it, a worker
// killed mid-run (a deploy rolls this Deployment like any other) would hold the
// activity open until StartToClose expired. With it, Temporal notices in minutes.
//
// MaximumAttempts is bounded rather than 1: heartbeat details are readable only
// by a LATER attempt of the same activity, so a single attempt has nothing to
// resume into and an interrupted run would re-sweep from the factory deploy
// block. Retrying is cheap for the same reason — an attempt after a
// deterministic failure restarts at the chunk that failed, not at the beginning.
// Three keeps the operator signal: a run still red after them has a cause no
// retry clears, and needs a human.
//
// Errors are not classified retryable vs not. Doing that honestly would mean the
// bootstrap service returning Temporal-typed errors, which would put the SDK
// inside a service that must not know about it; a small attempt count buys the
// resume without that.
var bootstrapActivityTimeouts = temporal.ActivityTimeouts{
	StartToClose:    6 * time.Hour,
	ScheduleToClose: 12 * time.Hour,
	MaximumAttempts: 3,
	Heartbeat:       time.Minute,
}

func setupRunner(ctx context.Context, deps temporal.Dependencies, progress morpho_v2_bootstrap.ProgressStore) (temporal.Runner, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return nil, err
	}

	sweepConfig, err := parseSweepConfig(os.Getenv)
	if err != nil {
		return nil, err
	}
	sweepConfig.ChainID = int64(chainID)
	sweepConfig.Logger = deps.Logger

	rpcURL, err := resolveRPCURL(os.Getenv)
	if err != nil {
		return nil, err
	}
	// The sweep issues long, wide eth_getLogs requests; the default 60s client
	// budget would abort them before the node finished collecting results.
	ethClient, err := rpchttp.DialEthereum(ctx, rpcURL, rpchttp.WithClientTimeout(5*time.Minute))
	if err != nil {
		return nil, fmt.Errorf("connecting to RPC: %w", err)
	}

	replayService, err := buildReplayService(ctx, deps, int64(chainID), ethClient)
	if err != nil {
		return nil, err
	}

	service, err := morpho_v2_bootstrap.NewService(sweepConfig, ethClient, replayService, progress)
	if err != nil {
		return nil, fmt.Errorf("creating morpho v2 bootstrap service: %w", err)
	}
	return temporal.RunnerFunc(service.Run), nil
}

// buildReplayService wires the morpho-indexer service in its replay
// configuration — the same one the morpho-vault-backfill uses. The
// bootstrap drives the real handlers through it rather than reimplementing them.
func buildReplayService(ctx context.Context, deps temporal.Dependencies, chainID int64, ethClient *ethclient.Client) (*morpho_indexer.Service, error) {
	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("creating multicall client: %w", err)
	}
	txManager, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}
	morphoRepo, err := postgres.NewMorphoRepository(deps.Pool, deps.Logger, buildReg.BuildID())
	if err != nil {
		return nil, fmt.Errorf("creating morpho repository: %w", err)
	}
	protocolRepo, err := postgres.NewProtocolRepository(deps.Pool, deps.Logger, buildReg.BuildID(), 0)
	if err != nil {
		return nil, fmt.Errorf("creating protocol repository: %w", err)
	}
	eventRepo := postgres.NewEventRepository(deps.Logger, buildReg.BuildID())

	svcConfig, err := morpho_indexer.NewReplayConfig(chainID, deps.Logger)
	if err != nil {
		return nil, err
	}

	replayService, err := morpho_indexer.NewReplayService(svcConfig, multicaller, txManager, protocolRepo, morphoRepo, eventRepo)
	if err != nil {
		return nil, fmt.Errorf("creating morpho replay service: %w", err)
	}
	return replayService, nil
}

// parseSweepConfig reads the two sweep tunables, defaulting both. getenv is
// injected so the parsing is unit-testable.
func parseSweepConfig(getenv func(string) string) (morpho_v2_bootstrap.Config, error) {
	cfg := morpho_v2_bootstrap.ConfigDefaults()

	if v := getenv("BOOTSTRAP_BLOCK_CHUNK_SIZE"); v != "" {
		size, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return morpho_v2_bootstrap.Config{}, fmt.Errorf("parsing BOOTSTRAP_BLOCK_CHUNK_SIZE %q: %w", v, err)
		}
		cfg.BlockChunkSize = size
	}
	if v := getenv("BOOTSTRAP_ADDRESS_BATCH_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return morpho_v2_bootstrap.Config{}, fmt.Errorf("parsing BOOTSTRAP_ADDRESS_BATCH_SIZE %q: %w", v, err)
		}
		cfg.AddressBatchSize = size
	}
	return cfg, nil
}

// resolveRPCURL builds the node URL from the same ALCHEMY_HTTP_URL +
// ALCHEMY_API_KEY pair every other indexer uses, so this cronjob's secret wiring
// matches the workers'.
func resolveRPCURL(getenv func(string) string) (string, error) {
	apiKey := getenv("ALCHEMY_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("ALCHEMY_API_KEY environment variable is required")
	}
	baseURL := getenv("ALCHEMY_HTTP_URL")
	if baseURL == "" {
		baseURL = "https://eth-mainnet.g.alchemy.com/v2"
	}
	return fmt.Sprintf("%s/%s", baseURL, apiKey), nil
}
